"""Service-day RT acquisition independent of static preparation.
Only runs after an explicit future deployment; imports never start processes.
"""
import fcntl,hashlib,importlib.util,json,logging,os,signal,subprocess,sys,threading,time
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from zoneinfo import ZoneInfo
TZ=ZoneInfo('Europe/Rome');FIRST=date(2026,11,8);LAST=date(2026,12,6)
# Absolute safety ceiling only when no static was acquired. It is not a GTFS end time.
DEADLINE=datetime(2026,12,7,7,tzinfo=timezone.utc).timestamp()
STOP=threading.Event();HEAVY=threading.Lock()
def read(path):
 try:return json.loads(Path(path).read_text())
 except (OSError,ValueError):return {}
def atomic(path,value):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(value));tmp.replace(path)
def module_for(root,day):
 spec=importlib.util.spec_from_file_location('collector_'+day.isoformat().replace('-',''),Path(__file__).with_name('app.py'))
 m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 m.ROOT=Path(root);m.DAY=day;m.BASE=m.ROOT/day.isoformat();m.MIDNIGHT=datetime.combine(day,datetime.min.time(),TZ);return m

def working_bytes():
 try:
  p=Path('/sys/fs/cgroup');s=dict(x.split() for x in (p/'memory.stat').read_text().splitlines());limit=int((p/'memory.max').read_text());return int(s.get('anon',0))+int(s.get('kernel',0)),limit
 except (OSError,ValueError):return None,None

def prepare_static(m):
 """Publish a fully committed SQLite then its plan; never expose an in-progress DB."""
 base=m.BASE;base.mkdir(parents=True,exist_ok=True)
 if (base/'plan.json').exists() and (base/'schedule.sqlite').exists():return read(base/'plan.json')
 if not (base/'static.zip').exists():
  if m.now().astimezone(TZ).date()!=m.DAY:raise RuntimeError('Cannot reconstruct service-day static snapshot after the date')
  attempt=base/'static-attempt.txt'
  if attempt.exists() and time.time()-float(attempt.read_text())<14400:raise RuntimeError('Static retry deferred')
  attempt.write_text(str(time.time()));raw,meta=m.request(m.STATIC,80*1024*1024)
  m.atomic(base/'static.zip',raw);m.save_json(base/'static-metadata.json',{'url':m.STATIC,'retrieved_at':m.now().isoformat(),**meta})
 pending=base/'schedule.sqlite.pending'
 for suffix in ('','-journal','-wal','-shm'):Path(str(pending)+suffix).unlink(missing_ok=True)
 plan=m.build_schedule(base/'static.zip',pending)
 import sqlite3
 with sqlite3.connect('file:'+str(pending)+'?mode=ro',uri=True) as db:
  if db.execute('pragma quick_check').fetchone()[0]!='ok':raise ValueError('Invalid derived SQLite')
 pending.replace(base/'schedule.sqlite');m.save_json(base/'plan.json',plan);return plan

def report_ready(root,day):
 """Preserve first-study ZIPs; future raw data need no second on-disk copy."""
 root=Path(root)
 if day<=date(2026,11,21):return (root/('ctm-'+day.isoformat()+'.zip')).exists()
 return (root/day.isoformat()/'completed.json').exists() and (root/day.isoformat()/'report/statistics.json').exists()

def heavy_worker(root,day,mode):
 """One bounded subprocess across static/report/fleet; never in the RT thread."""
 import resource
 root=Path(root);lockpath=root/'ctm-runtime-v2/heavy.lock';lockpath.parent.mkdir(parents=True,exist_ok=True)
 with lockpath.open('a') as lock:
  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:return 75
  used,limit=working_bytes()
  if used is not None and limit-used<160*1024*1024:return 75
  cap=128*1024*1024;resource.setrlimit(resource.RLIMIT_AS,(cap,cap));os.nice(10)
  m=module_for(root,day)
  if mode=='prepare':prepare_static(m)
  elif mode=='report':
   if not (m.BASE/'static.zip').exists() or not (m.BASE/'plan.json').exists():return 76
   if not report_ready(root,day):m.analyze(make_archive=day<=date(2026,11,21))
  else:raise ValueError('Invalid worker mode')
 return 0

class ServiceDay:
 def __init__(self,root,day,clock=time.time,launcher=None):
  self.m=module_for(root,day);self.root=Path(root);self.day=day;self.clock=clock;self.worker=None;self.worker_started=0;self.last_launch=0;self.launcher=launcher or self.launch
  self.last_cycle=None;self.error_type=None;self.done=False;self.plan=None
 def launch(self,mode):
  logdir=self.root/'ctm-runtime-v2/jobs';logdir.mkdir(parents=True,exist_ok=True)
  with (logdir/(self.day.isoformat()+'-'+mode+'.log')).open('a') as log:
   return subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--worker',mode,'--root',str(self.root),'--day',self.day.isoformat()],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 def manage_static(self,now):
  self.plan=read(self.m.BASE/'plan.json') or None
  if self.worker:
   rc=self.worker.poll()
   if rc is None and now-self.worker_started>180:
    # Signal only this instance's optional worker, never a collector or reader.
    self.worker.terminate();self.error_type='StaticWorkerTimeout'
   if rc is not None:self.worker=None
  if self.plan:
   try:
    assert self.plan.get('service_date')==self.day.isoformat();datetime.fromisoformat(self.plan['collection_end'])
   except (AssertionError,KeyError,TypeError,ValueError):self.plan=None;self.error_type='InvalidStaticPlan'
   else:return
  # Freeze on the actual service date only; never substitute tomorrow's static.
  can_prepare=(self.m.BASE/'static.zip').exists() or datetime.fromtimestamp(now,TZ).date()==self.day
  attempt=self.m.BASE/'static-attempt.txt'
  try:next_download=float(attempt.read_text())+14400
  except (OSError,ValueError):next_download=0
  ready=(self.m.BASE/'static.zip').exists() or now>=next_download
  if can_prepare and ready and not self.worker and now-self.last_launch>=60:
   self.last_launch=now
   try:self.worker=self.launcher('prepare');self.worker_started=now
   except Exception as e:self.error_type=type(e).__name__
 def step(self):
  now=self.clock();m=self.m
  if now<m.MIDNIGHT.timestamp():return min(60,m.MIDNIGHT.timestamp()-now)
  m.BASE.mkdir(parents=True,exist_ok=True);self.manage_static(now)
  end=datetime.fromisoformat(self.plan['collection_end']).timestamp() if self.plan else min(m.MIDNIGHT.timestamp()+72*3600,DEADLINE)
  if now>=end:
   self.done=True
   atomic(self.root/'ctm-runtime-v2/closed'/(self.day.isoformat()+'.json'),{'service_date':self.day.isoformat(),'closed_at':datetime.fromtimestamp(now,timezone.utc).isoformat(),'static_available':bool(self.plan),'provisional_ceiling':not bool(self.plan),'collection_end':end})
   return None
  slot=int(now)
  # A static failure never prevents either raw feed request.
  from concurrent.futures import ThreadPoolExecutor
  with ThreadPoolExecutor(max_workers=2) as pool:records=list(pool.map(lambda k:m.capture(k,slot),m.FEEDS))
  now=self.clock();self.last_cycle=datetime.fromtimestamp(now,timezone.utc).isoformat();next_poll=m.next_poll_time(records,now)
  changes=read(m.BASE/'sampling-changes.json')
  if not isinstance(changes,list):changes=[]
  if not changes or changes[-1].get('interval_seconds')!=30 or changes[-1].get('runtime')!='independent-static-v2':
   changes.append({'effective_at':self.last_cycle,'effective_at_epoch':now,'interval_seconds':30,'runtime':'independent-static-v2','synchronization':'feed_header_timestamp_plus_interval_plus_6s'});atomic(m.BASE/'sampling-changes.json',changes)
  atomic(m.BASE/'status.json',{'last_cycle':self.last_cycle,'plan':self.plan,'target_interval_seconds':30,'feed_timestamps':{k:r.get('timestamp') for k,r in zip(m.FEEDS,records)},'next_poll_utc':datetime.fromtimestamp(next_poll,timezone.utc).isoformat(),'static_status':'ready' if self.plan else 'unavailable_raw_rt_retained','runtime':'independent-static-v2'})
  return max(.1,next_poll-now)
 def run(self):
  try:
   while not STOP.is_set():
    try:
     wait=self.step()
     if wait is None:return
    except Exception as e:self.error_type=type(e).__name__;logging.error('COLLECTION_V2_RETRY day=%s type=%s',self.day,self.error_type);wait=30
    STOP.wait(wait)
  finally:
   if self.worker and self.worker.poll() is None:self.worker.terminate()

def main(root):
 root=Path(root);directory=root/'ctm-extension';directory.mkdir(parents=True,exist_ok=True)
 # Same lock as v1: coexistence cannot create duplicate writers.
 lock=(directory/'runner.lock').open('a')
 try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError:lock.close();return 0
 jobs=[ServiceDay(root,FIRST+timedelta(days=i)) for i in range((LAST-FIRST).days+1)];threads={}
 for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:STOP.set())
 while not STOP.is_set():
  free=os.statvfs(root);available=free.f_bavail*free.f_frsize
  if available<400_000_000:
   logging.error('COLLECTION_V2_CAPACITY_PAUSE');STOP.set()
  for job in jobs:
   thread=threads.get(job.day)
   if not job.done and not STOP.is_set() and (thread is None or not thread.is_alive()):
    threads[job.day]=threading.Thread(target=job.run,daemon=True,name='service-'+job.day.isoformat());threads[job.day].start()
  finished=all(j.done for j in jobs)
  atomic(directory/'state.json',{'updated_at':datetime.now(timezone.utc).isoformat(),'pid':os.getpid(),'status':'finished' if finished else 'paused_low_disk' if STOP.is_set() else 'running','first_service_date':FIRST.isoformat(),'last_service_date':LAST.isoformat(),'interval_seconds':30,'available_bytes':available,'collector_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'runtime':'independent-static-v2','days':[{'service_date':j.day.isoformat(),'thread_alive':threads.get(j.day).is_alive() if j.day in threads else False,'completed':(j.m.BASE/'completed.json').exists(),'last_cycle':j.last_cycle or read(j.m.BASE/'status.json').get('last_cycle'),'static_available':bool(j.plan),'error_type':j.error_type} for j in jobs]})
  if finished:return 0
  STOP.wait(15)
 for t in threads.values():t.join(timeout=28)
 return 0
if __name__=='__main__':
 import argparse
 p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--worker',choices=['prepare','report']);p.add_argument('--day',type=date.fromisoformat);a=p.parse_args()
 try:raise SystemExit(heavy_worker(a.root,a.day,a.worker) if a.worker else main(a.root))
 except Exception as e:logging.error('COLLECTION_V2_WORKER_ERROR type=%s',type(e).__name__);raise SystemExit(1)
