"""Startup-owned supervisor. Importing this module has no operational side effects.
The existing Render startCommand remains `python app.py`.
"""
import fcntl,importlib.util,json,logging,os,signal,subprocess,sys,threading,time
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request,urlopen
from zoneinfo import ZoneInfo
TZ=ZoneInfo('Europe/Rome');COLLECTION_END=datetime(2026,11,22,7,tzinfo=timezone.utc).timestamp();EXPIRY=datetime(2026,12,6,tzinfo=TZ).timestamp()
HERE=Path(__file__).resolve().parent

def read(p):
 try:return json.loads(Path(p).read_text())
 except (OSError,ValueError):return {}
def locked(p):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('a') as f:
  try:fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:return True
 return False

def spawn(argv,log,cap=None):
 def limits():
  if cap:
   import resource
   resource.setrlimit(resource.RLIMIT_AS,(cap*1024*1024,cap*1024*1024))
 Path(log).parent.mkdir(parents=True,exist_ok=True)
 with Path(log).open('a') as out:return subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,preexec_fn=limits)

def start_runtime(root):
 """Called by the normal HTTP service at startup, not by a Shell session."""
 stop=threading.Event();root=Path(root)
 def watch():
  backoff=1
  while not stop.is_set() and time.time()<EXPIRY:
   # Never duplicate a supervisor that was already started independently.
   if locked(root/'ctm-runtime-v2/supervisor.lock'):stop.wait(15);continue
   try:
    (root/'ctm-runtime-v2').mkdir(exist_ok=True)
    p=spawn([sys.executable,str(HERE/'runtime_bootstrap.py'),'--root',str(root)],root/'ctm-runtime-v2/supervisor.log')
    while p.poll() is None and not stop.wait(1):pass
    if stop.is_set():
     if p.poll() is None:p.terminate()
     return
   except Exception as e:logging.error('RUNTIME_BOOTSTRAP_RETRY type=%s',type(e).__name__)
   stop.wait(backoff);backoff=min(60,backoff*2)
 thread=threading.Thread(target=watch,daemon=True,name='ctm-runtime-bootstrap');thread.start();return stop

def fleet(root):
 """Run the private reader unchanged, coordinating its optional subprocesses."""
 root=Path(root);path=root/'ctm-fleet-daily/runner.py'
 spec=importlib.util.spec_from_file_location('private_fleet_runner',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 original_run=module.subprocess.run
 def guarded_run(*a,**kw):
  with (root/'ctm-runtime-v2/heavy.lock').open('a') as lock:
   try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
   except BlockingIOError:raise RuntimeError('Optional work deferred')
   from collection_runtime import working_bytes
   used,limit=working_bytes()
   if used is not None and limit-used<160*1024*1024:raise RuntimeError('Optional work deferred')
   return original_run(*a,**kw)
 module.subprocess.run=guarded_run
 module.main()

def health(root):
 now=time.time();state=read(root/'ctm-extension/state.json');days=[]
 for i in range(14):
  day=(date(2026,11,8)+timedelta(days=i)).isoformat();base=root/day;plan=read(base/'plan.json');status=read(base/'status.json')
  def epoch(v):
   try:return datetime.fromisoformat(v).timestamp()
   except (ValueError,TypeError):return None
  last=epoch(status.get('last_cycle'));end=epoch(plan.get('collection_end'));midnight=datetime.fromisoformat(day).replace(tzinfo=TZ).timestamp()
  active=midnight<=now<(end or min(midnight+72*3600,COLLECTION_END))
  days.append({'serviceDate':day,'active':active,'lastCycle':last,'ageSeconds':now-last if last else None,'collectionEnd':end,'stale':active and (not last or now-last>150)})
 memory={}
 for key in ('memory.current','memory.max'):
  try:memory[key]=int((Path('/sys/fs/cgroup')/key).read_text())
  except (OSError,ValueError):pass
 try:memory['events']=dict((k,int(v)) for k,v in (x.split() for x in Path('/sys/fs/cgroup/memory.events').read_text().splitlines()))
 except (OSError,ValueError):pass
 disk=os.statvfs(root)
 return {'schemaVersion':1,'generatedAt':now,'collectorLockHeld':locked(root/'ctm-extension/runner.lock'),'collectionFinished':state.get('status')=='finished','days':days,'freeBytes':disk.f_bavail*disk.f_frsize,'memory':memory,'stopAt':COLLECTION_END}

def publish(root,value):
 cfg=read(root/'.ctm-dashboard-reader.json');origin=cfg.get('siteOrigin','');u=urlparse(origin)
 if u.scheme!='https' or not u.hostname or not u.hostname.endswith('.chatgpt.site') or u.username or u.password or u.query or u.fragment:raise ValueError('Invalid private origin')
 req=Request(origin.rstrip('/')+'/api/operations',data=json.dumps(value).encode(),method='POST',headers={'Content-Type':'application/json','OAI-Sites-Authorization':'Bearer '+cfg['serviceToken']})
 with urlopen(req,timeout=12) as response:response.read(2048)

class Supervisor:
 def __init__(self,root,clock=time.time):self.root=Path(root);self.clock=clock;self.children={};self.attempts={};self.report=None;self.report_started=0;self.last_publish=0
 def ensure(self,name,argv,lock,log,cap=None):
  p=self.children.get(name)
  if p and p.poll() is None:return
  if locked(lock) or self.clock()-self.attempts.get(name,0)<60:return
  self.attempts[name]=self.clock();self.children[name]=spawn(argv,log,cap);logging.info('RUNTIME_CHILD_STARTED name=%s pid=%s',name,self.children[name].pid)
 def tick(self):
  root=self.root;out=root/'ctm-runtime-v2';now=self.clock()
  if now<COLLECTION_END and read(root/'ctm-extension/state.json').get('status')!='finished':
   self.ensure('collector',[sys.executable,str(HERE/'collection_runtime.py'),'--root',str(root)],root/'ctm-extension/runner.lock',root/'ctm-extension/runner.log')
  if (root/'ctm_daily_reader_supervisor.py').exists():self.ensure('daily',[sys.executable,str(root/'ctm_daily_reader_supervisor.py')],root/'ctm-daily-supervisor.lock',root/'ctm-daily-supervisor.log')
  if (root/'ctm_headway_reader.py').exists():self.ensure('headways',[sys.executable,str(root/'ctm_headway_reader.py')],root/'ctm-headway-reader.lock',root/'ctm-headway-reader.log',128)
  if (root/'ctm-fleet-daily/runner.py').exists() and read(root/'ctm-fleet-daily/published.json').get('periodEnd')!='2026-11-21':
   self.ensure('fleet',[sys.executable,str(HERE/'runtime_bootstrap.py'),'--root',str(root),'--fleet'],root/'ctm-fleet-daily/runner.lock',root/'ctm-fleet-daily/runner.log',128)
  collector=self.children.get('collector');external_collector=locked(root/'ctm-extension/runner.lock') and not (collector and collector.poll() is None)
  # Existing day7 report/archive are deliberately excluded.
  if self.report:
   if self.report.poll() is not None:self.report=None
   elif now-self.report_started>1800:self.report.terminate()
  if not self.report and not external_collector:
   for i in range(14):
    day=(date(2026,11,8)+timedelta(days=i)).isoformat();base=root/day;plan=read(base/'plan.json')
    if plan and datetime.fromisoformat(plan['collection_end']).timestamp()<=now and not (root/('ctm-'+day+'.zip')).exists() and now-self.attempts.get('report:'+day,0)>=300:
     self.report=spawn([sys.executable,str(HERE/'collection_runtime.py'),'--root',str(root),'--worker','report','--day',day],out/'report.log');self.report_started=now;self.attempts['report:'+day]=now;break
  value=health(root);value['supervisorPid']=os.getpid();tmp=out/'state.tmp';tmp.write_text(json.dumps(value));tmp.replace(out/'state.json')
  if now-self.last_publish>=60:
   try:publish(root,value);self.last_publish=now
   except Exception as e:logging.error('RUNTIME_HEARTBEAT_RETRY type=%s',type(e).__name__)
 def stop(self):
  # Only children started by this instance. Never terminate pre-existing processes.
  for p in [*self.children.values(),self.report]:
   if p and p.poll() is None:p.terminate()

def main(root):
 root=Path(root);out=root/'ctm-runtime-v2';out.mkdir(parents=True,exist_ok=True);lock=(out/'supervisor.lock').open('a')
 try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError:lock.close();return
 stop=threading.Event()
 for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
 s=Supervisor(root)
 try:
  while time.time()<EXPIRY and not stop.is_set():
   try:s.tick()
   except Exception as e:logging.error('RUNTIME_SUPERVISOR_RETRY type=%s',type(e).__name__)
   stop.wait(15)
 finally:s.stop()
if __name__=='__main__':
 import argparse
 p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--fleet',action='store_true');a=p.parse_args();logging.basicConfig(level=logging.INFO)
 if a.fleet:fleet(a.root)
 else:main(a.root)
