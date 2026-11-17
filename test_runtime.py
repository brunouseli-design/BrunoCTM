import fcntl,gzip,json,os,runpy,sqlite3,subprocess,sys,tempfile,time,unittest,zipfile
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from unittest.mock import Mock,patch
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2 as pb
import app
import collection_runtime as c
import runtime_bootstrap as b
TZ=ZoneInfo('Europe/Rome')
class Worker:
 def __init__(self,rc=1):self.rc=rc;self.terminated=0
 def poll(self):return self.rc
 def terminate(self):self.terminated+=1;self.rc=-15

def static_fixture(path,day):
 with zipfile.ZipFile(path,'w') as z:
  z.writestr('calendar.txt','service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nx,1,1,1,1,1,1,1,20261101,20261130\n')
  z.writestr('trips.txt','route_id,service_id,trip_id\n1,x,N\n')
  z.writestr('stop_times.txt','trip_id,arrival_time,departure_time,stop_id,stop_sequence\nN,25:10:00,25:10:00,A,1\nN,27:20:00,27:20:00,B,2\n')

class CollectionTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.day=date(2026,11,17);self.epoch=datetime.combine(self.day,datetime.min.time(),TZ).timestamp();self.now=self.epoch+3600;self.launched=[]
  def launch(mode):self.launched.append(mode);return Worker()
  self.job=c.ServiceDay(self.root,self.day,clock=lambda:self.now,launcher=launch)
  self.calls=[]
  def request(url,limit):
   self.calls.append(url);f=pb.FeedMessage();f.header.gtfs_realtime_version='2.0';f.header.timestamp=int(self.now)-6
   return f.SerializeToString(),{'http_status':200,'bytes':len(f.SerializeToString())}
  self.job.m.request=request
 def tearDown(self):self.tmp.cleanup()
 def test_static_failure_does_not_block_either_feed_and_retry_4h(self):
  base=self.job.m.BASE;base.mkdir();(base/'static-attempt.txt').write_text(str(self.now))
  for i in range(3):
   self.job.step();self.now+=30
  self.assertEqual(len(self.calls),6);self.assertEqual(self.launched,[])
  files=list((base/'snapshots').rglob('*.pb.gz'));self.assertEqual(len(files),6)
  for p in files:f=pb.FeedMessage();f.ParseFromString(gzip.decompress(p.read_bytes()));self.assertEqual(f.header.gtfs_realtime_version,'2.0')
  self.assertEqual(c.read(base/'status.json')['static_status'],'unavailable_raw_rt_retained')
  self.now+=14400;self.job.step();self.assertEqual(self.launched,['prepare']);self.assertEqual(len(self.calls),8)
 def test_failed_or_timed_out_worker_and_spawn_failure_do_not_stop_rt(self):
  self.job.step();self.now+=60;self.job.launcher=Mock(side_effect=OSError('synthetic spawn failure'));self.job.step();self.assertEqual(len(self.calls),4)
  w=Worker(None);self.job.worker=w;self.job.worker_started=self.now-181;self.now+=30;self.job.step();self.assertEqual(w.terminated,1);self.assertEqual(len(self.calls),6)
 def test_wrong_static_plan_does_not_block_rt(self):
  self.job.m.BASE.mkdir();c.atomic(self.job.m.BASE/'plan.json',{'service_date':'2026-11-16','collection_end':'bad'});self.job.step();self.assertEqual(len(self.calls),2);self.assertIsNone(self.job.plan)
 def test_static_arrives_and_closes_after_midnight_and_buffer(self):
  self.job.step();base=self.job.m.BASE;static_fixture(base/'static.zip',self.day);plan=self.job.m.build_schedule(base/'static.zip',base/'schedule.sqlite');c.atomic(base/'plan.json',plan)
  self.now=datetime.fromisoformat(plan['collection_end']).timestamp()-30;self.job.step();self.assertEqual(len(self.calls),4)
  self.now+=30;self.assertIsNone(self.job.step());self.assertTrue(self.job.done);self.assertEqual(len(self.calls),4)
  self.assertEqual(plan['collection_end'],'2026-11-18T03:50:00+01:00');self.assertFalse(c.read(self.root/'ctm-runtime-v2/closed'/(self.day.isoformat()+'.json'))['provisional_ceiling'])
 def test_7th_untouched_and_neighbor_ids_separate(self):
  old=self.root/'2026-11-07';old.mkdir();(old/'keep').write_bytes(b'original')
  other=c.ServiceDay(self.root,self.day+timedelta(days=1),clock=lambda:self.now,launcher=lambda _:Worker())
  self.assertIsNot(other.m.STOP,self.job.m.STOP);self.assertNotEqual(other.m.BASE,self.job.m.BASE);self.job.step();self.assertEqual((old/'keep').read_bytes(),b'original');self.assertEqual(other.step(),60)
 def test_local_http_partial_outage_then_recovery_keeps_metadata(self):
  from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
  from threading import Thread
  state={'fail':True};stamp=int(self.now)-6
  class Handler(BaseHTTPRequestHandler):
   def do_GET(self):
    if self.path.endswith('trip-updates.pb') and state['fail']:self.send_response(503);self.end_headers();return
    f=pb.FeedMessage();f.header.gtfs_realtime_version='2.0';f.header.timestamp=stamp;raw=f.SerializeToString();self.send_response(200);self.end_headers();self.wfile.write(raw)
   def log_message(self,*args):pass
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=Thread(target=server.serve_forever,daemon=True);thread.start()
  try:
   self.job.m.FEEDS={k:'http://127.0.0.1:'+str(server.server_port)+'/'+k+'.pb' for k in self.job.m.FEEDS};self.job.m.request=app.request
   self.job.step();records=[json.loads(p.read_text()) for p in self.job.m.BASE.rglob('*-trip-updates.json')];self.assertFalse(records[0]['valid_gtfs_rt']);self.assertIn('503',records[0]['error'])
   state['fail']=False;self.now+=30;self.job.step();all_records=[json.loads(p.read_text()) for p in self.job.m.BASE.rglob('*.json') if p.name.endswith(('vehicle-positions.json','trip-updates.json'))];self.assertEqual(len(all_records),4);self.assertEqual(sum(r['valid_gtfs_rt'] for r in all_records),3)
  finally:server.shutdown();server.server_close();thread.join(timeout=2)
 def test_existing_runner_lock_excludes_duplicate_writer(self):
  p=self.root/'ctm-extension/runner.lock';p.parent.mkdir()
  with p.open('a') as lock:
   fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);self.assertEqual(c.main(self.root),0)
  self.assertEqual(self.calls,[])
 def test_static_never_downloaded_for_past_day(self):
  self.job.m.BASE.mkdir()
  with patch.object(self.job.m,'now',return_value=datetime(2026,11,18,tzinfo=timezone.utc)):
   with self.assertRaisesRegex(RuntimeError,'Cannot reconstruct'):c.prepare_static(self.job.m)
  self.assertEqual(self.calls,[])
 def test_no_new_request_when_static_deferred(self):
  self.job.m.BASE.mkdir();(self.job.m.BASE/'static-attempt.txt').write_text(str(time.time()))
  with patch.object(self.job.m,'now',return_value=datetime(2026,11,17,tzinfo=timezone.utc)):
   with self.assertRaisesRegex(RuntimeError,'deferred'):c.prepare_static(self.job.m)
  self.assertEqual(self.calls,[])
 def test_low_memory_or_heavy_lock_defers_work_before_any_memory_limit_change(self):
  with patch.object(c,'working_bytes',return_value=(500*1024*1024,512*1024*1024)):
   self.assertEqual(c.heavy_worker(self.root,self.day,'prepare'),75)
  lock=self.root/'ctm-runtime-v2/heavy.lock'
  with lock.open('a') as f:
   fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);self.assertEqual(c.heavy_worker(self.root,self.day,'report'),75)
 def test_real_static_worker_subprocess_from_cached_zip(self):
  self.job.m.BASE.mkdir();static_fixture(self.job.m.BASE/'static.zip',self.day)
  result=subprocess.run([sys.executable,str(Path(c.__file__)),'--root',str(self.root),'--worker','prepare','--day',str(self.day)],capture_output=True,text=True,timeout=20)
  self.assertEqual(result.returncode,0,result.stderr);self.assertTrue((self.job.m.BASE/'schedule.sqlite').exists());self.assertEqual(c.read(self.job.m.BASE/'plan.json')['service_date'],str(self.day))

class StartupTests(unittest.TestCase):
 def test_import_has_no_process_side_effects(self):
  with patch('subprocess.Popen',side_effect=AssertionError('No launch during import')):runpy.run_path(str(Path(b.__file__)),run_name='validation_import')
 def test_parent_limit_is_not_inherited_as_64mb(self):
  with tempfile.TemporaryDirectory() as tmp:
   stop=__import__('threading').Event();thread=Mock()
   with patch.object(b.threading,'Thread',return_value=thread):returned=b.start_runtime(Path(tmp))
   self.assertFalse(returned.is_set());thread.start.assert_called_once()
 def test_supervisor_restarts_owned_child_but_does_not_touch_preexisting_lock(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);s=b.Supervisor(root,clock=lambda:1000);log=root/'jobs/x.log';argv=[sys.executable,'-c','import time;time.sleep(30)'];lock=root/'x.lock'
   try:
    s.ensure('test',argv,lock,log);first=s.children['test'];s.ensure('test',argv,lock,log);self.assertEqual(s.children['test'].pid,first.pid)
    first.terminate();first.wait(timeout=5);s.clock=lambda:1061;s.ensure('test',argv,lock,log);second=s.children['test'];self.assertNotEqual(first.pid,second.pid)
    with (root/'external.lock').open('a') as external:
     fcntl.flock(external,fcntl.LOCK_EX|fcntl.LOCK_NB);s.ensure('external',argv,root/'external.lock',log);self.assertNotIn('external',s.children)
   finally:
    s.stop()
    for child in s.children.values():child.wait(timeout=5)
 def test_full_restart_recreates_supervisor_and_keeps_archive(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);(root/'2026-11-07').mkdir();file=root/'2026-11-07/original';file.write_bytes(b'keep');argv=[sys.executable,'-c','import time;time.sleep(30)'];pids=[]
   for _ in range(2):
    s=b.Supervisor(root);s.ensure('dummy',argv,root/'dummy.lock',root/'dummy.log');pids.append(s.children['dummy'].pid);s.stop();s.children['dummy'].wait(timeout=5)
   self.assertNotEqual(*pids);self.assertEqual(file.read_bytes(),b'keep')
 def test_actual_app_entry_calls_bootstrap_without_changing_start_command(self):
  class HTTP:
   def __init__(self,*a,**kw):pass
   def serve_forever(self):pass
  with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{'DATA_DIR':tmp,'ACCESS_PASSWORD':'synthetic-test-only'}),patch('http.server.ThreadingHTTPServer',HTTP),patch('threading.Thread') as thread,patch.object(b,'start_runtime') as start:
   runpy.run_path(str(Path(c.__file__).with_name('app.py')),run_name='__main__');start.assert_called_once_with(Path(tmp));thread.return_value.start.assert_called_once()
 def test_health_empty_archive_is_not_zero_service(self):
  with tempfile.TemporaryDirectory() as tmp,patch.object(b.time,'time',return_value=datetime(2026,11,17,12,tzinfo=TZ).timestamp()):
   value=b.health(Path(tmp));d=next(d for d in value['days'] if d['serviceDate']=='2026-11-17');self.assertTrue(d['stale']);self.assertIsNone(d['lastCycle'])
if __name__=='__main__':unittest.main()
