import tempfile,unittest
from pathlib import Path
from datetime import date,datetime,timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2 as pb
import collection_runtime as c,runtime_bootstrap as b
TZ=ZoneInfo('Europe/Rome')
class Extension(unittest.TestCase):
 def test_period_and_final_processing_window(self):
  self.assertEqual(c.LAST,date(2026,12,31));self.assertEqual(b.LAST,c.LAST)
  self.assertEqual((b.LAST-b.FIRST).days+1,54)
  self.assertEqual(datetime.fromtimestamp(c.DEADLINE,TZ),datetime(2027,1,1,8,tzinfo=TZ))
  self.assertGreater(b.EXPIRY,c.DEADLINE)
 def test_final_service_overnight_both_feeds_until_buffer(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);day=date(2026,12,31);now=[datetime(2027,1,1,3,49,30,tzinfo=TZ).timestamp()]
   job=c.ServiceDay(root,day,clock=lambda:now[0]);job.m.BASE.mkdir();c.atomic(job.m.BASE/'plan.json',{'service_date':str(day),'collection_end':'2027-01-01T03:50:00+01:00'})
   calls=[]
   def request(url,limit):
    calls.append(url);f=pb.FeedMessage();f.header.gtfs_realtime_version='2.0';f.header.timestamp=int(now[0])-6
    return f.SerializeToString(),{'http_status':200}
   job.m.request=request;job.step();self.assertEqual(len(calls),2)
   self.assertEqual(len(list((job.m.BASE/'snapshots').rglob('*.pb.gz'))),2)
   now[0]+=30;self.assertIsNone(job.step());self.assertEqual(len(calls),2)
 def test_cross_month_health_and_overlap(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);now=datetime(2026,12,1,0,20,tzinfo=TZ).timestamp()
   for i in range((date(2026,12,1)-b.FIRST).days+1):
    day=(b.FIRST+timedelta(days=i)).isoformat()
    if day<'2026-11-30':
     c.atomic(root/day/'plan.json',{'collection_end':(datetime.fromisoformat(day).replace(tzinfo=TZ)+timedelta(hours=26)).isoformat()});continue
    c.atomic(root/day/'plan.json',{'collection_end':'2026-12-01T03:50:00+01:00' if day=='2026-11-30' else '2026-12-02T01:44:00+01:00'})
    c.atomic(root/day/'status.json',{'last_cycle':datetime.fromtimestamp(now-20,TZ).isoformat()})
   with patch.object(b.time,'time',return_value=now):h=b.health(root)
   self.assertEqual(len(h['days']),54);active=[x for x in h['days'] if x['active']]
   self.assertEqual([x['serviceDate'] for x in active],['2026-11-30','2026-12-01']);self.assertTrue(all(not x['stale'] for x in active))
 def test_report_completion_does_not_require_duplicate_future_zip(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);day=date(2026,11,22)
   self.assertFalse(c.report_ready(root,day))
   c.atomic(root/str(day)/'completed.json',{});c.atomic(root/str(day)/'report/statistics.json',{})
   self.assertTrue(c.report_ready(root,day));self.assertFalse((root/('ctm-'+str(day)+'.zip')).exists())
   self.assertFalse(c.report_ready(root,date(2026,11,21)))
 def test_final_day_in_health(self):
  with tempfile.TemporaryDirectory() as t,patch.object(b.time,'time',return_value=datetime(2026,12,31,12,tzinfo=TZ).timestamp()):
   h=b.health(Path(t));self.assertTrue(next(x for x in h['days'] if x['serviceDate']=='2026-12-31')['active'])
if __name__=='__main__':unittest.main()
