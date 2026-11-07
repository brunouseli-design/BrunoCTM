import csv
import gzip
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import app


def fixture(path):
    files={
      'calendar.txt':'service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nbase,1,1,1,1,1,1,1,20261101,20261130\n',
      'calendar_dates.txt':'service_id,date,exception_type\nbase,20261107,2\nextra,20261107,1\n',
      'trips.txt':'route_id,service_id,trip_id\n1,base,removed\n2,extra,night\n',
      'stop_times.txt':'trip_id,arrival_time,departure_time,stop_id,stop_sequence\nremoved,05:00:00,05:00:00,A,1\nnight,25:00:00,25:00:00,B,1\nnight,25:10:00,25:10:00,C,2\n'}
    with zipfile.ZipFile(path,'w') as z:
        for name,content in files.items():z.writestr(name,content)


class Tests(unittest.TestCase):
    def test_poll_follows_newer_feed_clock(self):
        records=[{'valid_gtfs_rt':True,'timestamp':1000},{'valid_gtfs_rt':True,'timestamp':1001}]
        self.assertEqual(app.next_poll_time(records,1008),1037)
        self.assertEqual(app.next_poll_time(records,1100),1130)
        self.assertEqual(app.next_poll_time([{'valid_gtfs_rt':False}],1008),1038)
        self.assertEqual(app.next_poll_time([{'valid_gtfs_rt':True,'timestamp':2000}],1008),1038)

    def test_service_exceptions_and_overnight(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);fixture(root/'static.zip')
            plan=app.build_schedule(root/'static.zip',root/'schedule.sqlite')
            self.assertEqual(plan['scheduled_trips'],1)
            self.assertEqual(plan['last_scheduled'],'2026-11-08T01:10:00+01:00')
            self.assertEqual(plan['collection_end'],'2026-11-08T01:40:00+01:00')

    def test_analysis_excludes_stale_and_other_days(self):
        with tempfile.TemporaryDirectory() as td, patch.object(app,'ROOT',Path(td)), patch.object(app,'BASE',Path(td)/'2026-11-07'):
            b=app.BASE;b.mkdir();fixture(b/'static.zip')
            plan=app.build_schedule(b/'static.zip',b/'schedule.sqlite');app.save_json(b/'plan.json',plan)
            event=int((app.MIDNIGHT+app.timedelta(hours=25)).timestamp())
            stamp=event-120
            for label,service_date,age in [('fresh','20261107',0),('stale','20261107',600),('other','20261106',0)]:
                f=app.pb.FeedMessage();f.header.gtfs_realtime_version='2.0';f.header.timestamp=stamp-age
                e=f.entity.add();e.id='1';u=e.trip_update;u.trip.trip_id='night';u.trip.start_date=service_date
                u.timestamp=stamp-age;st=u.stop_time_update.add();st.stop_sequence=1;st.stop_id='B';st.arrival.time=event+60
                p=b/'snapshots'/label;p.parent.mkdir(exist_ok=True)
                app.atomic(p.with_suffix('.pb.gz'),gzip.compress(f.SerializeToString()))
                app.save_json(p.with_suffix('.json'),{'kind':'trip-updates','scheduled_at':stamp+age,'requested_at':app.datetime.fromtimestamp(stamp,app.timezone.utc).isoformat(),'timestamp':stamp-age,'valid_gtfs_rt':True})
            app.analyze();r=json.loads((b/'report/statistics.json').read_text())
            self.assertEqual(r['matched_trips_seen'],1)
            self.assertEqual(r['all_routes']['events'],1)
            self.assertEqual(r['all_routes']['mean_delay_seconds'],60)
            self.assertEqual(r['quality']['stale_or_missing_header_timestamp'],1)
            self.assertEqual(r['quality']['other_or_missing_service_date'],1)
            self.assertTrue((app.ROOT/'ctm-2026-11-07.zip').exists())


if __name__=='__main__':unittest.main()
