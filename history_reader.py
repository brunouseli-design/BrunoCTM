"""Read completed archive files without changing or restarting the collector.

The publishing loop writes only its own state/configuration outside the archive.
The existing collector remains the sole writer of the original snapshots.
"""
import argparse
import gzip
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
import zipfile
import csv
import io
from google.transit import gtfs_realtime_pb2 as pb


def distance(lat, lon, target):
    a,b=map(math.radians,(lat,target[0]));dlat=b-a;dlon=math.radians(target[1]-lon)
    h=math.sin(dlat/2)**2+math.cos(a)*math.cos(b)*math.sin(dlon/2)**2
    return 12742000*math.atan2(math.sqrt(h),math.sqrt(max(0,1-h)))


class Reader:
    def __init__(self, base):
        self.base=Path(base)
        self.plan=json.loads((self.base/'plan.json').read_text())
        self.day=self.plan['service_date'].replace('-','')
        db=sqlite3.connect('file:'+str(self.base/'schedule.sqlite')+'?mode=ro',uri=True)
        self.trips={t:{'route':r,'end':end,'lastSeq':seq,'endStop':stop}
            for t,r,end,seq,stop in db.execute('SELECT t.trip,t.route,s.arrival,s.seq,s.stop FROM trips t JOIN stops s ON s.trip=t.trip WHERE s.seq=(SELECT max(x.seq) FROM stops x WHERE x.trip=t.trip)')}
        db.close()
        with zipfile.ZipFile(self.base/'static.zip') as z:
            self.coords={r['stop_id']:(float(r['stop_lat']),float(r['stop_lon'])) for r in csv.DictReader(io.TextIOWrapper(z.open('stops.txt'),encoding='utf-8-sig'))}
        self.states={};self.processed=set();self.first_request=None;self.last_request=None
        self.latest_timestamp=None;self.failed=0;self.stale=0;self.long_gaps=0;self.max_gap=0
        self.previous_request=None;self.previous_interval=60

    def scan(self):
        # Atomic .json completion markers are written after the Protobuf bytes.
        # Skip .tmp files, and leave every original file unchanged.
        for p in sorted((self.base/'snapshots').rglob('*-vehicle-positions.json')):
            if str(p) in self.processed:continue
            try:
                m=json.loads(p.read_text());request_time=datetime.fromisoformat(m['requested_at']).timestamp()
                if self.previous_request is not None:
                    gap=request_time-self.previous_request;self.max_gap=max(self.max_gap,gap)
                    if gap>max(self.previous_interval,m.get('target_interval_seconds',60))+15:self.long_gaps+=1
                self.previous_request=request_time;self.previous_interval=m.get('target_interval_seconds',60)
                self.first_request=request_time if self.first_request is None else min(self.first_request,request_time)
                self.last_request=request_time if self.last_request is None else max(self.last_request,request_time)
                if not m.get('valid_gtfs_rt'):
                    self.failed+=1;self.processed.add(str(p));continue
                f=pb.FeedMessage();f.ParseFromString(gzip.decompress(p.with_suffix('.pb.gz').read_bytes()))
                stamp=f.header.timestamp
                if not -60<=request_time-stamp<=120:
                    self.stale+=1;self.processed.add(str(p));continue
                self.latest_timestamp=max(self.latest_timestamp or stamp,stamp)
                for e in f.entity:
                    if e.is_deleted or not e.HasField('vehicle'):continue
                    v=e.vehicle;t=v.trip.trip_id
                    if v.trip.start_date!=self.day or t not in self.trips:continue
                    ts=v.timestamp if v.HasField('timestamp') else stamp
                    if not -60<=request_time-ts<=120 or not v.HasField('position'):continue
                    lat,lon=v.position.latitude,v.position.longitude
                    if not math.isfinite(lat) or not math.isfinite(lon) or not(-90<=lat<=90 and -180<=lon<=180):continue
                    trip=self.trips[t];target=self.coords.get(trip['endStop'])
                    if not target:continue
                    d=distance(lat,lon,target)
                    st=self.states.setdefault(t,{'route':trip['route'],'firstSeen':ts,'lastSeen':ts,'awaySeen':False,'completedAt':None,'basis':None})
                    st['lastSeen']=max(st['lastSeen'],ts)
                    explicit=v.HasField('current_stop_sequence') and v.current_stop_sequence>=trip['lastSeq'] and v.current_status==pb.VehiclePosition.STOPPED_AT
                    # Require an earlier sample away from the terminus to avoid
                    # treating a loop's first position as an arrival.
                    if st['completedAt'] is None and d<=250 and (st['awaySeen'] or explicit):
                        st['completedAt']=ts;st['basis']='rt_stopped_at_last_stop' if explicit else 'gps_approach_to_last_stop'
                    if d>500:st['awaySeen']=True
                self.processed.add(str(p))
            except Exception:
                # Retry unreadable/in-flight files on the next pass; never repair
                # or remove data that belongs to the collector.
                continue

    def payload(self):
        routes={r:{'completed':0,'seenTrips':0,'observedButCompletionUnconfirmed':0} for r in {t['route'] for t in self.trips.values()}}
        completions={}
        for t,s in self.states.items():
            r=routes[s['route']];r['seenTrips']+=1
            if s['completedAt'] is not None:
                r['completed']+=1;completions[t]={'route':s['route'],'at':s['completedAt'],'basis':s['basis']}
            else:r['observedButCompletionUnconfirmed']+=1
        finish=datetime.fromisoformat(self.plan['collection_end']).timestamp()
        return {'serviceDate':self.plan['service_date'],'generatedAt':time.time(),'asOf':self.latest_timestamp,
            'finished':time.time()>=finish+60 and (self.last_request or 0)>=finish-90,
            'archiveStart':self.first_request,'archiveLastRequest':self.last_request,'processedVehicleSnapshots':len(self.processed),
            'actualCompleted':sum(r['completed'] for r in routes.values()),'routes':routes,'completions':completions,
            'quality':{'invalidSnapshots':self.failed,'staleSnapshots':self.stale,'longCollectionGaps':self.long_gaps,'maximumRequestGapSeconds':round(self.max_gap,2)},
            'method':'Corse con riscontro al capolinea finale: GPS entro250m dopo una precedente osservazione oltre500m, oppure fermata finale esplicita STOPPED_AT con GPS entro250m.',
            'limitations':'Minimo osservato, non certificazione di tutte le corse svolte. Corse viste senza riscontro finale restano non confermate; assenze AVM non sono soppressioni.'}


def main():
    p=argparse.ArgumentParser();p.add_argument('--base',default='/var/data/2026-11-07');p.add_argument('--loop',action='store_true');p.add_argument('--config');p.add_argument('--summary',action='store_true');args=p.parse_args()
    lock=None
    if args.loop:
        import fcntl
        lock=open('/var/data/ctm-history-reader.lock','a')
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise SystemExit('Reader already running')
    reader=Reader(args.base)
    config=json.loads(Path(args.config).read_text()) if args.config else None
    if config and config['siteOrigin']!='https://ctm-servizio-live.energeticflounder1.chatgpt.site':raise SystemExit('Unexpected destination')
    while True:
        payload={}
        try:
            reader.scan();payload=reader.payload()
            if config:
                req=Request(config['siteOrigin']+'/api/history',data=json.dumps(payload).encode(),method='POST',headers={'Content-Type':'application/json','OAI-Sites-Authorization':'Bearer '+config['serviceToken']})
                with urlopen(req,timeout=15) as r:
                    if r.status!=200:raise RuntimeError('Publish failed')
                print('CTM_HISTORY published',payload['asOf'],payload['actualCompleted'],flush=True)
            else:print(json.dumps({k:v for k,v in payload.items() if k!='completions'} if args.summary else payload,separators=(',',':')),flush=True)
        except Exception as e:
            # Avoid printing request objects, authorization headers or config.
            print('CTM_HISTORY reader error',type(e).__name__,flush=True)
            if not args.loop:raise
        if not args.loop or payload.get('finished'):break
        time.sleep(30)


if __name__=='__main__':main()
