"""One service-day CTM collector. Every timestamp is retained in UTC.

The HTTP interface only reads data; the single background thread owns writes.
No endpoints can change the collection window or invoke arbitrary URLs.
"""
import base64
import csv
import gzip
import hashlib
import hmac
import io
import json
import logging
import math
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import zipfile

from google.transit import gtfs_realtime_pb2 as pb

TZ = ZoneInfo('Europe/Rome')
DAY = date.fromisoformat(os.environ.get('SERVICE_DATE', '2026-11-07'))
ROOT = Path(os.environ.get('DATA_DIR', 'data'))
BASE = ROOT / DAY.isoformat()
MIDNIGHT = datetime.combine(DAY, datetime.min.time(), TZ)
STATIC = 'https://flashnetctm.miz.it/download/gtfs/estrazione_gtfs.zip'
FEEDS = {kind: 'https://flashnetctm.miz.it/download/gtfs/gtfs-rt-' + kind + '.pb'
         for kind in ('trip-updates', 'vehicle-positions')}
STOP = threading.Event()
LOG = logging.getLogger('ctm')
POLL_SECONDS = 30
PUBLICATION_GRACE_SECONDS = 6


def next_poll_time(records, current):
    """Follow CTM's generation clock, allowing time for files to be published.

    This is timestamp-based best effort, not a push notification subscription.
    Invalid/stale headers use a bounded 30-second fallback.
    """
    stamps = [r['timestamp'] for r in records if r and r.get('valid_gtfs_rt')
              and r.get('timestamp') and 0 <= current-r['timestamp'] <= 120]
    if stamps:
        target = max(stamps) + POLL_SECONDS + PUBLICATION_GRACE_SECONDS
        if current + 1 <= target <= current + POLL_SECONDS + PUBLICATION_GRACE_SECONDS:
            return target
    return current + POLL_SECONDS


def now():
    return datetime.now(timezone.utc)


def atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('wb') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def save_json(path, obj):
    atomic(path, json.dumps(obj, ensure_ascii=False, indent=2).encode())


def request(url, limit):
    start = time.monotonic()
    req = Request(url, headers={'User-Agent': 'CTMServiceDayArchive/1.0 (one-day research)'})
    with urlopen(req, timeout=25) as r:
        data = r.read(limit + 1)
        if len(data) > limit:
            raise ValueError('Response exceeds size limit')
        meta = {'http_status': r.status, 'last_modified': r.headers.get('Last-Modified'),
                'content_type': r.headers.get('Content-Type'), 'bytes': len(data),
                'sha256': hashlib.sha256(data).hexdigest(),
                'duration_seconds': round(time.monotonic() - start, 3)}
        return data, meta


def rows(z, name):
    with z.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))


def seconds(value):
    h, m, s = map(int, value.split(':'))
    return h * 3600 + m * 60 + s


def build_schedule(static_path, db_path):
    """Stream stop_times; include exceptions and GTFS hours exceeding 24."""
    ds = DAY.strftime('%Y%m%d')
    with zipfile.ZipFile(static_path) as z:
        if z.testzip() is not None:
            raise ValueError('Corrupt static ZIP')
        active = set()
        if 'calendar.txt' in z.namelist():
            weekday = DAY.strftime('%A').lower()
            for r in rows(z, 'calendar.txt'):
                if r['start_date'] <= ds <= r['end_date'] and r[weekday] == '1':
                    active.add(r['service_id'])
        if 'calendar_dates.txt' in z.namelist():
            for r in rows(z, 'calendar_dates.txt'):
                if r['date'] == ds:
                    if r['exception_type'] == '1':
                        active.add(r['service_id'])
                    else:
                        active.discard(r['service_id'])
        trips = {r['trip_id']: r for r in rows(z, 'trips.txt') if r['service_id'] in active}
        if not trips:
            raise ValueError('Static feed has no scheduled trips for service date')
        tmp = Path(str(db_path) + '.tmp')
        tmp.unlink(missing_ok=True)
        db = sqlite3.connect(tmp)
        db.execute('CREATE TABLE trips (trip TEXT PRIMARY KEY, route TEXT)')
        db.execute('CREATE TABLE stops (trip TEXT, seq INTEGER, stop TEXT, arrival INTEGER, departure INTEGER, PRIMARY KEY(trip,seq))')
        db.executemany('INSERT INTO trips VALUES (?,?)', ((k, v['route_id']) for k,v in trips.items()))
        batch = []
        for r in rows(z, 'stop_times.txt'):
            if r['trip_id'] not in trips:
                continue
            a = seconds(r['arrival_time']) if r.get('arrival_time') else None
            d = seconds(r['departure_time']) if r.get('departure_time') else None
            batch.append((r['trip_id'], int(r['stop_sequence']), r['stop_id'], a, d))
            if len(batch) == 2000:
                db.executemany('INSERT INTO stops VALUES (?,?,?,?,?)', batch)
                batch.clear()
        db.executemany('INSERT INTO stops VALUES (?,?,?,?,?)', batch)
        first, last = db.execute('SELECT min(coalesce(departure,arrival)),max(coalesce(arrival,departure)) FROM stops').fetchone()
        db.commit()
        db.close()
        tmp.replace(db_path)
        if first is None or last is None:
            raise ValueError('No usable scheduled stop times')
        # The extra 30 minutes retain delayed last trips. Never end before midnight.
        end = MIDNIGHT + timedelta(seconds=max(86400, last + 1800))
        return {'service_date': DAY.isoformat(), 'timezone': str(TZ),
                'first_scheduled': (MIDNIGHT + timedelta(seconds=first)).isoformat(),
                'last_scheduled': (MIDNIGHT + timedelta(seconds=last)).isoformat(),
                'collection_start': MIDNIGHT.isoformat(), 'collection_end': end.isoformat(),
                'scheduled_trips': len(trips)}


def prepare():
    BASE.mkdir(parents=True, exist_ok=True)
    plan_file = BASE / 'plan.json'
    if plan_file.exists() and (BASE / 'schedule.sqlite').exists():
        return json.loads(plan_file.read_text())
    # Snapshot only once on the service day. Persist retries across restarts.
    if not (BASE / 'static.zip').exists():
        if now().astimezone(TZ).date() != DAY:
            raise RuntimeError('Cannot reconstruct service-day static snapshot after the date')
        attempt = BASE / 'static-attempt.txt'
        if attempt.exists() and time.time() - float(attempt.read_text()) < 14400:
            raise RuntimeError('Static retry deferred to respect CTM four-hour interval')
        attempt.write_text(str(time.time()))
        raw, meta = request(STATIC, 80 * 1024 * 1024)
        atomic(BASE / 'static.zip', raw)
        save_json(BASE / 'static-metadata.json', {'url': STATIC, 'retrieved_at': now().isoformat(), **meta})
    plan = build_schedule(BASE / 'static.zip', BASE / 'schedule.sqlite')
    save_json(plan_file, plan)
    return plan


def capture(kind, slot):
    name = datetime.fromtimestamp(slot, timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    prefix = BASE / 'snapshots' / name[:8] / (name + '-' + kind)
    metadata = Path(str(prefix) + '.json')
    if metadata.exists():
        return json.loads(metadata.read_text())
    record = {'kind': kind, 'url': FEEDS[kind], 'scheduled_at': slot,
              'target_interval_seconds': POLL_SECONDS,
              'requested_at': now().isoformat(), 'valid_gtfs_rt': False}
    try:
        raw, meta = request(FEEDS[kind], 20 * 1024 * 1024)
        record.update(meta)
        atomic(Path(str(prefix) + '.pb.gz'), gzip.compress(raw))
        feed = pb.FeedMessage()
        feed.ParseFromString(raw)
        if not feed.IsInitialized() or feed.header.gtfs_realtime_version not in ('1.0','2.0'):
            raise ValueError('Invalid GTFS Realtime payload')
        record.update(valid_gtfs_rt=True, version=feed.header.gtfs_realtime_version,
                      timestamp=feed.header.timestamp if feed.header.HasField('timestamp') else None,
                      incrementality=feed.header.incrementality, entities=len(feed.entity))
    except Exception as e:
        record['error'] = str(e)[:500]
        LOG.warning('%s: %s', kind, record['error'])
    record['completed_at'] = now().isoformat()
    save_json(metadata, record)
    return record


def analyze():
    """Latest fresh pre-event prediction per trip/stop/type; never claim observation."""
    from collections import Counter, defaultdict
    db = sqlite3.connect(BASE / 'schedule.sqlite')
    trip_routes = dict(db.execute('SELECT trip,route FROM trips'))
    seen_trips = set()
    seen_vehicles = set()
    attempted_slots = set()
    previous_fetch = {}
    previous_hash = {}
    max_gap_seconds = {}
    latest = {}
    stats = Counter()
    hours = defaultdict(Counter)
    for mpath in sorted((BASE / 'snapshots').rglob('*.json')):
        m = json.loads(mpath.read_text())
        attempted_slots.add((m['scheduled_at'], m['kind']))
        stats['requests'] += 1
        if not m.get('valid_gtfs_rt'):
            stats['failed_or_invalid'] += 1
            continue
        stats['valid_snapshots'] += 1
        fetched = datetime.fromisoformat(m['requested_at']).timestamp()
        kind=m['kind']
        if kind in previous_fetch:
            prev_time,prev_interval=previous_fetch[kind]
            gap=fetched-prev_time
            max_gap_seconds[kind]=max(max_gap_seconds.get(kind,0),gap)
            if gap > max(prev_interval,m.get('target_interval_seconds',60))+15:
                stats['long_collection_gaps']+=1
        previous_fetch[kind]=(fetched,m.get('target_interval_seconds',60))
        if m.get('sha256') and previous_hash.get(kind)==m['sha256']:
            stats['unchanged_valid_snapshots']+=1
        previous_hash[kind]=m.get('sha256')
        stamp = m.get('timestamp')
        if not stamp or not -60 <= fetched - stamp <= 120:
            stats['stale_or_missing_header_timestamp'] += 1
            continue
        hour = datetime.fromtimestamp(stamp, TZ).strftime('%Y-%m-%d %H:00')
        hours[hour]['fresh_snapshots'] += 1
        feed = pb.FeedMessage()
        feed.ParseFromString(gzip.decompress(mpath.with_suffix('.pb.gz').read_bytes()))
        for entity in feed.entity:
            if entity.is_deleted:
                continue
            if entity.HasField('vehicle'):
                v = entity.vehicle
                if v.vehicle.id:
                    seen_vehicles.add(v.vehicle.id)
                desc = v.trip
                est = v.timestamp if v.HasField('timestamp') else stamp
            elif entity.HasField('trip_update'):
                v = entity.trip_update
                desc = v.trip
                est = v.timestamp if v.HasField('timestamp') else stamp
            else:
                continue
            if not -60 <= fetched - est <= 120:
                stats['stale_entities'] += 1
                continue
            # Never mix other service days with the target day.
            if desc.start_date != DAY.strftime('%Y%m%d'):
                stats['other_or_missing_service_date'] += 1
                continue
            if desc.trip_id not in trip_routes:
                stats['unmatched_trip_entities'] += 1
                continue
            trip = desc.trip_id
            seen_trips.add(trip)
            if not entity.HasField('trip_update'):
                continue
            if desc.schedule_relationship != 0:
                stats['non_scheduled_trip_updates'] += 1
                continue
            for st in v.stop_time_update:
                if st.schedule_relationship != 0:
                    continue
                if st.HasField('stop_sequence'):
                    matches = db.execute('SELECT seq,stop,arrival,departure FROM stops WHERE trip=? AND seq=?', (trip,st.stop_sequence)).fetchall()
                else:
                    matches = db.execute('SELECT seq,stop,arrival,departure FROM stops WHERE trip=? AND stop=?', (trip,st.stop_id)).fetchall()
                if len(matches) != 1:
                    continue
                seq, stop, a, d = matches[0]
                if st.stop_id and st.stop_id != stop:
                    continue
                for kind, scheduled in [('arrival',a),('departure',d)]:
                    if scheduled is None or not st.HasField(kind):
                        continue
                    ev = getattr(st,kind)
                    scheduled_epoch = (MIDNIGHT + timedelta(seconds=scheduled)).timestamp()
                    predicted = ev.time if ev.HasField('time') else scheduled_epoch + ev.delay if ev.HasField('delay') else None
                    if predicted is None or predicted < est or scheduled_epoch - est > 3600:
                        continue
                    key = (trip,seq,kind)
                    # Capture the last forecast available before its predicted event,
                    # within one hour of schedule, with fresh header/entity timestamps.
                    if key not in latest or fetched > latest[key][0]:
                        latest[key] = (fetched, predicted-scheduled_epoch, trip_routes[trip], hour)
    db.close()
    out = BASE / 'report'
    out.mkdir(exist_ok=True)
    by_route = defaultdict(list)
    for fetched, delay, route, hour in latest.values():
        by_route[route].append(delay)
        hours[hour]['prediction_events'] += 1
    def summary(values):
        values = sorted(values)
        if not values:
            return {'events':0}
        return {'events':len(values), 'mean_delay_seconds':round(sum(values)/len(values),2),
                'median_delay_seconds':values[len(values)//2],
                'p90_delay_seconds':values[max(0, math.ceil(.9*len(values))-1)],
                'share_within_minus60_plus300':round(sum(-60<=v<=300 for v in values)/len(values),4)}
    results = {route:summary(values) for route,values in by_route.items()}
    plan=json.loads((BASE/'plan.json').read_text())
    changes_path=BASE/'sampling-changes.json'
    changes=json.loads(changes_path.read_text()) if changes_path.exists() else []
    boundaries=[(MIDNIGHT.timestamp(),60)]+[(r['effective_at_epoch'],r['interval_seconds']) for r in changes]
    end_epoch=datetime.fromisoformat(plan['collection_end']).timestamp()
    expected_slots=0
    for i,(begin,interval) in enumerate(boundaries):
        finish=min(end_epoch,boundaries[i+1][0] if i+1<len(boundaries) else end_epoch)
        expected_slots+=max(0,math.ceil((finish-max(begin,MIDNIGHT.timestamp()))/interval))*2
    stats['nominal_expected_requests']=expected_slots
    stats['shortfall_vs_nominal_requests']=max(0,expected_slots-len(attempted_slots))
    report = {'service_date':DAY.isoformat(),'generated_at':now().isoformat(),
              'quality':dict(stats),'unique_vehicle_ids':len(seen_vehicles),
              'scheduled_trips':len(trip_routes),'matched_trips_seen':len(seen_trips),
              'coverage_fraction':len(seen_trips)/len(trip_routes) if trip_routes else 0,
              'all_routes':summary([v[1] for v in latest.values()]), 'by_route':results,
              'hourly':dict(hours),
              'sampling_changes':changes,'maximum_fetch_gap_seconds_by_feed':max_gap_seconds,
              'limitations':['Delays are forecasts, not proven actual arrivals.',
                'Missing RT does not establish cancellation.',
                'Missing service_date, stale data and unmatched trip IDs are excluded.',
                'One last fresh pre-event prediction per trip/stop/event type; arrival and departure are distinct.',
                'Forecasts further than one hour before scheduled event are excluded.']}
    report['limitations'].append('Nominal request count is an estimate: synchronization follows upstream generation intervals, which can exceed 30 seconds. Shortfall alone does not prove lost published feeds.')
    save_json(out / 'statistics.json', report)
    with (out/'routes.csv').open('w',newline='') as f:
        fields=['route_id','events','mean_delay_seconds','median_delay_seconds','p90_delay_seconds','share_within_minus60_plus300']
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
        for route, s in results.items(): writer.writerow({'route_id':route,**s})
    with (out/'prediction_events.csv').open('w',newline='') as f:
        writer=csv.writer(f);writer.writerow(['trip_id','stop_sequence','event_type','last_fetch_utc','delay_seconds','route_id'])
        for key,(fetched,delay,route,hour) in latest.items():
            writer.writerow([*key,datetime.fromtimestamp(fetched,timezone.utc).isoformat(),delay,route])
    save_json(BASE/'completed.json', {'completed_at':now().isoformat(),'statistics':report})
    archive = ROOT / ('ctm-' + DAY.isoformat() + '.zip')
    with zipfile.ZipFile(str(archive)+'.tmp','w',zipfile.ZIP_DEFLATED) as z:
        for p in BASE.rglob('*'):
            if p.is_file() and not p.name.endswith('.tmp'):
                z.write(p, str(Path(DAY.isoformat())/p.relative_to(BASE)))
    Path(str(archive)+'.tmp').replace(archive)
    LOG.info('CTM_REPORT_SUMMARY %s', json.dumps({k:v for k,v in report.items() if k not in ('by_route','hourly')},ensure_ascii=False))
    for route,values in results.items():
        LOG.info('CTM_REPORT_ROUTE %s',json.dumps({'route_id':route,**values}))
    for hour,values in hours.items():
        LOG.info('CTM_REPORT_HOUR %s',json.dumps({'hour':hour,**dict(values)}))
    LOG.info('Archive and report ready')


def collect():
    registered_change=False
    while not STOP.is_set():
        try:
            current=now().astimezone(TZ)
            if current < MIDNIGHT:
                LOG.info('CTM_WAITING collection_start=%s',MIDNIGHT.isoformat())
                STOP.wait(min(60,(MIDNIGHT-current).total_seconds()));continue
            plan=prepare()
            if not registered_change:
                change_file=BASE/'sampling-changes.json'
                changes=json.loads(change_file.read_text()) if change_file.exists() else []
                if not changes or changes[-1]['interval_seconds']!=POLL_SECONDS:
                    changes.append({'effective_at_epoch':time.time(), 'effective_at':now().isoformat(),
                                    'interval_seconds':POLL_SECONDS,'synchronization':'feed_header_timestamp_plus_interval_plus_6s'})
                    save_json(change_file,changes)
                    LOG.info('CTM_SAMPLING_CHANGE %s',json.dumps(changes[-1]))
                registered_change=True
            end=datetime.fromisoformat(plan['collection_end'])
            if current >= end:
                archive=ROOT/('ctm-'+DAY.isoformat()+'.zip')
                if not archive.exists(): analyze()
                return
            # Use the actual second: phase correction can place two distinct
            # upstream generations inside the same wall-clock 30-second bin.
            slot=int(time.time())
            # Parallel requests keep the two snapshots close together.
            with ThreadPoolExecutor(max_workers=2) as pool:
                records=list(pool.map(lambda k:capture(k,slot), FEEDS))
            next_poll=next_poll_time(records,time.time())
            headers={k:r.get('timestamp') for k,r in zip(FEEDS,records)}
            save_json(BASE/'status.json',{'last_cycle':now().isoformat(),'plan':plan,
                      'target_interval_seconds':POLL_SECONDS,'feed_timestamps':headers,
                      'next_poll_utc':datetime.fromtimestamp(next_poll,timezone.utc).isoformat()})
            LOG.info('CTM_CYCLE completed slot=%s interval=%s headers=%s next_poll=%s',slot,POLL_SECONDS,json.dumps(headers),next_poll)
            STOP.wait(max(0.1,next_poll-time.time()))
        except Exception:
            LOG.exception('Collector cycle failed')
            STOP.wait(30)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=='/health':
            self.send_response(200);self.end_headers();self.wfile.write(b'ok');return
        password=os.environ.get('ACCESS_PASSWORD','')
        expected='Basic '+base64.b64encode(('ctm:'+password).encode()).decode()
        if not password or not hmac.compare_digest(self.headers.get('Authorization',''),expected):
            self.send_response(401);self.send_header('WWW-Authenticate','Basic realm="CTM archive"');self.end_headers();return
        archive=ROOT/('ctm-'+DAY.isoformat()+'.zip')
        allowed={'/archive.zip':archive,'/statistics.json':BASE/'report/statistics.json',
                 '/routes.csv':BASE/'report/routes.csv','/status.json':BASE/'status.json'}
        if self.path=='/':
            body=('<!doctype html><html lang="it"><meta charset="utf-8"><title>Archivio CTM</title>'
                  '<h1>CTM · 7 novembre 2026</h1><p>Raccolta ogni circa 30 secondi, allineata ai timestamp CTM. Statistiche a fine servizio.</p>'
                  '<ul><li><a href="/status.json">Stato</a></li><li><a href="/archive.zip">Archivio ZIP</a></li>'
                  '<li><a href="/statistics.json">Statistiche</a></li><li><a href="/routes.csv">CSV per linea</a></li></ul></html>').encode()
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers();self.wfile.write(body);return
        path=allowed.get(self.path)
        if not path or not path.is_file():
            self.send_response(404);self.end_headers();return
        self.send_response(200)
        self.send_header('Content-Type','application/zip' if path.suffix=='.zip' else 'application/json' if path.suffix=='.json' else 'text/csv')
        self.send_header('Content-Length',str(path.stat().st_size))
        self.send_header('Cache-Control','no-store')
        self.end_headers()
        with path.open('rb') as f:shutil.copyfileobj(f,self.wfile)


if __name__=='__main__':
    logging.basicConfig(level=logging.INFO)
    ROOT.mkdir(parents=True,exist_ok=True)
    probe=ROOT/'.write-probe'
    atomic(probe,b'write test')
    probe.unlink()
    LOG.info('CTM_STORAGE_READY path=%s',ROOT)
    if not os.environ.get('ACCESS_PASSWORD'):
        raise SystemExit('ACCESS_PASSWORD is required')
    thread=threading.Thread(target=collect,daemon=True);thread.start()
    server=ThreadingHTTPServer(('0.0.0.0',int(os.environ.get('PORT','10000'))),Handler)
    def shutdown(*_):
        STOP.set()
        threading.Thread(target=server.shutdown,daemon=True).start()
    signal.signal(signal.SIGTERM,shutdown)
    signal.signal(signal.SIGINT,shutdown)
    server.serve_forever()
    thread.join(timeout=27)
