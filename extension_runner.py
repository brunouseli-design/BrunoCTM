"""Independent CTM service-day extension; never writes the 7 November archive.
Run with the pinned collector source and an existing /var/data mount.
"""
import argparse
from datetime import date,datetime,timedelta,timezone
import fcntl
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import signal
import threading
import time
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Europe/Rome')
FIRST=date(2026,11,8)
LAST=date(2026,11,21)
STOP=threading.Event()
ANALYSIS_LOCK=threading.Lock()
COLLECTOR_SHA256='6dafbe0b43b601d548b032a3c56187f8f965a3a101b9ae52d6c887cfbed10ce0'

def service_days():return [FIRST+timedelta(days=i) for i in range((LAST-FIRST).days+1)]

def load_day(source,root,day):
    if day<FIRST or day>LAST:raise ValueError('Outside approved dates')
    if hashlib.sha256(Path(source).read_bytes()).hexdigest()!=COLLECTOR_SHA256:raise ValueError('Collector source mismatch')
    spec=importlib.util.spec_from_file_location('ctm_service_'+day.isoformat().replace('-',''),source)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    module.DAY=day;module.ROOT=Path(root);module.BASE=Path(root)/day.isoformat()
    module.MIDNIGHT=datetime.combine(day,datetime.min.time(),TZ)
    module.LOG=logging.getLogger('ctm_extension.'+day.isoformat())
    if module.POLL_SECONDS!=30:raise ValueError('Expected30 second polling')
    analyze=module.analyze
    def serial_analysis():
        with ANALYSIS_LOCK:analyze()
    module.analyze=serial_analysis
    return module

def state(root,modules,threads,status):
    days=[]
    for module,thread in zip(modules,threads):
        b=module.BASE;entry={'service_date':module.DAY.isoformat(),'thread_alive':thread.is_alive(),'completed':(b/'completed.json').exists()}
        if (b/'status.json').exists():
            try:entry['last_cycle']=json.loads((b/'status.json').read_text()).get('last_cycle')
            except (ValueError,OSError):pass
        if (b/'plan.json').exists():
            try:entry['plan']=json.loads((b/'plan.json').read_text())
            except (ValueError,OSError):pass
        days.append(entry)
    free=os.statvfs(root);payload={'updated_at':datetime.now(timezone.utc).isoformat(),'pid':os.getpid(),'status':status,'first_service_date':FIRST.isoformat(),'last_service_date':LAST.isoformat(),'interval_seconds':30,'available_bytes':free.f_bavail*free.f_frsize,'collector_sha256':COLLECTOR_SHA256,'days':days}
    target=Path(root)/'ctm-extension'/'state.json';tmp=target.with_suffix('.tmp');tmp.write_text(json.dumps(payload,indent=2));tmp.replace(target)

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',default='/var/data');p.add_argument('--collector',required=True);a=p.parse_args()
    root=Path(a.root).resolve()
    if not os.path.ismount(root):raise SystemExit('Expected actual persistent mount')
    usage=os.statvfs(root)
    if usage.f_blocks*usage.f_frsize<8_000_000_000:raise SystemExit('Approved8GB disk not available')
    directory=root/'ctm-extension';directory.mkdir(exist_ok=True)
    lock=(directory/'runner.lock').open('a')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise SystemExit('Extension already running')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(name)s %(levelname)s %(message)s')
    modules=[load_day(a.collector,root,d) for d in service_days()]
    threads=[threading.Thread(target=m.collect,name='service-'+m.DAY.isoformat()) for m in modules]
    def stop(*_):
        STOP.set()
        for m in modules:m.STOP.set()
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    for t in threads:t.start()
    logging.info('CTM_EXTENSION_READY first=%s last=%s interval=30 threads=%s preserved_service_day=2026-11-07',FIRST,LAST,len(threads))
    status='running'
    while not STOP.is_set():
        usage=os.statvfs(root);free=usage.f_bavail*usage.f_frsize
        if free<400_000_000:
            logging.error('CTM_EXTENSION_CAPACITY_PAUSE free_bytes=%s reserve=400000000 original7_untouched',free)
            status='paused_low_disk';stop()
        if not any(t.is_alive() for t in threads):status='finished';STOP.set()
        state(root,modules,threads,status)
        STOP.wait(30)
    for t in threads:t.join(timeout=30)
    state(root,modules,threads,status)

if __name__=='__main__':main()
