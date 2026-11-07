"""Read-only CTM capacity audit. Does not import or restart the collector."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from zoneinfo import ZoneInfo

KINDS=('trip-updates','vehicle-positions')
TZ=ZoneInfo('Europe/Rome')

def audit(root, day='2026-11-07', epoch=None):
    root=Path(root).resolve(); base=root/day
    epoch=datetime.now(timezone.utc).timestamp() if epoch is None else epoch
    plan=json.loads((base/'plan.json').read_text())
    fs=os.statvfs(root); block=fs.f_frsize or fs.f_bsize
    def sizes(p):
        st=p.stat();return st.st_size,st.st_blocks*512
    groups=defaultdict(lambda:[0,0,0]);hours=defaultdict(lambda:[0,0,0])
    total=[0,0];day_size=[0,0];unreadable=0;errors=0
    for p in root.rglob('*'):
        if p.is_symlink() or not p.is_file():continue
        try:l,a=sizes(p)
        except OSError:errors+=1;continue
        total[0]+=l;total[1]+=a
        if not p.is_relative_to(base):
            key='outside_day_zip' if p.suffix=='.zip' else 'outside_day_other'
        else:
            day_size[0]+=l;day_size[1]+=a
            if 'snapshots' in p.relative_to(base).parts:
                kind=next((k for k in KINDS if p.name.endswith('-'+k+'.pb.gz')),None)
                key='compressed_'+kind if kind else 'snapshot_metadata' if p.suffix=='.json' else 'snapshot_other'
            else:key='static' if p.name=='static.zip' else 'sqlite' if p.name.startswith('schedule.sqlite') else 'reports' if 'report' in p.relative_to(base).parts else 'day_other'
        g=groups[key];g[0]+=l;g[1]+=a;g[2]+=1
    for p in (base/'snapshots').rglob('*.json'):
        try:
            m=json.loads(p.read_text());kind=m.get('kind')
            if kind not in KINDS or m.get('target_interval_seconds',60)!=30:continue
            t=datetime.fromisoformat(m['requested_at']).timestamp()
            hour=math.floor(t/3600)*3600
            # Only closed hours; 80+ requests below excludes the transition hour.
            if hour+3600>epoch:continue
            l,a=sizes(p);raw=p.with_suffix('.pb.gz')
            if raw.exists():x,y=sizes(raw);l+=x;a+=y
            h=hours[(kind,hour)];h[0]+=1;h[1]+=l;h[2]+=a
        except (OSError,ValueError,KeyError):unreadable+=1
    peak={};evidence={}
    for kind in KINDS:
        eligible=[(h,v) for (k,h),v in hours.items() if k==kind and v[0]>=80]
        if not eligible:continue
        # Peak mean sample cost, normalized to 120 samples/hour at 30 seconds.
        best=max(eligible,key=lambda x:x[1][2]/x[1][0]);h,v=best
        peak[kind]=[max(x[1][1]/x[1][0] for x in eligible)*120,max(x[1][2]/x[1][0] for x in eligible)*120]
        evidence[kind]={'peak_allocated_hour':datetime.fromtimestamp(h,TZ).isoformat(),'requests_in_hour':v[0],'eligible_hours':len(eligible),'logical_bytes_per_hour_at_30s':math.ceil(peak[kind][0]),'allocated_bytes_per_hour_at_30s':math.ceil(peak[kind][1])}
    remaining=max(0,(datetime.fromisoformat(plan['collection_end']).timestamp()-epoch)/3600)
    result={'measured_at':datetime.fromtimestamp(epoch,TZ).isoformat(),'service_date':day,'filesystem':{'total_bytes':fs.f_blocks*block,'used_bytes':(fs.f_blocks-fs.f_bfree)*block,'available_bytes':fs.f_bavail*block,'root_is_mount':os.path.ismount(root)},'files':{'logical_bytes':total[0],'allocated_bytes':total[1],'day_logical_bytes':day_size[0],'day_allocated_bytes':day_size[1]},'groups':{k:dict(zip(('logical_bytes','allocated_bytes','files'),v)) for k,v in groups.items()},'sampling_evidence':evidence,'unreadable_metadata':unreadable,'stat_errors':errors,'remaining_collection_hours':remaining,'future_service_dates':['2026-11-08','2026-11-21'],'assumptions':['All future days provisioned for at least28h of collection including overnight overlap; no deduplication assumed.','Peak completed-hour sample costs from actual30s records, normalized to120 samples/hour, plus25% variability.','Frozen static and SQLite budgeted at120% of measured size for each future day.','Conservative duplicate final ZIP for each day, plus space for one temporary ZIP,50MB reports and10% disk reserve (minimum100MB).','Estimate only: never starts collection, modifies snapshots, deletes data or changes resources.']}
    if len(peak)!=2 or errors or not os.path.ismount(root):
        result['decision']='UNVERIFIED';result['reason']='Need both feeds with complete30s-hour evidence, readable files and the actual mounted disk.';return result
    rate=[sum(v[i] for v in peak.values())*1.25 for i in (0,1)]
    day_final=[math.ceil(day_size[i]+remaining*rate[i]) for i in (0,1)]
    fixed=[sum(groups[k][i] for k in ('static','sqlite'))*1.2 for i in (0,1)]
    future_hours=max(28,(datetime.fromisoformat(plan['collection_end']).timestamp()-datetime.fromisoformat(plan['collection_start']).timestamp())/3600)
    future_day=[math.ceil(fixed[i]+future_hours*rate[i]) for i in (0,1)]
    zip7=math.ceil(day_final[0]*1.02+1_000_000)
    future_zip=math.ceil(future_day[0]*1.02+1_000_000)
    zip7_existing=root/('ctm-'+day+'.zip')
    additional_zip7=max(0,zip7-(sizes(zip7_existing)[1] if zip7_existing.exists() else 0))
    reserve=max(100_000_000,math.ceil(result['filesystem']['total_bytes']*.1))+50_000_000
    temporary=max(zip7,future_zip)
    extra=math.ceil(remaining*rate[1])+additional_zip7+14*(future_day[1]+future_zip)+temporary+reserve
    margin=result['filesystem']['available_bytes']-extra
    result['projection']={'day7_final_logical_bytes':day_final[0],'day7_final_allocated_bytes':day_final[1],'day7_duplicate_zip_budget_bytes':zip7,'future_daily_hours':future_hours,'future_daily_allocated_bytes':future_day[1],'future_daily_zip_budget_bytes':future_zip,'fourteen_days_allocated_including_zips_bytes':14*(future_day[1]+future_zip),'temporary_zip_bytes':temporary,'reserve_and_report_bytes':reserve,'additional_bytes_required':extra,'remaining_margin_bytes':margin}
    result['decision']='SUFFICIENT_WITH_MODEL_MARGIN' if margin>=0 else 'INSUFFICIENT_WITH_MODEL_MARGIN'
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',default='/var/data');p.add_argument('--day',default='2026-11-07');a=p.parse_args()
    print(json.dumps(audit(a.root,a.day),ensure_ascii=False,indent=2))
