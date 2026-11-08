#!/usr/bin/env python3
"""Measure real account HTTP read plus /ready during controlled publication.
This is not proof of zero downtime and never retries financial writes.
"""
import json
import subprocess
import time
import urllib.request
from pathlib import Path
LAB='http://127.0.0.1:18082'
OUT=Path('publish_availability.json')
def probe(endpoint):
    begin=time.monotonic()
    try:
        with urllib.request.urlopen(LAB+endpoint,timeout=0.7) as r:
            data=json.loads(r.read())
            if endpoint=='/ready':
                good=(r.status==200 and data.get('banking_ready') is True and data.get('routing_epoch')==2 and data.get('healthy_replicas')=={'A':2,'B':2,'C':2})
            else:
                good=(r.status==200 and data.get('shard')=='A')
            return good,str(r.status),round((time.monotonic()-begin)*1000,2)
    except Exception as exc:
        return False,type(exc).__name__,round((time.monotonic()-begin)*1000,2)
runtime_log = open('publish_runtime.log', 'w', encoding='utf-8')
proc=subprocess.Popen(['bash','scripts/publish_lab.sh'],stdin=subprocess.DEVNULL,
                      stdout=runtime_log,stderr=subprocess.STDOUT,text=True)
start=time.monotonic(); rows=[]
# bounded wait, keep all observations including failures
while time.monotonic()-start<180:
    t=time.monotonic()-start
    row={'elapsed_s':round(t,3)}
    for endpoint,key in (('/ready','ready'),('/api/v1/accounts/A0500/balance','read')):
        good,status,latency=probe(endpoint)
        row.update({key:good,key+'_status':status,key+'_latency_ms':latency})
    rows.append(row)
    if proc.poll() is not None and t>8:
        break
    time.sleep(0.15)
if proc.poll() is None:
    proc.kill()
    print('PUBLISH_TIMEOUT_KILLED_OPERATOR_WRAPPER_ONLY',flush=True)
proc.wait(timeout=10)
runtime_log.close()
output=Path('publish_runtime.log').read_text(encoding='utf-8',errors='replace')
print(output[-12000:],flush=True)
if proc.returncode!=0:
    raise SystemExit('PUBLISH_FAILED_EXIT_'+str(proc.returncode))
if len(rows)<5:
    raise SystemExit('INSUFFICIENT_AVAILABILITY_SAMPLES')
result={'samples':len(rows),'publish_exit':proc.returncode,'read_failures':sum(not r['read'] for r in rows),'ready_failures':sum(not r['ready'] for r in rows),'samples_data':rows}
for key in ('ready','read'):
    begin=None; durations=[]
    for r in rows:
        if not r[key] and begin is None: begin=r['elapsed_s']
        elif r[key] and begin is not None:
            durations.append(r['elapsed_s']-begin); begin=None
    if begin is not None: durations.append(rows[-1]['elapsed_s']-begin)
    result[key+'_max_failure_s_approx']=round(max(durations,default=0),3)
OUT.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
print('BONUS10E5_CONTROLLED_PUBLISH_AVAILABILITY:',json.dumps({k:v for k,v in result.items() if k!='samples_data'},sort_keys=True),flush=True)
print('ZERO_DOWNTIME_NOT_ASSERTED')
