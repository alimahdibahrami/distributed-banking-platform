#!/usr/bin/env python3
"""Read-only host verification of isolated crash matrix final invariants."""
import json
import subprocess
import urllib.request
from pathlib import Path
assert 'LAB_ROUTING_EPOCH=2\n' in Path('.env').read_text()
p=subprocess.run(['bash','scripts/operator.sh','status'],input='',text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,check=True)
print(p.stdout,flush=True)
row=next(x for x in p.stdout.splitlines() if x.startswith('BONUS10E4D_STATUS: '))
d=json.loads(row.split(': ',1)[1]); j=d['journal']; s=d['shards']
assert j['phase']=='COMPLETED' and j['pending_id'] is None and j['pending_phase'] is None
assert j['transition_count']==6 and d['gateway_epoch']==2 and d['coordinator_pending']==0
assert [s[k]['accounts'] for k in 'ABC']==[1000,500,500]
assert sum(s[k]['total'] for k in 'ABC')==1999500
assert all(s[k]['prepared']==0 for k in 'ABC')
with urllib.request.urlopen('http://127.0.0.1:18082/ready',timeout=5) as r: ready=json.load(r)
assert ready['healthy_replicas']=={'A':2,'B':2,'C':2}
assert ready['routing_epoch']==2 and ready['epoch_coherent'] and ready['banking_ready']
with urllib.request.urlopen('http://127.0.0.1:8001/ready',timeout=5) as r: original=json.load(r)
assert original['banking_ready'] and original['healthy_replicas']=={'A':2,'B':2}
print('BONUS10E5_POST_CRASH_FINAL_INVARIANTS_PASS')
