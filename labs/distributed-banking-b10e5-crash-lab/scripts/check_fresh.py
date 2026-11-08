#!/usr/bin/env python3
"""Fail closed if this isolated DB/Journal has ever begun a migration.
A stopped matrix must be repaired step by step, not repeated.
"""
import json
import subprocess
import sys
import urllib.request
from pathlib import Path
config=json.loads(Path('config/active_map.json').read_text())
assert config['version']==1
proc=subprocess.run(['bash','scripts/operator.sh','status'],input='',text=True,
                    stdout=subprocess.PIPE,stderr=subprocess.STDOUT,check=True)
row=next((x for x in proc.stdout.splitlines() if x.startswith('BONUS10E4D_STATUS: ')),None)
if row is None: raise SystemExit('NO_JOURNAL_STATUS')
d=json.loads(row.split(': ',1)[1]);j=d['journal'];s=d['shards']
if not (j['phase']=='PLANNED' and j['transition_count']==0 and
        j['pending_id'] is None and j['pending_phase'] is None and
        d.get('gateway_epoch')==1 and d.get('coordinator_pending')==0 and
        [s[k]['accounts'] for k in 'ABC']==[1000,1000,0] and
        sum(s[k]['total'] for k in 'ABC')==1999500 and
        all(s[k]['prepared']==0 for k in 'ABC')):
    print('B10E5_NOT_FRESH_DO_NOT_REPEAT_MATRIX',file=sys.stderr)
    raise SystemExit(2)
print('BONUS10E5_FRESH_LAB_INVARIANTS_PASS')
