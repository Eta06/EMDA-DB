#!/usr/bin/env python3
"""Summarize retained comparison runs without excluding noise or failed-run records.

Usage: python3 scripts/summarize_engines.py INPUT.json OUTPUT.json
Latencies are medians of run-level quantiles, not pooled latency quantiles.
"""
import json, statistics, sys
from pathlib import Path
from collections import defaultdict

def summary(path):
 data=json.loads(Path(path).read_text()); groups=defaultdict(list)
 for r in data['runs']:
  if 'result' in r: groups[(r['backend'],r['case']['name'],r['mode'])].append(r)
 out={'revision':data['revision'],'successful_runs':sum(map(len,groups.values())),'failures':data['failures'],'groups':[]}
 for (backend,case,mode),runs in groups.items():
  g={'backend':backend,'case':case,'mode':mode,'runs':len(runs),'metrics':{},'metadata':runs[0]['result']['metadata']}
  for metric in ['ops_per_sec']+[f'{op}.{p}' for op in ['read','write','create','update','delete'] for p in ['p50_us','p95_us','p99_us','p99_9_us','max_us']]:
   vals=[]
   for r in runs:
    v=r['result']
    for part in metric.split('.'): v=v.get(part,{})
    if isinstance(v,(int,float)): vals.append(v)
   if vals:
    g['metrics'][metric]={'median':statistics.median(vals),'min':min(vals),'max':max(vals),'mean':statistics.mean(vals),'sample_cv_percent':statistics.stdev(vals)/statistics.mean(vals)*100 if len(vals)>1 and statistics.mean(vals) else 0}
  g['quiet_failures']=sum(not r['pre_run_quiet']['quiet'] for r in runs)
  g['throttled_runs']=sum(r['result']['noise']['cgroup_cpu_delta'].get('nr_throttled',0)>0 for r in runs)
  g['memory_limit_runs']=sum(r['result']['noise'].get('memory_events_delta',{}).get('max',0)>0 for r in runs)
  g['direct_reclaim_runs']=sum(r['result']['noise'].get('memory_reclaim_delta',{}).get('pgscan_direct',0)>0 for r in runs)
  g['max_steal_percent']=max(r['result']['noise'].get('vm_steal_percent') or 0 for r in runs)
  g['retry_counts']=[r['result']['metadata'].get('conflict_retries',0) for r in runs]
  out['groups'].append(g)
 return out

if __name__=='__main__':
 out=summary(sys.argv[1]);Path(sys.argv[2]).write_text(json.dumps(out,indent=2)+'\n')
 print('successful',out['successful_runs'],'failed',len(out['failures']))
 for g in out['groups']:
  if g['case']=='concurrent_mixed':
   print(g['backend'],g['mode'],*[f'{metric}={g["metrics"][metric]["median"]:.1f}' for metric in ['ops_per_sec','read.p50_us','write.p50_us','write.p99_us']])
 print('noise', {key:sum(g[key] for g in out['groups']) for key in ['quiet_failures','throttled_runs','memory_limit_runs','direct_reclaim_runs']})
