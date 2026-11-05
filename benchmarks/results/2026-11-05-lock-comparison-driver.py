import hashlib, itertools, json, os, platform, statistics, subprocess, tempfile, time
from pathlib import Path
binaries = {'baseline':'/tmp/emda-controlled-base-target/release/examples/controlled_lock_bench','candidate':'/tmp/emda-fair-target/release/examples/controlled_lock_bench'}
source = '/tmp/emda-fair-locks/examples/controlled_lock_bench.rs'
source_base = '/tmp/emda-controlled-bench-base/examples/controlled_lock_bench.rs'
def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
assert sha(source) == sha(source_base)
affinity_before = sorted(os.sched_getaffinity(0))
selected = affinity_before[:min(8,len(affinity_before))]
os.sched_setaffinity(0,set(selected))
result = {'schema':1,'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()), 'kernel':platform.platform(), 'affinity_before':affinity_before,'affinity_used':selected,'source_sha256':sha(source),'binary_sha256':{k:sha(v) for k,v in binaries.items()},'runs':[]}
output = Path('/tmp/emda-fair-lock-comparison.json')
scenarios = list(itertools.product(['ordinary','checkpoint'],['plain','controlled'],['idle','contended'],[1,4]))
for location,parent in [('tmpfs','/tmp'),('workspace','/workspace')]:
    for index,(backend,mode,workload,clients) in enumerate(scenarios):
        for repeat in range(3):
            order = ['baseline','candidate'] if repeat%2==0 else ['candidate','baseline']
            for version in order:
                record = {'location':location,'backend':backend,'mode':mode,'workload':workload,'clients':clients,'repeat':repeat,'version':version}
                with tempfile.TemporaryDirectory(prefix='emda-admission-bench-',dir=parent) as directory:
                    start=time.monotonic()
                    try:
                        proc=subprocess.run([binaries[version],backend,mode,workload,str(clients),'300',directory],capture_output=True,text=True,timeout=20)
                        record.update(elapsed_s=time.monotonic()-start,exit=proc.returncode)
                        if proc.returncode == 0:
                            record['result']=json.loads(proc.stdout)
                        else:
                            record['stderr']=proc.stderr[-4000:]
                    except subprocess.TimeoutExpired as error:
                        record.update(elapsed_s=time.monotonic()-start,timeout=True,stderr=str(error))
                result['runs'].append(record)
        output.write_text(json.dumps(result))
        print(f'{location} {index+1}/{len(scenarios)} failures={sum(1 for r in result["runs"] if r.get("timeout") or r.get("exit") != 0)}',flush=True)
