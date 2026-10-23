#!/usr/bin/env python3
"""Run binaries serially in randomized order; retain every observation."""
import argparse
import hashlib
import json
import os
import platform
import random
import statistics
import subprocess
import time
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--bin-dir', type=Path, default=Path(__file__).parent / 'build')
p.add_argument('--n', type=int, default=30000)
p.add_argument('--repeats', type=int, default=7)
p.add_argument('--engines', default='asio-baseline,asio,go,crossbeam,tokio')
p.add_argument('--out', type=Path, default=Path(__file__).parent / 'results')
a = p.parse_args()
if a.n < 1 or a.repeats < 1:
    p.error('--n and --repeats must be positive')
root = Path(__file__).resolve().parents[2]
bench = Path(__file__).resolve().parent
a.bin_dir = a.bin_dir.resolve()
a.out = a.out.resolve()
a.out.mkdir(parents=True, exist_ok=True)

def call(cmd):
    return subprocess.check_output([str(c) for c in cmd], text=True, stderr=subprocess.STDOUT, timeout=120).strip()

def digest(file):
    return hashlib.sha256(file.read_bytes()).hexdigest()

all_engines = {'asio-baseline': [a.bin_dir / 'asio-baseline-bench'],
               'asio': [a.bin_dir / 'asio-bench'], 'go': [a.bin_dir / 'go-bench'],
               'crossbeam': [bench / 'rust/target/release/select-bench', 'crossbeam'],
               'tokio': [bench / 'rust/target/release/select-bench', 'tokio']}
try:
    engines = {e: all_engines[e] for e in a.engines.split(',')}
except KeyError as e:
    p.error('unknown engine: ' + str(e))
scenarios = [('ready', 1, 1), ('mixed', 1, 1), ('fanin', 0, 1), ('fanin', 0, 4),
             ('fanin', 1, 1), ('fanin', 1, 4), ('fanin', 64, 1), ('fanin', 64, 4)]
cases = [(e, m, c, w) for e in engines for m, c, w in scenarios
         if not (e == 'tokio' and c == 0)
         and not (e == 'crossbeam' and m == 'fanin' and w == 1)]
files = [bench / 'select.cpp', bench / 'select.go', bench / 'rust/src/main.rs',
         bench / 'rust/Cargo.toml', bench / 'rust/Cargo.lock', bench / 'build.py', Path(__file__),
         root / 'include/asio/experimental/channel_select.hpp',
         bench / 'reference/asio/experimental/channel_select.hpp']
source_hashes = {str(f.relative_to(root)): digest(f) for f in files}
build_file = a.bin_dir / 'build.json'
build = json.loads(build_file.read_text()) if build_file.exists() else None
if build:
    for file, expected in build['source_sha256'].items():
        if digest(root / file) != expected:
            p.error('source differs from build metadata; rebuild first: ' + file)
machine = platform.processor()
if platform.system() == 'Darwin':
    machine = call(['sysctl', '-n', 'machdep.cpu.brand_string'])
elif Path('/proc/cpuinfo').exists():
    machine = next((s.split(':', 1)[1].strip() for s in Path('/proc/cpuinfo').read_text().splitlines()
                    if s.startswith('model name')), machine)
meta = dict(timestamp=time.strftime('%Y-%m-%dT%H:%M:%S%z'), platform=platform.platform(), machine=machine,
            cpu_count=os.cpu_count(), affinity=sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
            head=call(['git', '-C', root, 'rev-parse', 'HEAD']), tree=call(['git', '-C', root, 'rev-parse', 'HEAD^{tree}']),
            status=call(['git', '-C', root, 'status', '--short']), n=a.n, repeats=a.repeats,
            source_sha256=source_hashes, build=build,
            binary_sha256={e: digest(Path(cmd[0])) for e, cmd in engines.items()})
(a.out / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
records = []
# Separate discarded processes warm system caches; timed loops remain uninstrumented.
for e, m, c, w in cases:
    call(engines[e] + [m, str(min(a.n, 3000)), str(c), str(w)])
jobs = cases * a.repeats
random.Random(1907).shuffle(jobs)
for e, m, c, w in jobs:
    cmd = engines[e] + [m, str(a.n), str(c), str(w)]
    output = call(cmd)
    ns, *counts = map(int, output.split(','))
    ops = a.n * (3 if m == 'fanin' else 1)
    assert len(counts) == 3 and sum(counts) == ops and ns > 0
    if m == 'fanin':
        assert counts == [a.n] * 3
    records.append(dict(engine=e, mode=m, capacity=c, workers=w, n=a.n, ns=ns,
                        ns_per_message=ns / ops, counts=counts, command=list(map(str, cmd))))
    print(f'{len(records)}/{len(jobs)} {e} {m} c={c} w={w}: {ns / ops:.1f} ns/msg', flush=True)
    (a.out / 'raw.json').write_text(json.dumps(records, indent=2) + '\n')
for file, expected in source_hashes.items():
    assert digest(root / file) == expected, 'source changed during run: ' + file
rows = []
for e, m, c, w in cases:
    vals = [r['ns_per_message'] for r in records
            if (r['engine'], r['mode'], r['capacity'], r['workers']) == (e, m, c, w)]
    median = statistics.median(vals)
    rows.append(dict(engine=e, mode=m, capacity=c, workers=w, median_ns=median,
                     min_ns=min(vals), max_ns=max(vals), messages_per_second=1e9 / median))
(a.out / 'summary.json').write_text(json.dumps(rows, indent=2) + '\n')
lines = ['| Engine | Scenario | Capacity | Workers | Median ns/message | Min–max |',
         '|---|---|---:|---:|---:|---:|']
for r in rows:
    lines.append(f"| {r['engine']} | {r['mode']} | {r['capacity']} | {r['workers']} | "
                 f"{r['median_ns']:.1f} | {r['min_ns']:.1f}–{r['max_ns']:.1f} |")
(a.out / 'table.md').write_text('\n'.join(lines) + '\n')
print('\n'.join(lines))
