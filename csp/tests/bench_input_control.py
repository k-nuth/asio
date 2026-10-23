#!/usr/bin/env python3
"""Real original RED/current GREEN build+run pipeline after manifest/flag changes.

Requires the benchmark's Go/Cargo/C++ toolchains. Uses disposable local clones;
never edits production inputs. Original scripts are the unmodified c834f8b files.
"""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess

p = argparse.ArgumentParser()
p.add_argument('out', type=Path)
p.add_argument('--go', required=True)
p.add_argument('--cxx', default=os.environ.get('CXX', 'c++'))
a = p.parse_args()
root = Path(__file__).resolve().parents[2]
a.out = a.out.resolve()
a.out.mkdir(parents=True, exist_ok=True)
tracked = [root / f for f in ['csp/bench/build.py', 'csp/bench/run.py', 'csp/bench/rust/Cargo.toml']]
original = {p: p.read_bytes() for p in tracked}
for mode in ('baseline', 'fixed'):
    fixture = a.out / mode
    assert not fixture.exists(), 'use a fresh output directory'
    subprocess.run(['git', 'clone', '--shared', '--quiet', str(root), str(fixture)], check=True)
    # Source and toolchain inputs are identical in both fixtures. Only the
    # original versus fixed production scripts differ.
    for name in ['build.py', 'run.py', 'select.cpp']:
        src = root / 'csp/bench' / name
        if mode == 'baseline' and name in ('build.py', 'run.py'):
            src = root / 'csp/bench/reference' / name.replace('.py', '-original.py')
        shutil.copy2(src, fixture / 'csp/bench' / name)
    with (a.out / (mode + '-build.log')).open('w') as log:
        subprocess.run(['python3', 'csp/bench/build.py', '--cxx', a.cxx, '--go', a.go],
                       cwd=fixture, stdout=log, stderr=subprocess.STDOUT, check=True)
    if mode == 'fixed':
        metadata = fixture / 'csp/bench/build/build.json'
        current_metadata = metadata.read_bytes()
        metadata.write_bytes((a.out / 'baseline/csp/bench/build/build.json').read_bytes())
        try:
            run = subprocess.run(['python3', 'csp/bench/run.py', '--engines', 'asio', '--n', '1', '--repeats', '1'],
                                 cwd=fixture, capture_output=True, text=True, timeout=15)
            (a.out / 'fixed-legacy-metadata.log').write_text(run.stdout + run.stderr)
            assert run.returncode == 2 and 'unsupported build metadata' in run.stderr
        finally:
            metadata.write_bytes(current_metadata)
    for target in ('manifest', 'flags'):
        file = fixture / ('csp/bench/rust/Cargo.toml' if target == 'manifest' else 'csp/bench/build.py')
        before = file.read_text()
        changed = before + '\n[profile.release]\nopt-level = 0\n' if target == 'manifest' else before.replace("'-O3'", "'-O2'")
        assert changed != before
        file.write_text(changed)
        try:
            engine = 'tokio' if target == 'manifest' else 'asio'
            expected_progress = '6/6' if target == 'manifest' else '8/8'
            run = subprocess.run(['python3', 'csp/bench/run.py', '--engines', engine, '--n', '1', '--repeats', '1',
                                  '--out', str(a.out / (mode + '-' + target + '-observations'))],
                                 cwd=fixture, capture_output=True, text=True, timeout=30)
            text = run.stdout + run.stderr
            (a.out / (mode + '-' + target + '.log')).write_text(text + '\nexit=' + str(run.returncode) + '\n')
            if mode == 'baseline':
                assert run.returncode == 0 and expected_progress in text, text  # Wrongly accepts old binaries.
            else:
                assert run.returncode == 2 and 'source differs from build metadata' in text, text
        finally:
            file.write_text(before)
assert all(p.read_bytes() == data for p, data in original.items())
print('Original scripts RED (stale binaries accepted); current scripts GREEN (manifest and flags rejected)')
