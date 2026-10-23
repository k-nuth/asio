#!/usr/bin/env python3
"""Deterministic original RED/current GREEN controls, without timing thresholds."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=True)
baseline = root / 'csp/bench/reference/asio/experimental/channel_select.hpp'
assert hashlib.sha256(baseline.read_bytes()).hexdigest() == '1a553194765e1e73513f4584442fd03da12b4d55a932d3bd00c08be70d26e27b'
for name in ('ready_path', 'pending_path'):
    for mode in ('baseline', 'fixed'):
        binary = out / (mode + '-' + name)
        cmd = [os.environ.get('CXX', 'c++'), '-std=c++20', '-pthread', '-DASIO_STANDALONE']
        if mode == 'baseline':
            cmd += ['-I' + str(root / 'csp/bench/reference')]
        cmd += ['-I' + str(root / 'include'), str(root / 'csp/tests' / (name + '.cpp')), '-o', str(binary)]
        subprocess.run(cmd, check=True)
        run = subprocess.run([str(binary)], capture_output=True, text=True, timeout=15)
        text = run.stdout + run.stderr
        (out / (mode + '-' + name + '.log')).write_text(text + '\nexit=' + str(run.returncode) + '\n')
        if mode == 'baseline':
            expected = 'executor_turns=514' if name == 'ready_path' else 'pending=1 reactor=1'
            assert run.returncode != 0 and expected in text, (name, text)
        else:
            assert run.returncode == 0, (name, text)
print('Original ready/pending mechanisms RED; identical public scenarios GREEN, without timing thresholds')

# The second iteration preserves its preceding optimized implementation too.
v1 = root / 'csp/bench/reference-v1/asio/experimental/channel_select.hpp'
assert hashlib.sha256(v1.read_bytes()).hexdigest() == 'f299765376c3cecca2d5520dac5c7858c8628df3b07b583f39d73f4eba5a3612'
for label, overlay in [('v1', v1.parents[2]), ('current', root / 'include')]:
    binary = out / (label + '-arbitration')
    subprocess.run([os.environ.get('CXX', 'c++'), '-std=c++20', '-pthread', '-DASIO_STANDALONE',
                    '-I' + str(overlay), '-I' + str(root / 'include'),
                    str(root / 'csp/tests/arbitration.cpp'), '-o', str(binary)], check=True)
    subprocess.run([str(binary)], check=True, timeout=15)
print('Preceding/current seeded arbitration and losing queues remain compatible')
