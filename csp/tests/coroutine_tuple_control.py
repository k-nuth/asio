#!/usr/bin/env python3
"""Characterize a compiler lifetime issue and verify explicit tuple ownership.

No Asio or channel implementation participates in this standard-C++ control.
GCC 16.1 -O1 on the measured Linux system retains the first direct binding;
Clang does not reproduce it. Both must release all owners with named storage.
"""
from pathlib import Path
import os
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=True)
cxx = os.environ.get('CXX', 'c++')
(out / 'compiler.log').write_text(subprocess.check_output([cxx, '--version'], text=True))
for mode in ('direct', 'named'):
    binary = out / mode
    cmd = [cxx, '-std=c++20', '-O1', '-g']
    if mode == 'named':
        cmd += ['-DNAMED_TUPLE_STORAGE']
    cmd += [str(root / 'csp/tests/coroutine_tuple_lifetime.cpp'), '-o', str(binary)]
    subprocess.run(cmd, check=True)
    run = subprocess.run([str(binary)], capture_output=True, text=True, timeout=10)
    (out / (mode + '.log')).write_text(run.stdout + run.stderr + '\nexit=' + str(run.returncode) + '\n')
    if mode == 'named':
        assert run.returncode == 0 and 'owners=0,0' in run.stdout
    elif run.returncode:
        assert run.returncode == 2 and 'owners=1,0' in run.stdout
        print('Compiler-only direct binding RED: first owner retained after natural scope exit')
    else:
        assert 'owners=0,0' in run.stdout
        print('Compiler-only direct binding did not reproduce on this compiler')
print('Same ownership scenario GREEN with named tuple storage; no explicit cleanup or diagnostic suppression')
