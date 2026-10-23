#!/usr/bin/env python3
"""Build every comparison without replacing system toolchains."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

p = argparse.ArgumentParser()
p.add_argument('--out', type=Path, default=Path(__file__).parent / 'build')
p.add_argument('--cxx', default=os.environ.get('CXX', 'c++'))
p.add_argument('--go', default=shutil.which('go'))
p.add_argument('--cargo', default=shutil.which('cargo') or str(Path.home() / '.cargo/bin/cargo'))
a = p.parse_args()
if not a.go:
    p.error('Go is required: pass --go /path/to/go')
root = Path(__file__).resolve().parents[2]
bench = Path(__file__).resolve().parent
a.out = a.out.resolve()
a.out.mkdir(parents=True, exist_ok=True)
commands = []
for name, overlay in [('asio', []), ('asio-baseline', ['-I' + str(bench / 'reference')])]:
    commands.append([a.cxx, '-std=c++20', '-O3', '-DNDEBUG', '-DASIO_STANDALONE', '-pthread',
                     *overlay, '-I' + str(root / 'include'), str(bench / 'select.cpp'),
                     '-o', str(a.out / (name + '-bench'))])
commands += [[a.go, 'build', '-o', str(a.out / 'go-bench'), str(bench / 'select.go')],
             [a.cargo, 'build', '--release', '--locked', '--manifest-path', str(bench / 'rust/Cargo.toml')]]
for cmd in commands:
    print(' '.join(cmd), flush=True)
    subprocess.run(cmd, cwd=root, check=True)
versions = {}
for name, cmd in [('cxx', [a.cxx, '--version']), ('go', [a.go, 'version']), ('cargo', [a.cargo, '--version']),
                  ('rustc', [os.environ.get('RUSTC', shutil.which('rustc') or str(Path.home() / '.cargo/bin/rustc')), '--version'])]:
    versions[name] = subprocess.check_output(cmd, text=True).strip()
files = [root / 'include/asio/experimental/channel_select.hpp', bench / 'reference/asio/experimental/channel_select.hpp',
         bench / 'select.cpp', bench / 'select.go', bench / 'rust/src/main.rs', bench / 'rust/Cargo.lock']
metadata = dict(commands=commands, versions=versions,
                source_sha256={str(f.relative_to(root)): hashlib.sha256(f.read_bytes()).hexdigest() for f in files})
(a.out / 'build.json').write_text(json.dumps(metadata, indent=2) + '\n')
