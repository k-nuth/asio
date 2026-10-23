#!/usr/bin/env python3
"""Run the same lifetime scenario on the frozen defective input and the fix."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=True)
fixture = root / 'csp/tests/fixtures/reference-v4/asio/experimental/channel_select.hpp'
expected = '88cee7151f630ef3062cd6b25a0d8fff1720f515b2b50ac0ab27bd7b1b8b7640'
assert hashlib.sha256(fixture.read_bytes()).hexdigest() == expected
source = fixture.read_text()

def insert_once(needle, replacement):
    global source
    assert source.count(needle) == 1, needle
    source = source.replace(needle, replacement)

insert_once('  virtual ~operation() = default;', '''  virtual ~operation() {
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_FREE)
    ASIO_CHANNEL_SELECT_TEST_OBSERVE_FREE(this);
#endif
  }''')
insert_once('      if (node->identity->claim(node->index)) { selected = node; return node->identity; }', '''#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_CLAIM)
      if (!ASIO_CHANNEL_SELECT_TEST_OBSERVE_CLAIM(node->identity)) return nullptr;
#endif
      if (node->identity->claim(node->index)) { selected = node; return node->identity; }''')
overlay = out / 'baseline/asio/experimental/channel_select.hpp'
overlay.parent.mkdir(parents=True, exist_ok=True)
overlay.write_text(source)
for mode, expected_code, marker in [('baseline', 1, 'RED:'), ('fixed', 0, 'GREEN:')]:
    binary = out / (mode + '-control')
    command = [os.environ.get('CXX', 'c++'), '-std=c++20', '-O2', '-pthread', '-DASIO_STANDALONE']
    if mode == 'baseline':
        command += ['-I' + str(out / 'baseline')]
    command += ['-I' + str(root / 'include'), str(root / 'csp/tests/finalize_withdraw_race.cpp'), '-o', str(binary)]
    subprocess.run(command, check=True)
    # A timeout is only a harness failure watchdog, never evidence of success.
    run = subprocess.run([str(binary)], capture_output=True, text=True, timeout=60)
    output = run.stdout + run.stderr
    (out / (mode + '.log')).write_text(output)
    assert run.returncode == expected_code and marker in output and 'all threads joined' in output, output
    print(output, end='')
