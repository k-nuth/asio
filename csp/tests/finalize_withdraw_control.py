#!/usr/bin/env python3
"""Run the same lifetime scenario on the frozen defective input and the fix.

The frozen pre-fix header receives only the control's observation hooks, at the
same points where the current header carries them: operation destruction, the
peer's claim gate, the marker before the withdrawal step, and the attempt and
acquisition of each channel mutex inside both of its lock primitives (the
all-channel lock set and the per-node withdrawal lock). The verdict is the
delivered order of those events, so the defective input must report RED and
the current header GREEN from identical observation points.
"""
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
insert_once('    withdraw_cases();\n', '''#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCKING)
    ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCKING(this);
#endif
    withdraw_cases();
''')
insert_once('''      if (channels.size() <= local.size()) {
        for (std::size_t i = 0; i < channels.size(); ++i)
          local[i] = std::unique_lock<std::mutex>(channels[i]->mutex);
      } else {
        extra.reserve(channels.size());
        for (auto* c : channels) extra.emplace_back(c->mutex);
      }''', '''      if (channels.size() <= local.size()) {
        for (std::size_t i = 0; i < channels.size(); ++i) {
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ATTEMPT)
          ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ATTEMPT(&channels[i]->mutex);
#endif
          local[i] = std::unique_lock<std::mutex>(channels[i]->mutex);
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ACQUIRED)
          ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ACQUIRED(&channels[i]->mutex);
#endif
        }
      } else {
        extra.reserve(channels.size());
        for (auto* c : channels) {
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ATTEMPT)
          ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ATTEMPT(&c->mutex);
#endif
          extra.emplace_back(c->mutex);
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ACQUIRED)
          ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ACQUIRED(&c->mutex);
#endif
        }
      }''')
insert_once('''    auto* l = list.get();
    if (!l) return;
    std::lock_guard<std::mutex> lock(*l->mutex);
    unlink(removing);''', '''    auto* l = list.get();
    if (!l) return;
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ATTEMPT)
    ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ATTEMPT(l->mutex);
#endif
    std::lock_guard<std::mutex> lock(*l->mutex);
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ACQUIRED)
    ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ACQUIRED(l->mutex);
#endif
    unlink(removing);''')
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
