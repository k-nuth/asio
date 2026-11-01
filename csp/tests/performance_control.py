#!/usr/bin/env python3
"""Deterministic original RED/current GREEN controls, without timing thresholds."""
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=True)
cxx = os.environ.get('CXX', 'c++')
bench = root / 'csp/tests/fixtures'
# Frozen headers, and the unmodified upstream awaitable implementation that
# every frozen header was measured against.
upstream = bench / 'reference-upstream'
frozen = {bench / 'reference/asio/experimental/channel_select.hpp':
              '1a553194765e1e73513f4584442fd03da12b4d55a932d3bd00c08be70d26e27b',
          bench / 'reference-v1/asio/experimental/channel_select.hpp':
              'f299765376c3cecca2d5520dac5c7858c8628df3b07b583f39d73f4eba5a3612',
          bench / 'reference-v2/asio/experimental/channel_select.hpp':
              '36e924196e8e1dcf0dc8d4cd8df6c51873eb5e06000018369d02239ad5ce77e5',
          bench / 'reference-v3/asio/experimental/channel_select.hpp':
              'c469b0ec77bff998096a48ca7e866aa75258b5f2e448f6f771084f7d9c3e7788',
          bench / 'reference-v3/asio/impl/awaitable.hpp':
              '66bf427516c0df91bf28bfdf0565372d3454ed7e6b9b840234e236f73258fd73',
          upstream / 'asio/impl/awaitable.hpp':
              '5ef353a54789a676723c62f62f041a24d07b4f69ce25fa9587b002ef69ad0657'}
for path, digest in frozen.items():
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, path


def build(name, overlays, binary, defines=()):
    cmd = [cxx, '-std=c++20', '-pthread', '-DASIO_STANDALONE', *defines]
    cmd += ['-I' + str(overlay) for overlay in overlays]
    cmd += ['-I' + str(root / 'include'), str(root / 'csp/tests' / (name + '.cpp')), '-o', str(binary)]
    subprocess.run(cmd, check=True)


def run(binary, log):
    result = subprocess.run([str(binary)], capture_output=True, text=True, timeout=15)
    text = result.stdout + result.stderr
    log.write_text(text + '\nexit=' + str(result.returncode) + '\n')
    return result.returncode, text


for name in ('ready_path', 'pending_path'):
    for mode in ('baseline', 'fixed'):
        binary = out / (mode + '-' + name)
        build(name, [bench / 'reference', upstream] if mode == 'baseline' else [], binary)
        code, text = run(binary, out / (mode + '-' + name + '.log'))
        if mode == 'baseline':
            expected = 'executor_turns=514' if name == 'ready_path' else 'pending=1 reactor=1'
            assert code != 0 and expected in text, (name, text)
        else:
            assert code == 0, (name, text)
print('Original ready/pending mechanisms RED; identical public scenarios GREEN, without timing thresholds')

# Third iteration: a ready selection awaited through as_tuple(asio::deferred).
# The preceding header runs that token as a generic deferred operation: a start
# turn, a posted completion and an owned operation allocation per selection.
# The current header commits in the awaiting coroutine's frame.
for mode in ('v2', 'current'):
    binary = out / (mode + '-immediate_path')
    build('immediate_path', [bench / 'reference-v2', upstream] if mode == 'v2' else [], binary)
    code, text = run(binary, out / (mode + '-immediate_path.log'))
    turns = int(re.search(r'executor_turns=(\d+)', text).group(1))
    allocations = int(re.search(r'heap_allocations=(\d+)', text).group(1))
    if mode == 'v2':
        assert code != 0 and turns >= 512 and allocations >= 256, (turns, allocations, text)
    else:
        assert code == 0 and turns < 20 and allocations <= 4, (turns, allocations, text)
print('Preceding deferred-token path RED (turn and allocation per selection); immediate path GREEN')

# Fourth iteration: a loop of suspended selections. The preceding header's
# one-shot operation is the only path it offers; with the same construction
# observer inserted into its operation constructor (test-only, no behavior
# change), it constructs one owned operation per suspended selection. The
# task-owned reusable selector constructs none and allocates nothing per
# suspension; its generation isolation is checked by the same binary.
v3 = bench / 'reference-v3'
v3_text = (v3 / 'asio/experimental/channel_select.hpp').read_text()
v3_anchor = '    std::iota(order.begin(), order.end(), 0);\n  }\n  virtual ~operation() = default;'
assert v3_text.count(v3_anchor) == 1
observed = out / 'v3-observed'
(observed / 'asio/experimental').mkdir(parents=True, exist_ok=True)
(observed / 'asio/experimental/channel_select.hpp').write_text(v3_text.replace(v3_anchor,
    '    std::iota(order.begin(), order.end(), 0);\n#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_OPERATION)\n'
    '    ASIO_CHANNEL_SELECT_TEST_OBSERVE_OPERATION(this);\n#endif\n  }\n  virtual ~operation() = default;'))
for mode in ('v3', 'current'):
    binary = out / (mode + '-reusable_path')
    build('reusable_path', [observed, v3] if mode == 'v3' else [], binary, ('-DONE_SHOT_ONLY',) if mode == 'v3' else ())
    code, text = run(binary, out / (mode + '-reusable_path.log'))
    one_shot = int(re.search(r'one_shot_loop_operations=(\d+)', text).group(1))
    looped = int(re.search(r'selector_loop_operations=(\d+)', text).group(1))
    if mode == 'v3':
        assert code != 0 and one_shot >= 254 and looped >= 254, (one_shot, looped, text)
    else:
        allocations = int(re.search(r'selector_loop_heap_allocations=(\d+)', text).group(1))
        assert code == 0 and one_shot >= 254 and looped == 0 and allocations == 0, (one_shot, looped, allocations, text)
print('Preceding one-shot loop RED (an owned operation per suspended selection); reusable selector loop GREEN')

# Each iteration preserves its preceding optimized implementation too.
for label, overlays in [('v1', [bench / 'reference-v1', upstream]),
                        ('v2', [bench / 'reference-v2', upstream]), ('v3', [v3]), ('current', [])]:
    binary = out / (label + '-arbitration')
    build('arbitration', overlays, binary)
    subprocess.run([str(binary)], check=True, timeout=15)
print('Preceding/current seeded arbitration and losing queues remain compatible')
