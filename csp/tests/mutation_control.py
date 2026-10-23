#!/usr/bin/env python3
"""Reject a faithful losing-consumption mutation in an isolated include overlay.

Usage: python3 csp/tests/mutation_control.py OUTPUT_DIRECTORY
Production source is never modified. Compiler defaults to CXX or c++.
"""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]).resolve()
header = root / "include/asio/experimental/channel_select.hpp"
original = header.read_bytes()
anchor = "for (auto i : order) if (attempt(i)) { ready = true; break; }"
text = original.decode()
assert text.count(anchor) == 1
text = text.replace(anchor, "for (auto i : order) if (attempt(i)) { ready = true; }")
overlay = out / "asio/experimental"
overlay.mkdir(parents=True, exist_ok=True)
(overlay / header.name).write_text(text)
binary = out / "select-mutated"
subprocess.run([os.environ.get("CXX", "c++"), "-std=c++20", "-pthread",
                "-DASIO_STANDALONE", "-I" + str(out), "-I" + str(root / "include"),
                str(root / "csp/tests/select.cpp"), "-o", str(binary)], check=True)
with (out / "mutation.log").open("w") as log:
    result = subprocess.run([str(binary)], stdout=log, stderr=subprocess.STDOUT, timeout=15)
    log.write(f"\nexit={result.returncode}\n")
assert result.returncode != 0, "control failed to discriminate"
assert header.read_bytes() == original
print("Mutation rejected; unchanged header sha256=" + hashlib.sha256(original).hexdigest())

# Independently discriminate an eager-consumption mutation in the optimized
# coroutine probe. It commits every ready branch, then reports only the last.
probe = original.decode()
assert probe.count('  for (auto index : order) {') == 1
assert probe.count('    if (ready) return true;') == 1
probe = probe.replace('  for (auto index : order) {', '  bool committed = false;\n  for (auto index : order) {')
probe = probe.replace('    if (ready) return true;', '    if (ready) committed = true;')
probe = probe.replace('  if (options.nonblocking) { decision.error', '  if (committed) return true;\n  if (options.nonblocking) { decision.error')
probe_overlay = out / 'ready/asio/experimental'
probe_overlay.mkdir(parents=True, exist_ok=True)
(probe_overlay / header.name).write_text(probe)
probe_binary = out / 'ready-mutated'
subprocess.run([os.environ.get('CXX', 'c++'), '-std=c++20', '-pthread',
                '-DASIO_STANDALONE', '-I' + str(out / 'ready'), '-I' + str(root / 'include'),
                str(root / 'csp/tests/ready_path.cpp'), '-o', str(probe_binary)], check=True)
with (out / 'mutation-ready.log').open('w') as log:
    result = subprocess.run([str(probe_binary)], stdout=log, stderr=subprocess.STDOUT, timeout=15)
    log.write(f'\nexit={result.returncode}\n')
assert result.returncode != 0, 'ready-probe control failed to discriminate'
# The same probe serves the immediate path of the lazy deferred operation.
immediate_binary = out / 'immediate-mutated'
subprocess.run([os.environ.get('CXX', 'c++'), '-std=c++20', '-pthread',
                '-DASIO_STANDALONE', '-I' + str(out / 'ready'), '-I' + str(root / 'include'),
                str(root / 'csp/tests/immediate_path.cpp'), '-o', str(immediate_binary)], check=True)
with (out / 'mutation-immediate.log').open('w') as log:
    result = subprocess.run([str(immediate_binary)], stdout=log, stderr=subprocess.STDOUT, timeout=15)
    log.write(f'\nexit={result.returncode}\n')
assert result.returncode != 0, 'immediate-path control failed to discriminate'
assert header.read_bytes() == original
print('Ready-probe losing-consumption mutation rejected on both coroutine paths; production header unchanged')

# Generation isolation of the reusable selector. Each mutation lets one kind
# of request raised for a finished generation reach the next one. A stale
# cancellation and a stale timer resume are each filtered twice, at their
# ingress and in the runner, so both filters are removed; a stale peer
# wakeup has only its runner filter. The reusable-path control raises
# exactly those requests and must reject every mutation.
guards = [('cancel', [('    if (gen != generation.load(std::memory_order_acquire)) return;\n    raise(cancel_generation, gen);',
                      '    raise(cancel_generation, gen);'),
                     ('      if ((requested & turn_cancel) && cancel_generation.load(std::memory_order_relaxed) == gen)\n        cancel(asio::error::operation_aborted);',
                      '      if (requested & turn_cancel)\n        cancel(asio::error::operation_aborted);')]),
          ('resume', [('    if (gen != generation.load(std::memory_order_acquire)) return;\n    raise(resume_generation, gen);',
                      '    raise(resume_generation, gen);'),
                     ('      if ((requested & turn_resume) && resume_generation.load(std::memory_order_relaxed) == gen)\n        start(true);',
                      '      if (requested & turn_resume)\n        start(true);')]),
          ('wakeup', [('      if ((requested & turn_finalize) && finalize_generation.load(std::memory_order_relaxed) == gen)\n        finalize();',
                      '      if (requested & turn_finalize)\n        finalize();')])]
for label, replacements in guards:
    mutated = original.decode()
    for before, after in replacements:
        assert mutated.count(before) == 1, (label, before)
        mutated = mutated.replace(before, after)
    mutant_overlay = out / ('generation-' + label) / 'asio/experimental'
    mutant_overlay.mkdir(parents=True, exist_ok=True)
    (mutant_overlay / header.name).write_text(mutated)
    mutant_binary = out / ('generation-' + label + '-mutated')
    subprocess.run([os.environ.get('CXX', 'c++'), '-std=c++20', '-pthread',
                    '-DASIO_STANDALONE', '-I' + str(out / ('generation-' + label)), '-I' + str(root / 'include'),
                    str(root / 'csp/tests/reusable_path.cpp'), '-o', str(mutant_binary)], check=True)
    with (out / ('mutation-generation-' + label + '.log')).open('w') as log:
        result = subprocess.run([str(mutant_binary)], stdout=log, stderr=subprocess.STDOUT, timeout=60)
        log.write(f'\nexit={result.returncode}\n')
    assert result.returncode != 0, 'generation guard control failed to discriminate: ' + label
assert header.read_bytes() == original
print('Stale cancellation, timer and wakeup generation mutations rejected by the reusable-path control; production header unchanged')
