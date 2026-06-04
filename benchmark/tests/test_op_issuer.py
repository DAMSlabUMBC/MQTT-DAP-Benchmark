import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import GlobalDefs
from TestExecutor import TestExecutor


def _fake_publisher(instance_id, connected=True):
    p = types.SimpleNamespace()
    p.instance_id = instance_id
    p.is_connected = connected
    return p


def _make_executor():
    # TestExecutor.__init__ logs a seed via the module-global LOGGING_MODULE,
    # which Benchmark.py wires up at runtime. Stub it for unit testing.
    GlobalDefs.LOGGING_MODULE = types.SimpleNamespace(log_seed=lambda *a, **k: None)
    return TestExecutor("obs", "localhost", 1883, GlobalDefs.PurposeManagementMethod.PM_UNIFIED)


def test_select_op_issuer_is_single_and_stable():
    ex = _make_executor()
    pubs = [_fake_publisher(f"dev{i:02d}") for i in range(1, 41)]
    first = ex._select_op_issuer(pubs)
    # same issuer on subsequent ticks
    assert ex._select_op_issuer(pubs) is first
    assert first in pubs


def test_select_op_issuer_deterministic_across_instances():
    pubs1 = [_fake_publisher(f"dev{i:02d}") for i in range(1, 41)]
    pubs2 = [_fake_publisher(f"dev{i:02d}") for i in range(1, 41)]
    a = _make_executor()._select_op_issuer(pubs1)
    b = _make_executor()._select_op_issuer(pubs2)
    assert a.instance_id == b.instance_id


def test_select_op_issuer_reselects_if_pinned_disconnected():
    ex = _make_executor()
    pubs = [_fake_publisher(f"dev{i:02d}") for i in range(1, 41)]
    pinned = ex._select_op_issuer(pubs)
    pinned.is_connected = False
    new = ex._select_op_issuer([p for p in pubs if p.is_connected])
    assert new.is_connected is True
    assert new is not pinned
