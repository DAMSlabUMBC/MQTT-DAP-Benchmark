#!/usr/bin/env python3
"""Proof that the operation ISSUER MODE is selectable and own-data scoping holds
in BOTH modes.

Drives the REAL TestExecutor op-issue path
(_send_operational_requests_if_ready -> _select_op_issuer -> _send_operational_request)
with a stubbed MQTT client module (no broker), so we observe exactly which
publishers issue and with what OpTFs/OpPFs scope.

  all_publishers (default) -> every connected publisher issues, each scoped to
                              its OWN topic/purpose.
  single_random            -> exactly ONE pinned publisher issues across
                              intervals, still scoped to its OWN topic/purpose.

Run: python3 test_op_issuer_mode.py    (no broker needed)
"""
import os
import sys
import types
import random
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import GlobalDefs
from TestExecutor import TestExecutor


class FakeDevDef:
    def __init__(self, topic):
        self.topic = topic


class FakePub:
    def __init__(self, iid, topic, purpose):
        self.instance_id = iid
        self.is_connected = True
        self.message_count = 0
        self.message_id_to_send_counter = {}
        self.mqtt_client = object()
        self.mqtt_client_name = iid
        self.device_definition = FakeDevDef(topic)
        self.current_purpose_filter = purpose


class FakeDM:
    def __init__(self, pubs):
        self._pubs = pubs

    def get_all_publishers(self):
        return self._pubs


class RecordingClientModule:
    """Stands in for GlobalDefs.CLIENT_MODULE; records the scope of each request."""
    def __init__(self):
        self.calls = []

    def publish_operation_request(self, client, method, operation, corr,
                                  qos=0, op_tfs="*", op_pfs="*"):
        self.calls.append({"operation": operation, "op_tfs": op_tfs, "op_pfs": op_pfs})
        return []  # empty results -> skip the pending-publish bookkeeping loop


PUBS = [
    FakePub("pub1", "device/d1", "p1"),
    FakePub("pub2", "device/d2", "p2"),
    FakePub("pub3", "device/d3", "p3"),
]
PUB_TP = {p.device_definition.topic: p.current_purpose_filter for p in PUBS}


def make_self(mode):
    s = types.SimpleNamespace()
    s.next_op_time_ms = 0
    s.all_operations = {"HISTORY": "C2"}
    s.device_manager = FakeDM(PUBS)
    s.current_config = types.SimpleNamespace(op_send_rate=1000, op_issuer_mode=mode, qos=0)
    s.method = GlobalDefs.PurposeManagementMethod.PM_UNIFIED
    s.publish_lock = threading.Lock()
    s.pending_publishes = {}
    s._op_issuer_id = None
    s._op_issuer_rng = random.Random(1074)
    s._select_op_issuer = types.MethodType(TestExecutor._select_op_issuer, s)
    s._send_operational_request = types.MethodType(TestExecutor._send_operational_request, s)
    s._send_operational_requests_if_ready = types.MethodType(TestExecutor._send_operational_requests_if_ready, s)
    return s


def scope_ok(calls):
    # Every issued op pairs the issuer's OWN topic with its OWN purpose.
    return all(PUB_TP.get(c["op_tfs"]) == c["op_pfs"] for c in calls) and len(calls) > 0


def main():
    # --- all_publishers: every publisher issues, each scoped to its own data ---
    cm = RecordingClientModule()
    GlobalDefs.CLIENT_MODULE = cm
    s = make_self("all_publishers")
    s._send_operational_requests_if_ready(1000)
    all_issuers = sorted({c["op_tfs"] for c in cm.calls})
    all_scope = scope_ok(cm.calls)

    # --- single_random: one pinned publisher issues across two intervals ---
    cm2 = RecordingClientModule()
    GlobalDefs.CLIENT_MODULE = cm2
    s2 = make_self("single_random")
    s2._send_operational_requests_if_ready(1000)
    s2._send_operational_requests_if_ready(2000)
    single_issuers = sorted({c["op_tfs"] for c in cm2.calls})
    single_scope = scope_ok(cm2.calls)

    print("==== OP ISSUER MODE RESULTS ====")
    print(f"  all_publishers: issuers(by own topic)={all_issuers}  scoped-to-own={all_scope}")
    print(f"  single_random : issuers(by own topic)={single_issuers}  calls={len(cm2.calls)}  scoped-to-own={single_scope}")

    all_ok = len(all_issuers) == 3 and all_scope
    single_ok = len(single_issuers) == 1 and len(cm2.calls) == 2 and single_scope

    print(f"\n  all_publishers issued from ALL 3 publishers, own-data scoped: {all_ok}")
    print(f"  single_random issued from exactly ONE pinned publisher, own-data scoped: {single_ok}")

    if all_ok and single_ok:
        print("\nOP ISSUER MODE TEST PASSED")
        return 0
    print("\nOP ISSUER MODE TEST FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
