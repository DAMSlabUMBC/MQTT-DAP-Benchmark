#!/usr/bin/env python3
"""End-to-end proof that operations are scoped to the issuing publisher's own data.

Drives the real ClientInterface against a running DAP broker:

  pubA registers MP purpose_A for topic_A, publishes data on topic_A
  pubB registers MP purpose_B for topic_B, publishes data on topic_B
  subA subscribes topic_A with SP purpose_A  -> receives A's data
  subB subscribes topic_B with SP purpose_B  -> receives B's data
  each subscriber also subscribes to its op inbox OP_REQ/<id>

Then pubA issues a HISTORY operation scoped to its OWN topic/purpose
(op_tfs=topic_A, op_pfs=purpose_A). The broker forwards a scoped operation only
to subscribers that received the matching data from the issuer.

Assertions:
  (1) cross-publisher scope: A's op reaches subA (A's subscriber) and NOT subB.
  (2) the broker actually consults OpTFs: the same op issued with a NON-matching
      topic filter reaches NOBODY (proves the filter value is honored, not ignored).

Run: python3 test_op_scope_routing.py [host] [port]   (broker must be up, DAP unified)
"""
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import GlobalDefs
import ClientInterface as CI

# Align the MP-registration topic with the broker's hardcoded value ($MP_REG).
# (OSYS_TOPIC, OP_RESPONSE_TOPIC, OP_PURPOSE and all DAP-* property keys already
# match the broker defaults.)
GlobalDefs.REG_BY_MSG_REG_TOPIC = "$MP_REG"

HOST = "127.0.0.1"
PORT = 18831
METHOD = GlobalDefs.PurposeManagementMethod.PM_UNIFIED

TOPIC_A, PURPOSE_A = "device/devA", "quality/assurance"
TOPIC_B, PURPOSE_B = "device/devB", "vendor/maintenance"

data_rx = {"subA": [], "subB": []}
op_rx = {"subA": [], "subB": []}


def op_type_of(msg):
    if not (msg.properties and hasattr(msg.properties, "UserProperty")):
        return None
    for k, v in msg.properties.UserProperty:
        if k == GlobalDefs.PROPERTY_OPTYPE:
            return v
    return None


def make_on_message(name):
    def on_message(client, userdata, msg):
        if msg.topic.startswith(GlobalDefs.ORS_TOPIC + "/") or msg.topic.startswith("OP_REQ/"):
            op_rx[name].append((msg.topic, op_type_of(msg)))
        else:
            data_rx[name].append(msg.topic)
    return on_message


def mk(cid, on_msg=None):
    c = CI.create_v5_client(cid)
    if on_msg:
        c.on_message = on_msg
    CI.connect_client(c, HOST, PORT)
    c.loop_start()
    return c


def main():
    global HOST, PORT
    if len(sys.argv) > 1:
        HOST = sys.argv[1]
    if len(sys.argv) > 2:
        PORT = int(sys.argv[2])

    subA = mk("subA", make_on_message("subA"))
    subB = mk("subB", make_on_message("subB"))
    pubA = mk("pubA")
    pubB = mk("pubB")
    time.sleep(0.4)

    # subscribers: data subscription (with SP) + their operation inbox OP_REQ/<id>
    CI.subscribe_with_purpose_filter(subA, METHOD, TOPIC_A, PURPOSE_A)
    CI.subscribe_for_operations(subA, METHOD, "OP_REQ/subA")
    CI.subscribe_with_purpose_filter(subB, METHOD, TOPIC_B, PURPOSE_B)
    CI.subscribe_for_operations(subB, METHOD, "OP_REQ/subB")
    time.sleep(0.5)

    # publishers register their MP, then publish their data
    CI.register_publish_purpose_for_topic(pubA, METHOD, TOPIC_A, PURPOSE_A)
    CI.register_publish_purpose_for_topic(pubB, METHOD, TOPIC_B, PURPOSE_B)
    time.sleep(0.4)
    CI.publish_with_purpose(pubA, METHOD, TOPIC_A, PURPOSE_A, qos=0, payload=b"A-data")
    CI.publish_with_purpose(pubB, METHOD, TOPIC_B, PURPOSE_B, qos=0, payload=b"B-data")
    time.sleep(0.6)

    print(f"data: subA={data_rx['subA']}  subB={data_rx['subB']}")

    # (1) pubA issues HISTORY scoped to its OWN topic/purpose
    CI.publish_operation_request(pubA, METHOD, "HISTORY", 1, qos=0,
                                 op_tfs=TOPIC_A, op_pfs=PURPOSE_A)
    time.sleep(0.7)
    a_after_scoped = list(op_rx["subA"])
    b_after_scoped = list(op_rx["subB"])

    # (2) discriminator: pubA issues HISTORY with a NON-matching topic filter
    CI.publish_operation_request(pubA, METHOD, "HISTORY", 2, qos=0,
                                 op_tfs="device/none", op_pfs=PURPOSE_A)
    time.sleep(0.7)
    a_after_nonmatch = list(op_rx["subA"])

    for c in (subA, subB, pubA, pubB):
        c.loop_stop(); CI.disconnect_client(c)

    print(f"(1) scoped op_tfs={TOPIC_A}: subA op inbox={a_after_scoped}  subB op inbox={b_after_scoped}")
    print(f"(2) op_tfs=device/none (no match): subA op inbox={a_after_nonmatch}")

    ok_data = data_rx["subA"] == [TOPIC_A] and data_rx["subB"] == [TOPIC_B]
    ok_reaches_A = any(t == "HISTORY" for _, t in a_after_scoped)
    ok_not_B = len(b_after_scoped) == 0
    ok_nonmatch_excluded = len(a_after_nonmatch) == len(a_after_scoped)  # no new op delivered

    print(f"\n  data delivered correctly (A->subA, B->subB): {ok_data}")
    print(f"  (1) scoped op reached A's subscriber subA:      {ok_reaches_A}")
    print(f"  (1) scoped op did NOT reach B's subscriber subB:{ok_not_B}")
    print(f"  (2) non-matching OpTFs reached nobody new:      {ok_nonmatch_excluded}")

    if ok_data and ok_reaches_A and ok_not_B and ok_nonmatch_excluded:
        print("\nOP SCOPE TEST PASSED: A's operation hit only A's flow; broker honored OpTFs.")
        return 0
    print("\nOP SCOPE TEST FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
