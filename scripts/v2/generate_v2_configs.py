"""Generator for the v2 correctness experiment matrix (paper section 7.3).

Emits 20 config files consumed by benchmark/ConfigParser.py. See
docs/superpowers/specs/2026-06-03-v2-experiment-generators-design.md.
"""
import argparse
import math
import os
import random

import yaml

from scripts.v2 import psmark_f_profile as prof

SEED = 1074
N_PUBLISHERS = 40
DURATION_MS = 180100
FIXED_PAYLOAD = prof.FIXED_PAYLOAD_BYTES

OP_SEND_RATE_MS = 10000
START_PUBLISH_MS = 100
TICK_START_MS = 10100
TICK_INTERVAL_MS = 10000
DISCONNECT_MS = 60100
RECONNECT_MS = 120100

SUBSCRIBER_DEF_ID = "device_subscriber"


def _pub_id(i):  # i is 1-indexed
    return f"dev{i:02d}"


def build_publisher_definitions():
    """40 publisher definitions, one unique topic each, PSMark-F rates."""
    rows = prof.expand_publisher_rows()
    defs = []
    for idx, row in enumerate(rows, start=1):
        defs.append({
            "id": _pub_id(idx),
            "type": "publisher",
            "topic": f"device/{_pub_id(idx)}",
            "pub_period_ms": row["pub_period_ms"],
            "min_payload_bytes": FIXED_PAYLOAD,
            "max_payload_bytes": FIXED_PAYLOAD,
        })
    return defs


def build_publisher_instances(n_purposes):
    """One instance per publisher; purpose round-robins over min(N,40)."""
    span = min(n_purposes, N_PUBLISHERS)
    insts = []
    for idx in range(1, N_PUBLISHERS + 1):
        purpose = f"p{((idx - 1) % span) + 1}"
        insts.append({
            "device_def_id": _pub_id(idx),
            "instance_id": _pub_id(idx),
            "purpose_filter": purpose,
            "count": 1,
        })
    return insts
