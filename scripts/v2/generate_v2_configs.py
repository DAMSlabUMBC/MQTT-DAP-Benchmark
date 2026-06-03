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


def build_subscriber_definition():
    return {
        "id": SUBSCRIBER_DEF_ID,
        "type": "subscriber",
        "topic_filter": "device/+",
    }


def build_subscriber_instances(n_purposes):
    insts = []
    for k in range(1, n_purposes + 1):
        insts.append({
            "device_def_id": SUBSCRIBER_DEF_ID,
            "instance_id": f"{SUBSCRIBER_DEF_ID}_p{k}",
            "purpose_filter": f"p{k}",
            "count": 1,
        })
    return insts


def build_purpose_definitions(n_purposes):
    return [{"id": f"p{k}", "description": f"Purpose {k}"}
            for k in range(1, n_purposes + 1)]


def subset_size(count):
    """25% of count, round-half-up, minimum 1."""
    return max(1, math.floor(0.25 * count + 0.5))


def select_subset(ids, label):
    """Deterministically pick subset_size(len(ids)) ids.

    Seeded per (SEED, label) so MP and SP subsets differ but are reproducible.
    Sorts the input first so selection is independent of caller ordering.
    """
    k = subset_size(len(ids))
    rng = random.Random(f"{SEED}:{label}")
    chosen = rng.sample(sorted(ids), k)
    return sorted(chosen)
