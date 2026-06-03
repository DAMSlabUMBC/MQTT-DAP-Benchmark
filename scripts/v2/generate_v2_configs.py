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


def tick_times():
    times = []
    t = TICK_START_MS
    # include a tick while a full interval still fits before the run ends
    while t + TICK_INTERVAL_MS <= DURATION_MS:
        times.append(t)
        t += TICK_INTERVAL_MS
    return times


def change_purpose_events(subset, n_purposes):
    events = []
    for tick_idx, t in enumerate(tick_times()):
        purpose = f"p{(tick_idx % n_purposes) + 1}"
        events.append({
            "time_ms": t,
            "type": "change_purpose",
            "devices": list(subset),
            "new_purpose": purpose,
            "description": f"Dynamic change: {len(subset)} devices -> {purpose}",
        })
    return events


def lifecycle_events():
    return [
        {"time_ms": 0, "type": "connect_all", "description": "Connect all devices"},
        {"time_ms": START_PUBLISH_MS, "type": "start_publishing_all",
         "description": "Start all publishers"},
        {"time_ms": DURATION_MS, "type": "disconnect_all",
         "description": "Disconnect all devices"},
    ]


def connectivity_events(subscriber_ids):
    subset = select_subset(subscriber_ids, label="disconnect")
    return [
        {"time_ms": DISCONNECT_MS, "type": "disconnect", "devices": subset,
         "description": f"Disconnect {len(subset)} subscribers (1/3 of run)"},
        {"time_ms": RECONNECT_MS, "type": "reconnect", "devices": subset,
         "description": f"Reconnect {len(subset)} subscribers (2/3 of run)"},
    ]


def _header(name):
    return {
        "node_name": "TestNode",
        "client_module_name": "ClientInterface",
        "output_dir": "logs",
        "reg_by_msg_reg_topic": "$DAP/purpose_management",
        "reg_by_topic_pub_reg_topic": "$DAP/MP_reg",
        "reg_by_topic_sub_reg_topic": "$DAP/SP_reg",
        "or_topic_name": "OR",
        "ors_topic_name": "ORS",
        "on_topic_name": "ON",
        "onp_topic_name": "ONP",
        "osys_topic_name": "$OSYS",
        "operational_response_topic_prefix": "op_resp",
        "operational_purpose": "DAP_OP",
        "purpose_management_method": 3,
        "monitor_broker": True,
        "node_exporter_url": "http://localhost:9100/metrics",
        "monitor_interval_ms": 1000,
    }


def _ops_block(with_ops):
    if not with_ops:
        return {"op_send_rate": 0, "c1_reg_ops": [], "c1_ops": [],
                "c2_ops": [], "c3_ops": []}
    return {
        "op_send_rate": OP_SEND_RATE_MS,
        "c1_reg_ops": list(prof.C1_REG_OPS),
        "c1_ops": list(prof.C1_OPS),
        "c2_ops": list(prof.C2_OPS),
        "c3_ops": list(prof.C3_OPS),
    }


def assemble_config(set_id, variant, n_purposes, dynamic_side, with_ops, connectivity):
    name = f"v2_set{set_id}_{variant}_{n_purposes}p_unified"

    pub_defs = build_publisher_definitions()
    sub_def = build_subscriber_definition()
    pub_insts = build_publisher_instances(n_purposes)
    sub_insts = build_subscriber_instances(n_purposes)

    events = []
    lc = lifecycle_events()
    events.extend(lc[:2])  # connect_all, start_publishing_all

    if dynamic_side in ("mp", "both"):
        pub_subset = select_subset([d["instance_id"] for d in pub_insts], label="mp")
        events.extend(change_purpose_events(pub_subset, n_purposes))
    if dynamic_side in ("sp", "both"):
        sub_subset = select_subset([s["instance_id"] for s in sub_insts], label="sp")
        events.extend(change_purpose_events(sub_subset, n_purposes))
    if connectivity:
        events.extend(connectivity_events([s["instance_id"] for s in sub_insts]))

    events.append(lc[2])  # disconnect_all
    events.sort(key=lambda e: e["time_ms"])

    cfg = _header(name)
    cfg["device_definitions"] = pub_defs + [sub_def]
    cfg["purpose_definitions"] = build_purpose_definitions(n_purposes)
    cfg["test"] = {
        "name": name,
        "duration_ms": DURATION_MS,
        "data_qos": 0,
        "device_instances": pub_insts + sub_insts,
        "scheduled_events": events,
        **_ops_block(with_ops),
    }
    return cfg


def build_matrix():
    """Return list of (set_subdir, filename, config_dict) for all 20 configs."""
    matrix = []

    def add(set_id, subdir, variant, n, side, ops, conn):
        cfg = assemble_config(set_id, variant, n, side, ops, conn)
        matrix.append((subdir, cfg["test"]["name"] + ".cfg", cfg))

    # (i) static, no ops: 1/10/100
    for n in (1, 10, 100):
        add(1, "set1_static", "static", n, None, False, False)

    # (ii) dynamic, no ops: {10,100} x {mp,sp,both}
    for n in (10, 100):
        for side in ("mp", "sp", "both"):
            add(2, "set2_dynamic", f"dynamic_{side}", n, side, False, False)

    # (iii) static, with ops: 1/10/100
    for n in (1, 10, 100):
        add(3, "set3_static_ops", "static_ops", n, None, True, False)

    # (iv) dynamic, with ops: {10,100} x {mp,sp,both}
    for n in (10, 100):
        for side in ("mp", "sp", "both"):
            add(4, "set4_dynamic_ops", f"dynamic_{side}", n, side, True, False)

    # (v) dynamic connectivity (as iii + disconnect): 10/100 only
    for n in (10, 100):
        add(5, "set5_connectivity", "connectivity", n, None, True, True)

    return matrix


def write_matrix(out_root):
    written = []
    for subdir, filename, cfg in build_matrix():
        d = os.path.join(out_root, subdir)
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, filename)
        with open(path, "w") as f:
            yaml.safe_dump(cfg, f, sort_keys=False, default_flow_style=False)
        written.append(path)
    return written


def main():
    ap = argparse.ArgumentParser(description="Generate the v2 experiment matrix")
    ap.add_argument("--out-dir", default="test-configs/v2")
    ap.add_argument("--dry-run", action="store_true",
                    help="print per-config counts without writing")
    args = ap.parse_args()

    if args.dry_run:
        for subdir, filename, cfg in build_matrix():
            t = cfg["test"]
            n_pubs = len([d for d in cfg["device_definitions"] if d["type"] == "publisher"])
            n_subs = len([i for i in t["device_instances"]
                          if i["device_def_id"] == SUBSCRIBER_DEF_ID])
            print(f"{subdir}/{filename}: "
                  f"{n_pubs} pubs, {n_subs} subs, "
                  f"{len(cfg['purpose_definitions'])} purposes, "
                  f"{len(t['scheduled_events'])} events, "
                  f"ops={t['op_send_rate']}")
        return

    paths = write_matrix(args.out_dir)
    print(f"Wrote {len(paths)} configs to {args.out_dir}")


if __name__ == "__main__":
    main()
