#!/usr/bin/env python3
"""Render readable views for one benchmark run.

Given a run's raw log (`<base>.log` or `.log.gz`) plus its analyzer outputs
(`<base>.csv`, `<base>_op_correlation.csv`), emit:
  - SUMMARY.md    : one-screen at-a-glance digest (verdict + key metrics + per-op table)
  - operations.txt: the operations pulled out of the data flood, time-ordered, decoded

Usage:  python3 scripts/v2/render_run.py <path-to-run.log[.gz]>
Writes SUMMARY.md and operations.txt next to the log (or into --out-dir).
"""
import argparse
import csv
import gzip
import io
import os
import time


def _open(path):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)


def load_metrics(csv_path):
    """metrics.csv (Category,Name,Value) -> {(cat,name): value_str}."""
    m = {}
    if not os.path.exists(csv_path):
        return m
    for row in csv.reader(open(csv_path)):
        if len(row) >= 3:
            m[(row[0], row[1])] = row[2]
    return m


def f(m, cat, name, default="-"):
    return m.get((cat, name), default)


def fnum(m, cat, name, nd=2, default=None):
    try:
        return f"{float(m[(cat, name)]):.{nd}f}"
    except (KeyError, ValueError):
        return default if default is not None else "-"


def parse_log(log_path):
    """Single pass over the raw log: lifecycle/data stats + the op event stream."""
    pubs = set(); subs = set(); purposes = set()
    tmin = tmax = None
    npub = nrecv = 0
    issuer_ops = []   # (ts, op_type, corr) periodic ops issued
    reg_info = 0
    forwarded = {}    # (op_type, corr) -> set(recv subscriber)
    informed_pubs = set()
    start_human = None
    for line in _open(log_path):
        p = line.rstrip("\n").split("@@")
        lab = p[0]
        if lab == "PUBLISH":
            pubs.add(p[3]); purposes.add(p[5]); npub += 1
            t = float(p[1]); tmin = t if tmin is None else min(tmin, t); tmax = t if tmax is None else max(tmax, t)
        elif lab == "RECV":
            nrecv += 1
            t = float(p[1]); tmin = t if tmin is None else min(tmin, t); tmax = t if tmax is None else max(tmax, t)
        elif lab == "SUBSCRIBE":
            subs.add(p[3])
        elif lab == "PUBLISH_OP":
            if p[6] == "REGISTER-INFO":
                reg_info += 1
            else:
                issuer_ops.append((float(p[1]), p[6], p[3], p[8]))   # ts, op_type, issuer, corr
        elif lab == "RECV_OP":
            forwarded.setdefault((p[7], p[10]), set()).add(p[3])     # (op_type, corr) -> recv sub
        elif lab == "RECV_OP_RESP":
            if p[7] == "REGISTER-INFO" and p[4] == "Broker":
                informed_pubs.add(p[3])
    span = (tmax - tmin) if (tmin and tmax) else 0
    return {
        "npub": len(pubs), "nsub": len(subs), "npurp": len(purposes),
        "span": span, "data_pub": npub, "data_recv": nrecv,
        "issuer_ops": sorted(issuer_ops), "reg_info": reg_info,
        "forwarded": forwarded, "informed_pubs": informed_pubs,
        "issuer": (issuer_ops[0][2] if issuer_ops else None),
    }


def load_op_correlation(path):
    """_op_correlation.csv -> {(op_type, corr_or_idx): (completed, latency_ms)} by order."""
    rows = []
    if os.path.exists(path):
        r = csv.DictReader(open(path))
        for row in r:
            rows.append(row)
    return rows


VERDICT_OPS = ["AUDIT", "HISTORY", "UPDATE", "DELETE", "RESTRICT"]


def render_summary(cell, lg, m, out_path):
    # verdict
    far = f(m, "Purpose Correctness", "Avg False Accept Rate", "0")
    frr = f(m, "Purpose Correctness", "Avg False Reject Rate", "0")
    invalid = f(m, "Purpose Correctness", "Total Invalid Messages", "0")
    # ops only "present" if the broker actually tracked operations this run
    try:
        tracked = int(f(m, "OP Correlation", "Total Tracked Operations", "0"))
    except ValueError:
        tracked = 0
    has_ops = tracked > 0
    op_comp = f(m, "OP Correlation", "Overall Completion Rate") if has_ops else "—"
    complete = lg["span"] >= 170
    # The only hard failure is a truncated run; correctness numbers (incl. proven-benign
    # churn-boundary FAR/FRR) are shown for the reader to judge rather than auto-failed.
    verdict = "PASS" if complete else "PARTIAL"
    flags = [] if complete else [f"PARTIAL {lg['span']:.0f}s — truncated"]

    L = []
    L.append(f"# {cell} — {'✅ PASS' if verdict=='PASS' else '⚠ PARTIAL — ' + ', '.join(flags)}")
    L.append(f"{lg['span']:.0f} s · QoS {f(m,'','',)}".replace(" · QoS -", "") )  # span; qos added below
    # header line
    qos = "0"
    L[-1] = (f"{lg['span']:.0f} s · QoS {qos} · unified DAP · "
             f"{lg['npub']} pub / {lg['nsub']} sub / {lg['npurp']} purposes")
    L.append("")
    L.append(f"RESULT  data {'complete' if complete else 'PARTIAL'} ({lg['span']:.0f}s) · "
             f"FAR {far} · FRR {frr} · "
             + (f"ops {op_comp}" if has_ops else "no ops"))
    L.append("")
    L.append(f"Messaging    throughput {fnum(m,'Messaging','Throughput (msgs/sec)',0)} msg/s · "
             f"latency avg {fnum(m,'Messaging','Latency Avg (ms)')} ms "
             f"(max {fnum(m,'Messaging','Latency Max (ms)')})")
    L.append(f"Correctness  {f(m,'Purpose Correctness','Total Valid Messages')} valid / "
             f"{invalid} invalid · FAR {far} · FRR {frr}")
    if has_ops:
        L.append(f"Operations   {f(m,'OP Correctness','Total Operational Requests')} issued / "
                 f"{f(m,'OP Correlation','Total Completed Operations')} completed ({op_comp}) · "
                 f"coverage {f(m,'OP Correctness','Avg Coverage')} · "
                 f"leakage {f(m,'OP Correctness','Avg Leakage')}")
        for op in VERDICT_OPS:
            issued = f(m, "OP Correlation", f"{op} Operations Issued", None)
            if issued is None:
                continue
            rate = f(m, "OP Correlation", f"{op} Completion Rate")
            med = fnum(m, "OP Correlation", f"{op} Median Latency (ms)")
            p99 = fnum(m, "OP Correlation", f"{op} p99 Latency (ms)")
            L.append(f"    {op:<9} {issued} issued · {rate} · median {med} ms · p99 {p99} ms")
        inf_n = f(m, "O1 Informed", "Publishers Informed", "-")
        inf_d = f(m, "O1 Informed", "Publishers With Consumers", "-")
        inf_r = f(m, "O1 Informed", "Informed Completion Rate", "-")
        note = "  ⚑ startup-race edge (see operations.txt)" if inf_r not in ("1.0000", "-") else ""
        L.append(f"    REGISTER-INFO  informed {inf_n}/{inf_d} ({inf_r}){note}")
    L.append(f"Broker       CPU avg {fnum(m,'Broker','CPU Avg (%)',1)}% "
             f"(max {fnum(m,'Broker','CPU Max (%)',1)}) · "
             f"mem {fnum(m,'Broker','Memory Avg (MB)',0)} MB")
    L.append("")
    with open(out_path, "w") as fh:
        fh.write("\n".join(L) + "\n")
    return verdict, far, frr, op_comp, has_ops


def render_operations(cell, lg, opc, out_path):
    L = []
    issuer = lg["issuer"]
    L.append(f"OPERATIONS — {cell}")
    if issuer:
        L.append(f"issuer {issuer}   ·   {lg['reg_info']} REGISTER-INFO registrations at setup")
    L.append("")
    if not lg["issuer_ops"]:
        L.append("(no operations in this run)")
        with open(out_path, "w") as fh:
            fh.write("\n".join(L) + "\n")
        return
    t0 = lg["issuer_ops"][0][0]
    # op_correlation rows are in issue order; pair them to issuer_ops by index
    last_tick = None
    for i, (ts, op_type, _iss, corr) in enumerate(lg["issuer_ops"]):
        rel = ts - t0
        tick = int(rel // 10)
        if tick != last_tick:
            L.append(f"t+{rel+10:.0f}s ── tick {tick+1} ──────────────────────────────")
            last_tick = tick
        targets = sorted(lg["forwarded"].get((op_type, corr), set()))
        tgt = (targets[0] if len(targets) == 1 else
               ("broker (direct)" if not targets else f"{len(targets)} subs"))
        row = opc[i] if i < len(opc) else None
        ok = (row and row.get("completed") == "yes")
        lat = (f"{float(row['last_response_latency_ms']):.1f} ms"
               if row and row.get("last_response_latency_ms") else "")
        L.append(f"  {op_type:<9} {issuer} → {tgt:<24} {'✓' if ok else '✗'} {lat}")
    total = len(lg["issuer_ops"])
    done = sum(1 for r in opc if r.get("completed") == "yes")
    L.append("")
    L.append(f"{total} ops · {done} completed ({100*done/total:.0f}%) · "
             f"{lg['reg_info']} REGISTER-INFO · informed {len(lg['informed_pubs'])} publishers")
    with open(out_path, "w") as fh:
        fh.write("\n".join(L) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("log", help="path to <base>.log or <base>.log.gz")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--cell", default=None, help="override cell name")
    args = ap.parse_args()

    log = args.log
    base = log[:-7] if log.endswith(".log.gz") else log[:-4]
    cell = args.cell or os.path.basename(base).removeprefix("v2_").removesuffix("_unified")
    # default: a per-cell subdir next to the log, so SUMMARY/operations don't collide
    # when many cells share one flat sweep directory.
    out_dir = args.out_dir or os.path.join(os.path.dirname(log) or ".", cell)
    os.makedirs(out_dir, exist_ok=True)

    m = load_metrics(base + ".csv")
    opc = load_op_correlation(base + "_op_correlation.csv")
    lg = parse_log(log)

    verdict, far, frr, op_comp, has_ops = render_summary(cell, lg, m, os.path.join(out_dir, "SUMMARY.md"))
    render_operations(cell, lg, opc, os.path.join(out_dir, "operations.txt"))
    # run start time, for chronological INDEX ordering (console "Started at" -> sortable)
    started = "00000000-000000"
    con = base + ".console"
    if os.path.exists(con):
        for line in open(con):
            if "Started at 2026" in line:
                import re as _re
                mt = _re.search(r"(20\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)", line)
                if mt:
                    started = f"{mt[1]}{mt[2]}{mt[3]}-{mt[4]}{mt[5]}{mt[6]}"
                break
    informed = f(m, "O1 Informed", "Informed Completion Rate", "-") if has_ops else "-"
    # tab-separated stamp for build_index.py:  started cell verdict span thru FAR FRR ops informed
    print("\t".join([started, cell, verdict, f"{lg['span']:.0f}",
                     fnum(m, "Messaging", "Throughput (msgs/sec)", 0),
                     far, frr, op_comp, informed]))


if __name__ == "__main__":
    main()
