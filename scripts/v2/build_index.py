#!/usr/bin/env python3
"""Build a chronological INDEX.md dashboard from render_run.py stamp lines.

Each stamp (one per run) is tab-separated:
  started  cell  verdict  span  thru  FAR  FRR  ops  informed

Usage:  python3 scripts/v2/build_index.py <stamps.tsv> [-o INDEX.md]
"""
import argparse
import time


def human(stamp):
    # 20260603-195202 -> 2026-06-03 19:52:02
    if len(stamp) == 15 and "-" in stamp:
        d, t = stamp.split("-")
        return f"{d[:4]}-{d[4:6]}-{d[6:8]} {t[:2]}:{t[2:4]}:{t[4:6]}"
    return stamp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("stamps")
    ap.add_argument("-o", "--out", default="INDEX.md")
    args = ap.parse_args()

    rows = []
    for line in open(args.stamps):
        parts = line.rstrip("\n").split("\t")
        if len(parts) >= 9:
            rows.append(parts)
    rows.sort(key=lambda r: r[0])  # by start time

    out = []
    out.append("# Run index — chronological\n")
    npass = sum(1 for r in rows if r[2] == "PASS")
    out.append(f"{len(rows)} runs · {npass} PASS · "
               f"generated {time.strftime('%Y-%m-%d %H:%M')}\n")
    out.append("| time | run | result | span | thru/s | FAR | FRR | ops | informed |")
    out.append("|------|-----|--------|------|--------|-----|-----|-----|----------|")
    for started, cell, verdict, span, thru, far, frr, ops, informed in (r[:9] for r in rows):
        mark = "✅" if verdict == "PASS" else "⚠"
        ops_s = "—" if ops in ("-", "") else ops
        inf_s = "—" if informed in ("-", "") else informed
        out.append(f"| {human(started)[11:]} | {cell} | {mark} {verdict} | {span}s | "
                   f"{thru} | {far} | {frr} | {ops_s} | {inf_s} |")
    with open(args.out, "w") as fh:
        fh.write("\n".join(out) + "\n")
    print(f"wrote {args.out} ({len(rows)} runs)")


if __name__ == "__main__":
    main()
