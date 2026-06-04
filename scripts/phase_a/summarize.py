#!/usr/bin/env python3
"""Aggregate /tmp/dap_eval/<system>/<variant>/rep<N>/ across 5 reps per experiment."""
import csv
import gzip
import statistics
import sys
from pathlib import Path

ROOT = Path("/tmp/dap_eval")

EXPERIMENTS = [
    ("baseline", "base"),
    ("baseline", "2xdev"),
    ("baseline", "2srv"),
    ("baseline", "2srv_2x"),
    ("topic", "base_purpose"),
    ("topic", "double_purpose"),
    ("topic", "triple_purpose"),
    ("dap", "base"),
    ("dap", "2xdev"),
    ("dap", "2srv"),
    ("dap", "2srv_2x"),
    ("dap", "double_purpose"),
]


def read_psmark_per_rep(rep_dir: Path):
    """Aggregate throughput, latency, drops across all per-runner result dirs in a rep."""
    sum_thpt = 0.0
    recv_total = 0
    sent_total = 0
    drop_total = 0
    p50_vals = []
    p99_vals = []
    for runner_dir in sorted(rep_dir.iterdir()):
        if not runner_dir.is_dir():
            continue
        result = runner_dir / "result"
        if not result.is_dir():
            continue
        with (result / "throughput.csv").open() as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                if row["Sender"] == "overall":
                    sum_thpt += float(row["AverageThroughput"])
                    recv_total += int(row["TotalMessagesRecv"])
        with (result / "latency.csv").open() as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                if row["Sender"] == "overall":
                    p50_vals.append(float(row["MedianMs"]))
                    p99_vals.append(float(row["P99Ms"]))
        with (result / "dropped_messages.csv").open() as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                if row["Sender"] == "overall":
                    sent_total += int(row["TotalMessagesSentFromSender"])
                    drop_total += int(row["PubsDroppedFromSender"])
    drop_pct = (100.0 * drop_total / sent_total) if sent_total else 0.0
    return {
        "thpt_agg": sum_thpt,
        "p50_ms": statistics.mean(p50_vals) if p50_vals else 0.0,
        "p99_ms": statistics.mean(p99_vals) if p99_vals else 0.0,
        "drop_pct": drop_pct,
        "recv": recv_total,
    }


def read_dap_correctness(rep_dir: Path):
    """Pull FAR/FRR and obs_negative leakage from the merged.csv analyzer output."""
    merged_csv = rep_dir / "merged.csv"
    if not merged_csv.exists():
        return None
    far, frr, total_inv = None, None, None
    with merged_csv.open() as f:
        for row in csv.reader(f):
            if len(row) < 3:
                continue
            if row[0] == "Purpose Correctness" and row[1] == "Avg False Accept Rate":
                far = float(row[2])
            elif row[0] == "Purpose Correctness" and row[1] == "Avg False Reject Rate":
                frr = float(row[2])
            elif row[0] == "Purpose Correctness" and row[1] == "Total Invalid Messages":
                total_inv = int(row[2])
    obs_neg = 0
    obs_log_gz = rep_dir / "observer.log.gz"
    if obs_log_gz.exists():
        with gzip.open(obs_log_gz, "rt") as f:
            for line in f:
                parts = line.rstrip().split("@@")
                if parts and parts[0] == "RECV" and len(parts) > 3 and parts[3] == "obs_negative":
                    obs_neg += 1
    return {"far": far, "frr": frr, "invalid": total_inv, "obs_neg": obs_neg}


def mean_stdev(xs):
    if not xs:
        return (0.0, 0.0)
    if len(xs) == 1:
        return (xs[0], 0.0)
    return (statistics.mean(xs), statistics.stdev(xs))


def main():
    print(f"{'system':<9} {'variant':<14} {'thpt mean':>12} {'thpt sd':>9} "
          f"{'p50 ms':>8} {'p99 ms':>8} {'drop%':>7} {'FAR':>8} {'FRR':>8} {'obs_neg':>8}")
    print("-" * 110)
    for system, variant in EXPERIMENTS:
        exp_dir = ROOT / system / variant
        if not exp_dir.is_dir():
            print(f"{system:<9} {variant:<14}  (missing dir)")
            continue
        thpts, p50s, p99s, drops, fars, frrs, negs = [], [], [], [], [], [], []
        for rep_dir in sorted(exp_dir.glob("rep*")):
            ps = read_psmark_per_rep(rep_dir)
            thpts.append(ps["thpt_agg"])
            p50s.append(ps["p50_ms"])
            p99s.append(ps["p99_ms"])
            drops.append(ps["drop_pct"])
            if system == "dap":
                dc = read_dap_correctness(rep_dir)
                if dc:
                    fars.append(dc["far"] or 0.0)
                    frrs.append(dc["frr"] or 0.0)
                    negs.append(dc["obs_neg"])
        m_thpt, sd_thpt = mean_stdev(thpts)
        m_p50, _ = mean_stdev(p50s)
        m_p99, _ = mean_stdev(p99s)
        m_drop, _ = mean_stdev(drops)
        if system == "dap" and fars:
            m_far, _ = mean_stdev(fars)
            m_frr, _ = mean_stdev(frrs)
            m_neg, _ = mean_stdev([float(n) for n in negs])
            print(f"{system:<9} {variant:<14} {m_thpt:>12.1f} {sd_thpt:>9.1f} "
                  f"{m_p50:>8.1f} {m_p99:>8.1f} {m_drop:>7.3f} "
                  f"{m_far:>8.4f} {m_frr:>8.4f} {m_neg:>8.1f}")
        else:
            print(f"{system:<9} {variant:<14} {m_thpt:>12.1f} {sd_thpt:>9.1f} "
                  f"{m_p50:>8.1f} {m_p99:>8.1f} {m_drop:>7.3f} "
                  f"{'-':>8} {'-':>8} {'-':>8}")


if __name__ == "__main__":
    main()
