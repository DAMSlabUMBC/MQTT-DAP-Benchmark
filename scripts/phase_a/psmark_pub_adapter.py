#!/usr/bin/env python3
"""
Translate PSMark per-runner pub_events/recv_events CSVs into synthetic
PUBLISH@@... log lines and append them to the Python observer log so that
MetricsCalculator.calculate_purpose_correctness can join recv events from
the observer subscribers with the originating PSMark publishes.

Inputs:
  --psmark-root  Directory containing one <runner>/results/run_<ts>_*/ tree
                 per PSMark VM. Reads raw_events/<runner>_pub_events.csv and
                 raw_events/<runner>_recv_events.csv from each.
  --observer-log Existing Python observer log file. New lines are appended.

The device-type -> purpose map mirrors the dap_4n_observed scenario.
"""

import argparse
import csv
import os
import re
import sys
from pathlib import Path

SEPARATOR = "@@"
BENCH_ID = "PSMARK"
MSG_TYPE = "DATA"

DEVICE_PURPOSE = {
    "machine_temperature_sensor": "maintenance.predictive",
    "machine_speed_sensor":       "quality.assurance",
    "machine_energy_consumption": "finance.reporting",
    "production_quality_sensor":  "quality.assurance",
    "vibration_sensor":           "maintenance.predictive",
    "robot_farmap":               "partner.logistics.routing",
    "robot_nearmap":              "partner.logistics.routing",
    "robot_imu":                  "maintenance.routine",
    "robot_lidar":                "vendor.maintenance",
    "robot_odometry":             "supply.forecast",
}

BIN_RE = re.compile(r'^<<"(.+)">>$')


def unquote(cell: str) -> str:
    m = BIN_RE.match(cell.strip())
    return m.group(1) if m else cell.strip()


def device_type_from_topic(topic: str) -> str:
    parts = topic.split("/")
    return parts[-1] if len(parts) == 4 else ""


def collect_pub_timestamps(recv_csv: Path) -> dict:
    """Map (sender_client, seq) -> earliest PubTimeNs observed on any receiver."""
    pubts = {}
    with recv_csv.open() as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            sender = unquote(row["SenderID"])
            client = unquote(row.get("ReceivingClient", ""))
            # SenderID is the per-VM aggregate; the per-publisher identity is
            # encoded in the publisher_id binary embedded in the payload, but
            # we don't have it here. Use SenderID for now and refine below.
            try:
                seq = int(row["SeqId"])
                ts_ns = int(row["PubTimeNs"])
            except (ValueError, KeyError):
                continue
            # SeqId is global per (publisher_client, topic). Use (SenderID, topic, seq)
            # as the key and join with pub_events on the same triple.
            topic = unquote(row["Topic"])
            key = (sender, topic, seq)
            prev = pubts.get(key)
            if prev is None or ts_ns < prev:
                pubts[key] = ts_ns
    return pubts


def synth_publish_lines(pub_csv: Path, pubts: dict) -> list:
    lines = []
    missing = 0
    with pub_csv.open() as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            sender = unquote(row["SenderID"])
            sending_client = unquote(row["SendingClient"])
            topic = unquote(row["Topic"])
            try:
                seq = int(row["SeqId"])
            except (ValueError, KeyError):
                continue
            dt = device_type_from_topic(topic)
            purpose = DEVICE_PURPOSE.get(dt, "*")
            ts_ns = pubts.get((sender, topic, seq))
            if ts_ns is None:
                missing += 1
                continue
            ts_s = ts_ns / 1e9
            line = SEPARATOR.join([
                "PUBLISH",
                f"{ts_s:.9f}",
                BENCH_ID,
                sending_client,
                topic,
                purpose,
                MSG_TYPE,
                str(seq),
            ])
            lines.append(line)
    return lines, missing


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--psmark-root", required=True)
    ap.add_argument("--observer-log", required=True)
    ap.add_argument("--out-log", required=True,
                    help="Merged log written here; observer-log is left untouched.")
    args = ap.parse_args()

    psmark_root = Path(args.psmark_root)
    if not psmark_root.is_dir():
        sys.exit(f"psmark-root not a directory: {psmark_root}")

    all_lines = []
    total_missing = 0
    for runner_dir in sorted(psmark_root.iterdir()):
        if not runner_dir.is_dir():
            continue
        result_dirs = sorted(runner_dir.glob("results/run_*"))
        if not result_dirs:
            result_dirs = sorted(runner_dir.glob("run_*"))
        if not result_dirs:
            continue
        result = result_dirs[-1]
        raw = result / "raw_events"
        runner = runner_dir.name
        pub_csv = raw / f"{runner}_pub_events.csv"
        recv_csv = raw / f"{runner}_recv_events.csv"
        if not pub_csv.exists() or not recv_csv.exists():
            print(f"skip {runner}: missing pub or recv CSV", file=sys.stderr)
            continue
        pubts = collect_pub_timestamps(recv_csv)
        lines, missing = synth_publish_lines(pub_csv, pubts)
        all_lines.extend(lines)
        total_missing += missing
        print(f"{runner}: emitted {len(lines)} PUBLISH lines, "
              f"dropped {missing} pubs with no matching recv", file=sys.stderr)

    observer_log = Path(args.observer_log)
    out_log = Path(args.out_log)
    out_log.parent.mkdir(parents=True, exist_ok=True)
    with out_log.open("w") as f:
        if observer_log.exists():
            with observer_log.open() as g:
                f.write(g.read())
        for line in all_lines:
            f.write(line + "\n")
    print(f"merged log: {out_log} (psmark pubs added: {len(all_lines)}, "
          f"dropped: {total_missing})", file=sys.stderr)


if __name__ == "__main__":
    main()
