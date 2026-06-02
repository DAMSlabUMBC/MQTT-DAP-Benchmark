#!/bin/bash
# Targeted verification of the two bug fixes before resuming the full sweep:
#   bug 1 (multi-node serialization) -> baseline/2xdev rep1 must produce throughput.csv
#                                       in both runner1/ and runner2/.
#   bug 2 (PSMark cleanup wall budget) -> baseline/2srv rep1 must produce throughput.csv
#                                         after the 80-device metric calc finishes.

set -u

REPS=1 PHASE_A_EXPERIMENTS='baseline|2xdev|baseline_2xdev|runner1 runner2|1884||benchmark-mosquitto-vanilla
baseline|2srv|baseline_2srv|runner1|1884||benchmark-mosquitto-vanilla' \
  bash "$(dirname "$0")/orchestrator.sh"
