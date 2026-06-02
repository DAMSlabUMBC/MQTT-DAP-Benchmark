#!/bin/bash
# Phase A sweep driver. Runs 12 experiments x 5 reps across three system types.
# Layout: /tmp/dap_eval/<system>/<variant>/rep<N>/
#
# Per-rep flow:
#   baseline -> restart vanilla broker, launch PSMark VM(s), wait, snapshot, gzip
#   topic    -> restart vanilla broker, launch observer (PM_TOPIC_ENCODED) + PSMark, snapshot, gzip
#   dap      -> restart DAP broker, launch observer (PM_UNIFIED) + PSMark, adapter, analyze, gzip
#
# Failures are caught per-rep and recorded; the sweep continues.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPS=${REPS:-5}
EVAL_ROOT=/tmp/dap_eval
PSMARK=/Users/nathansamson/PS-Bench/psmark
HARNESS=/Users/nathansamson/MQTT-DAP-Benchmark
VENV=$HARNESS/venv
ADAPTER="$SCRIPT_DIR/psmark_pub_adapter.py"
SUMMARIZE="$SCRIPT_DIR/summarize.py"
WALL_MS=240000   # 240s wall budget per rep: 180s scenario + 60s for PSMark cleanup/metric-calc (needed at 80+ devices)

mkdir -p "$EVAL_ROOT"
P="$EVAL_ROOT/progress.log"
F="$EVAL_ROOT/failures.log"
[ -f "$P" ] && mv "$P" "$P.prev"
[ -f "$F" ] && mv "$F" "$F.prev"
: > "$P"; : > "$F"

log() { echo "$(date '+%H:%M:%S') $*" >> "$P"; }

# Experiment table: system|variant|psmark_scenario|deployment_runners|broker_port|observer_cfg|broker_container
EXPERIMENTS=(
  "baseline|base|baseline_base|runner1|1884||benchmark-mosquitto-vanilla"
  "baseline|2xdev|baseline_2xdev|runner1 runner2|1884||benchmark-mosquitto-vanilla"
  "baseline|2srv|baseline_2srv|runner1|1884||benchmark-mosquitto-vanilla"
  "baseline|2srv_2x|baseline_2srv_2x|runner1 runner2|1884||benchmark-mosquitto-vanilla"
  "topic|base_purpose|topic_base_purpose|runner1|1884|$HARNESS/test-configs/phase_a/topic_base_purpose.cfg|benchmark-mosquitto-vanilla"
  "topic|double_purpose|topic_double_purpose|runner1|1884|$HARNESS/test-configs/phase_a/topic_double_purpose.cfg|benchmark-mosquitto-vanilla"
  "topic|triple_purpose|topic_triple_purpose|runner1|1884|$HARNESS/test-configs/phase_a/topic_triple_purpose.cfg|benchmark-mosquitto-vanilla"
  "dap|base|dap_static_base|runner1|1883|$HARNESS/test-configs/dap_observed/observer_4n_unified.cfg|benchmark-mosquitto-unified"
  "dap|2xdev|dap_static_2xdev|runner1 runner2|1883|$HARNESS/test-configs/dap_observed/observer_4n_unified.cfg|benchmark-mosquitto-unified"
  "dap|2srv|dap_static_2srv|runner1|1883|$HARNESS/test-configs/dap_observed/observer_4n_unified.cfg|benchmark-mosquitto-unified"
  "dap|2srv_2x|dap_static_2srv_2x|runner1 runner2|1883|$HARNESS/test-configs/dap_observed/observer_4n_unified.cfg|benchmark-mosquitto-unified"
  "dap|double_purpose|dap_double_purpose|runner1|1883|$HARNESS/test-configs/phase_a/dap_double_purpose.cfg|benchmark-mosquitto-unified"
)

# Allow targeted reruns by setting PHASE_A_EXPERIMENTS to a newline-separated subset.
if [ -n "${PHASE_A_EXPERIMENTS:-}" ]; then
  IFS=$'\n' read -d '' -ra EXPERIMENTS <<< "$PHASE_A_EXPERIMENTS" || true
fi

wait_broker_ready() {
  local port=$1
  for i in $(seq 1 60); do
    if nc -z 127.0.0.1 "$port" 2>/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

run_rep() {
  local system=$1 variant=$2 scenario=$3 runners="$4" port=$5 obs_cfg="$6" broker="$7" rep=$8
  local rep_dir="$EVAL_ROOT/$system/$variant/rep${rep}"

  local complete=1
  for role in $runners; do
    [ -f "$rep_dir/$role/result/throughput.csv" ] || { complete=0; break; }
  done
  if [ "$complete" -eq 1 ]; then
    log "  rep$rep skip (already complete)"
    return
  fi

  rm -rf "$rep_dir"
  mkdir -p "$rep_dir"
  log "  rep$rep start ($system/$variant scenario=$scenario port=$port broker=$broker)"

  docker restart "$broker" >/dev/null 2>&1
  if ! wait_broker_ready "$port"; then
    echo "$system/$variant/rep$rep: broker not ready" >> "$F"
    log "  rep$rep BROKER_TIMEOUT"
    return
  fi

  local obs_pid=""
  if [ -n "$obs_cfg" ]; then
    local obs_log="$rep_dir/observer.log"
    (
      cd "$HARNESS/benchmark"
      "$VENV/bin/python" Benchmark.py run "$obs_cfg" 127.0.0.1 -p "$port" -o "$obs_log"
    ) > "$rep_dir/observer.stdout" 2> "$rep_dir/observer.stderr" &
    obs_pid=$!
    sleep 5
  fi

  local psmark_pids=()
  for role in $runners; do
    local dir="$rep_dir/$role"
    mkdir -p "$dir"
    rm -f "$dir/configs" "$dir/_build"
    ln -sf "$PSMARK/configs" "$dir/configs"
    ln -sf "$PSMARK/_build" "$dir/_build"
    (
      cd "$dir"
      ERLANG_DIST_MODE=longnames erl \
        -name "${role}@127.0.0.1" -setcookie psmark_cookie \
        -pa _build/default/lib/*/ebin \
        -config configs/ps_bench \
        -noinput \
        -eval "application:load(ps_bench), application:set_env(ps_bench, node_name, ${role}), application:set_env(ps_bench, selected_scenario, ${scenario}), {ok, _} = application:ensure_all_started(mnesia), {ok, _} = application:ensure_all_started(ps_bench), timer:sleep(${WALL_MS}), init:stop()." \
        > "$dir/run.out" 2> "$dir/run.err"
    ) &
    psmark_pids+=("$!")
  done

  local psmark_rc_all=0
  for pid in "${psmark_pids[@]}"; do
    wait "$pid"; rc=$?
    [ "$rc" -ne 0 ] && psmark_rc_all=$rc
  done
  log "  rep$rep psmark rc=$psmark_rc_all"
  [ "$psmark_rc_all" -ne 0 ] && echo "$system/$variant/rep$rep: psmark rc=$psmark_rc_all" >> "$F"

  if [ -n "$obs_pid" ]; then
    wait "$obs_pid"; obs_rc=$?
    log "  rep$rep observer rc=$obs_rc"
    [ "$obs_rc" -ne 0 ] && echo "$system/$variant/rep$rep: observer rc=$obs_rc" >> "$F"
  fi

  for role in $runners; do
    src=$(ls -t "$rep_dir/$role/results" 2>/dev/null | head -1)
    if [ -n "$src" ]; then
      mv "$rep_dir/$role/results/$src" "$rep_dir/$role/result" 2>/dev/null
      rm -rf "$rep_dir/$role/results" "$rep_dir/$role/configs" "$rep_dir/$role/_build" "$rep_dir/$role/Mnesia."*
    else
      echo "$system/$variant/rep$rep/$role: no results dir" >> "$F"
    fi
  done

  if [ "$system" = "dap" ] && [ -n "$obs_cfg" ]; then
    local merged="$rep_dir/merged.log"
    python3 "$ADAPTER" --psmark-root "$rep_dir" \
                       --observer-log "$rep_dir/observer.log" \
                       --out-log "$merged" \
                       > "$rep_dir/adapter.log" 2>&1
    (
      cd "$HARNESS/benchmark"
      "$VENV/bin/python" Benchmark.py analyze "$merged"
    ) > "$rep_dir/analyze.stdout" 2> "$rep_dir/analyze.stderr"
  fi

  for big in "$rep_dir/observer.log" "$rep_dir/merged.log"; do
    [ -f "$big" ] && gzip "$big"
  done

  log "  rep$rep end"
}

START_TS=$(date '+%s')
for exp in "${EXPERIMENTS[@]}"; do
  IFS='|' read -r system variant scenario runners port obs_cfg broker <<<"$exp"
  log "==== $system/$variant start ===="
  for rep in $(seq 1 "$REPS"); do
    run_rep "$system" "$variant" "$scenario" "$runners" "$port" "$obs_cfg" "$broker" "$rep"
  done
  log "==== $system/$variant end ===="
done
END_TS=$(date '+%s')
log "ALL_DONE elapsed=$((END_TS - START_TS))s"

python3 "$SUMMARIZE" > "$EVAL_ROOT/summary.txt" 2>> "$F"
log "summary -> $EVAL_ROOT/summary.txt"
