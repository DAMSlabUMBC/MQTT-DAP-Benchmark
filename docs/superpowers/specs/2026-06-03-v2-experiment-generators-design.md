# v2 Experiment Generators — Design Spec

**Date:** 2026-06-03
**Scope:** Net-new generator for the v2 experiment matrix (paper §7.3). Independent of
the Phase A observer fix and Pi6 work. No changes to the broker, PSMark, or the
benchmark engine — this produces config files the existing full-harness mode consumes.

---

## 1. Goal

Generate the v2 config matrix programmatically. The matrix is a synthetic, controlled
topology: **40 publishers (one topic each = device type), one purpose per publisher, one
subscriber per purpose subscribed to all topics, QoS 0, 3-minute runs**, across five sets:

- **(i) static, no ops** — 1, 10, 100 purposes
- **(ii) dynamic, no ops** — 10 and 100 purposes × {dynamic MP, dynamic SP, both}; 25% of dynamic MPs/SPs change every 10 s
- **(iii) static, with ops** — 1, 10, 100 purposes; each operation invoked by one publisher every 10 s
- **(iv) dynamic, with ops** — as (ii) plus ops every 10 s
- **(v) dynamic connectivity** — as (iii) but 25% of subscribers disconnect at 1/3 of the run, reconnect at 2/3 — **10 and 100 purposes only** (N=1 is degenerate: a single subscriber disconnecting; skipped)

**Total: 20 configs.**

---

## 2. Locked decisions

| Decision | Choice | Consequence |
|---|---|---|
| Execution model | **Full-harness publishers** (`Benchmark.py run`) | All 40 pubs + N subs run in one Python process. `change_purpose`/ops/disconnect all work today; no PSMark engine. Performance is the Python harness's, not Erlang PSMark's. |
| Purpose count semantics | **Subscribers define the count** | N subscribers each get a unique purpose `p1..pN`; the 40 publishers are distributed over `min(N,40)` purposes. At N=100, ~60 subscribers match no publisher (intended negative-case coverage). |
| Operation vocabulary | **Legacy unified set** | `op_send_rate: 10000`; `c1_reg_ops: [REGISTER-INFO]`, `c1_ops: []`, `c2_ops: [AUDIT, HISTORY]`, `c3_ops: [UPDATE, DELETE, RESTRICT]` — identical to existing set3/set4 configs. |
| Dynamic 25% behavior | **Fixed seeded subset, shared purpose per tick** | One subset chosen at generation time; every 10 s the whole subset moves to one shared purpose, cycling `p1..pN` (legacy set2 idiom). One `change_purpose` event per tick. |
| 25% rounding | `round(0.25·count)`, **min 1** | Ensures a dynamic/disconnect cell always changes ≥1 device. |
| Topic scheme | `device/dev01 … device/dev40`; subscriber filter `device/+` | "Subscribed to all topics" = one wildcard. |
| Seed | `1074` (fixed) | Reproducible subset selection and op-publisher pinning. |
| Workload params | **PSMark-F per-device RATES only; fixed 100-byte payload** (§4) | Keeps PSMark-F `pub_period_ms` so routing/queue pressure during dynamic changes is realistic. Payload is fixed-small because correctness is payload-size-independent — this removes the ~48 MB/s bandwidth blocker. |
| Venue | **Paladin, loopback to local broker** (correctness sweep) | Correctness needs no real network; Paladin's headroom covers the 140-clients-in-one-process concern. Pi6 stays a PSMark load node for the *separate* performance runs. |

---

## 3. Architecture & topology

### Two separate jobs — do not conflate
This spec covers the **correctness sweep** only. It is deliberately split from the performance runs:

- **Correctness (this work):** full-harness Python publishers + subscribers, **Paladin loopback** to
  the local broker, **PSMark-F rates** but **fixed 100-byte payloads**. Goal: purpose-correctness /
  operational-correctness scoring (FAR/FRR, coverage/leakage), which is payload-size-independent.
- **Performance (separate work, unchanged):** the live PSMark Erlang engine on Pi6 as a load node,
  with the true PSMark-F payloads. Goal: throughput/latency/broker-cost realism.

Keeping the rates (not payloads) in the correctness job means routing and queue pressure during
dynamic MP/SP changes stay realistic without the 48 MB/s `robot_lidar` bandwidth that a 2 GB Pi
cannot (and need not) push through a Python publisher.

### Topology
Full-harness mode: `Benchmark.py run <cfg> <broker>` instantiates publishers and
subscribers in-process and drives a deterministic `EventScheduler`. The generator emits
YAML in the existing config schema (`ConfigParser` / `DeviceDefinitions`):

- **40 publisher device definitions** `dev01..dev40`, each `type: publisher`, each with its
  **own topic** `device/devNN` (topic lives on the *definition*, so 40 defs, `count: 1`).
  The device *type* (and thus rate/payload, §4) cycles in blocks of 4, matching the PSMark
  `dap_scale_40p_1n` deployment (10 types × 4).
- **1 subscriber device definition** with `topic_filter: "device/+"`, instantiated N times,
  each instance a distinct `purpose_filter` `p1..pN`. (`SubscriberDefinition` holds a single
  topic filter; the wildcard is how one subscriber covers all 40 topics.)
- **Purpose definitions** `p1..pN`.
- Static header (node/client/output, registration topics, op topics,
  `purpose_management_method: 3`, `data_qos: 0`, `monitor_broker: true`) copied from the
  existing unified configs. `duration_ms: 180100` (3 min + 100 ms start buffer, matching
  legacy).

### Publisher → purpose assignment
Publisher `i` (1-indexed) gets `p[((i-1) mod min(N,40)) + 1]`. This guarantees every one of
the `min(N,40)` published purposes has ≥1 publisher and publishers spread evenly.

---

## 4. PSMark-F workload derivation (rates only)

The correctness job keeps the PSMark-F (smart-factory) per-device **rates** so routing and queue
pressure during dynamic changes are realistic, but uses a **fixed 100-byte payload** — correctness
(purpose matching, FAR/FRR, op coverage/leakage) is payload-size-independent, and the true PSMark-F
payloads (esp. `robot_lidar` at 1.2 MB) would impose ~48 MB/s that a 2 GB Pi can't push through a
Python publisher and that adds nothing to correctness. Payload realism lives in the separate
performance job (§3).

**Source profiles:** `PSMark-MQTT-DAP/psmark/configs/builtin-test-suites/devices/smart_factory/*.device`
**Publisher set:** the `dap_scale_40p_1n` deployment (10 types × 4 instances = 40 publishers).

**Mapping:** `pub_period_ms = publication_frequency_ms`; `min_payload_bytes = max_payload_bytes = 100`
(fixed, so `random.randint(100,100)` in `TestExecutor.py:433` always yields 100 B). The
`payload_bytes_mean`/`payload_bytes_variance` and `disconnect_*`/`reconnect_*` fields are **not used**
(payloads fixed; v2 set v uses *scheduled* disconnect, §5).

| Device type (×4) | `publication_frequency_ms` → `pub_period_ms` | msg/s (×4 instances) |
|---|---|---|
| machine_temperature_sensor | 1000 | 4 |
| machine_speed_sensor | 1000 | 4 |
| machine_energy_consumption | 1000 | 4 |
| production_quality_sensor | 1000 | 4 |
| vibration_sensor | 60000 | 0.07 |
| robot_farmap | 50 | 80 |
| robot_nearmap | 20 | 200 |
| robot_imu | 10 | 400 |
| robot_odometry | 10 | 400 |
| robot_lidar | 100 | 40 |

### Workload weight (informs Gate F, §6)
Aggregate publish rate ≈ **1,136 msg/s**, dominated by the AMR/robot devices:
`robot_imu` + `robot_odometry` = 800 msg/s (10 ms period), `robot_nearmap` 200, `robot_farmap` 80,
`robot_lidar` 40. With fixed 100 B payloads this is only ~114 KB/s of data — **the load is message
*rate*, not bandwidth.** The heaviest cell is **N=100** (40 publishers + 100 subscribers = 140
clients) at this ~1,136 msg/s. Whether Paladin sustains the configured rate in one Python process is
what Gate F measures.

---

## 5. Per-set event logic & config enumeration

All sets: `connect_all`@0, `start_publishing_all`@100, `disconnect_all`@180100.
Dynamic ticks run at 10100, 20100, …, 170100 ms (17 ticks within the 180 s run).

| Set | Variants | Events added | Ops? | Configs |
|---|---|---|---|---|
| (i) static, no ops | N ∈ {1,10,100} | none | no | 3 |
| (ii) dynamic, no ops | N ∈ {10,100} × {MP, SP, both} | `change_purpose` per tick on seeded 25% subset → shared cycling purpose | no | 6 |
| (iii) static, ops | N ∈ {1,10,100} | none | yes | 3 |
| (iv) dynamic, ops | N ∈ {10,100} × {MP, SP, both} | as (ii) | yes | 6 |
| (v) dynamic connectivity | N ∈ {10,100} | `disconnect`@60100 + `reconnect`@120100 on seeded 25% of subscribers | yes (as iii) | 2 |
| | | | **Total** | **20** |

**Dynamic-side semantics (sets ii/iv):**
- *dynamic MP*: subset = `round(0.25·40)=10` publishers (seeded). Each tick: one `change_purpose`
  event moving all 10 to the next purpose in the `p1..pN` cycle. The handler re-registers the
  publisher MP (`register_publish_purpose_for_topic`).
- *dynamic SP*: subset = `floor(0.25·N + 0.5)` subscribers (**round-half-up**: N=10→**3**, N=100→25),
  min 1, seeded. Each tick: one `change_purpose` moving the subset to the next purpose; the handler
  re-subscribes with the new SP filter.
- *both*: independent seeded subsets on each side, each cycling per tick.

**Operational-request issuer (sets iii/iv/v):** the paper has *a single publisher* issue operations.
The engine currently calls `random.choice` over all publishers *per op, per tick*
(`_send_operational_requests_if_ready`), so it neither pins a single issuer nor is reproducible. To
match the paper we **pin the op issuer to one seeded publisher for the whole run**. This requires a
**minimal, in-scope engine change**: select the op-issuing publisher once (seeded `random.Random(1074)`)
at test start and reuse it, instead of re-randomizing each tick. (Generator-only seeding can't fix
this — the engine re-picks every tick.) This is the one engine edit in scope; everything else is
config generation.

**Naming:** `v2_set{N}_<variant>_<purposes>p_unified.cfg`, e.g.
`v2_set2_dynamic_mp_100p_unified.cfg`, `v2_set5_connectivity_10p_unified.cfg`.
**Output layout:** `test-configs/v2/set{1..5}_*/`.

---

## 6. Verification gates (must pass before any dynamic sweep)

These are **gates, not nice-to-haves**. Sets ii/iv exercise broker code paths that never ran
under load in Phase A (which was static-only, `bump_count=0`).

### Gate B — publisher MP re-register → broker bump (HARD GATE)
The publisher `change_purpose` path (`_handle_change_purpose` → `register_publish_purpose_for_topic`)
**was never exercised in Phase A.** Sets ii/iv depend on broker code that has never handled an MP
change under load. A smoke run of one dynamic-MP config **must** confirm:
1. broker `bump_count > 0` after the first `change_purpose` tick, and
2. post-change routing is correct — messages from a re-registered publisher reach exactly the
   subscribers whose purpose matches the *new* MP (and no longer the old one), scored via
   `MetricsCalculator` FAR/FRR.

No dynamic sweep (ii or iv) runs until Gate B passes.

### Gate W — wildcard correctness across 40 topics (HARD GATE)
Confirm a `device/+` subscriber under unified PM scores correctly across 40 distinct publisher
topics — i.e. the `(sending_client, corr_data)` correctness join holds when one subscription
fans out over 40 topics. Phase A proved `+` *receives*; this proves it *scores*.

### Gate C — 140 clients in one process (addressed by venue; confirm on first smoke run)
N=100 means 40 publishers + 100 subscribers = 140 paho clients with `loop_start()` threads in one
Python process. Running on **Paladin** (not a Pi) is the mitigation — its headroom covers this; the
Phase A orchestrator budgeted 60 s cleanup "at 80+ devices" on far weaker hardware. No longer a
feasibility blocker, but the first 100p smoke run must still confirm 140 clients connect, run, and
tear down cleanly without dropped connections or scheduler stalls.

### Gate F — Paladin sustains configured msg/s (MUST PASS, before any full sweep)
With payloads fixed at 100 B the risk is **message rate, not bandwidth**. Confirm Paladin (loopback)
sustains the configured aggregate publish rate — achieved vs. configured msg/s per device type over a
full run — for the **heaviest cell, N=100**, where the fast AMR rates (`robot_imu`/`robot_odometry`
@ 10 ms = 800 msg/s, `robot_nearmap` 200) dominate. If the single-process Python harness can't keep up
even at small payloads, see open question O1 before sweeping.

---

## 7. Build steps

1. **Engine edit (op-issuer pinning, §5):** in `TestExecutor`, select the op-issuing publisher once
   from a seeded `random.Random(1074)` at test start and reuse it across ticks, replacing the
   per-tick `random.choice`. Smallest change that matches the paper's single-publisher behavior and
   makes op runs reproducible. This is the only engine change in scope.
2. Write `scripts/v2/generate_v2_configs.py`: builders `build_publishers()`,
   `build_subscribers(N)`, `assign_pub_purposes(N)`, `purpose_defs(N)`, `ops_block()`,
   per-set `emit_set{1..5}()`, seeded subset selection, YAML emit. PSMark-F rate table (§4) baked in
   as a constant; payload fixed at 100 B. `--dry-run` prints per-config counts (pubs, subs, purposes,
   events) without writing.
3. Generate all 20 configs into `test-configs/v2/`.
4. **Parse-validation:** round-trip every config through `ConfigParser.parse_config` (no run) — all
   parse cleanly, counts match intent.
5. **Smoke runs (the gates, §6), on Paladin loopback:** Gate B (a dynamic-MP config), Gate W, Gate F
   (a 100p config), and confirm Gate C on the 100p run. Record results.
6. Only after Gates B/W/F pass (and Gate C confirmed): hand off to the sweep orchestrator (separate
   work), run on Paladin.

---

## 8. Out of scope / open questions

**Out of scope:** broker/PSMark changes; the sweep orchestrator; metrics aggregation; the Phase A
observer fix; the separate Pi6/PSMark performance runs. (The one in-scope engine edit is the
op-issuer pinning, §7 step 1.)

**Resolved (recorded for traceability):**
- **O2 (payload) — RESOLVED:** drop moment-matching; fixed 100 B. Correctness is payload-independent;
  PSMark-F payload realism stays in the performance job.
- **O3 (rounding) — RESOLVED:** round-half-up, min 1 → dynamic-SP subset N=10→3, N=100→25.
- **O4 (op issuer) — RESOLVED:** pin to one seeded publisher for the whole run (engine edit, §7).

**Remaining open question:**
- **O1 (rate fallback):** If Gate F shows Paladin can't sustain the configured msg/s even at 100 B
  payloads, the fallback is to scale down the fast AMR rates (`robot_imu`/`robot_odometry`/`nearmap`)
  and document the deviation, OR report achieved-vs-configured and proceed. **Recommend deciding after
  measuring** — Paladin headroom makes a shortfall unlikely.
