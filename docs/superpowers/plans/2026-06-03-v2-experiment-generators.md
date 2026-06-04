# v2 Experiment Generators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a seeded generator that emits the 20-config v2 correctness matrix (paper §7.3) for the existing full-harness mode, plus the one engine edit (op-issuer pinning) the matrix requires.

**Architecture:** A standalone Python generator (`scripts/v2/generate_v2_configs.py`) of pure builder functions that assemble config dicts and emit YAML consumed by the existing `ConfigParser`. Workload uses PSMark-F per-device *rates* with fixed 100-byte payloads (correctness is payload-independent; see spec §3–4). One small `TestExecutor` change pins operational requests to a single seeded publisher. All correctness smoke runs target Paladin loopback.

**Tech Stack:** Python 3.10, PyYAML, pytest (dev), the existing `benchmark/` engine (paho-mqtt 2.1.0).

**Spec:** `docs/superpowers/specs/2026-06-03-v2-experiment-generators-design.md`

---

## File Structure

- **Create** `scripts/v2/__init__.py` — package marker.
- **Create** `scripts/v2/psmark_f_profile.py` — the PSMark-F rate table + op vocabulary constants (one responsibility: workload constants).
- **Create** `scripts/v2/generate_v2_configs.py` — builders + per-set assembly + CLI.
- **Create** `scripts/v2/tests/__init__.py`
- **Create** `scripts/v2/tests/test_builders.py` — unit tests for pure builders.
- **Create** `scripts/v2/tests/test_assembly.py` — per-set assembly + ConfigParser round-trip.
- **Modify** `benchmark/TestExecutor.py` — pin op issuer (`__init__` field + `_select_op_issuer` + the loop in `_send_operational_requests_if_ready`).
- **Create** `benchmark/tests/__init__.py`, `benchmark/tests/test_op_issuer.py` — unit test for op-issuer pinning.
- **Modify** `benchmark/requirements.txt` — add `pytest`.
- **Create** `test-configs/v2/` (output tree, generated).
- **Create** `docs/superpowers/runbooks/2026-06-03-v2-smoke-gates.md` — Gate B/W/F/C runbook.

**Constants (single source of truth, defined in Task 2 / Task 3):**
- `SEED = 1074`, `N_PUBLISHERS = 40`, `DURATION_MS = 180100`, `FIXED_PAYLOAD = 100`
- `OP_SEND_RATE_MS = 10000`, `TICK_START_MS = 10100`, `TICK_INTERVAL_MS = 10000` (ticks 10100..170100 ⇒ 17 ticks < 180000)
- `DISCONNECT_MS = 60100`, `RECONNECT_MS = 120100`
- `START_PUBLISH_MS = 100`

---

## Task 1: Scaffolding + pytest

**Files:**
- Create: `scripts/v2/__init__.py`, `scripts/v2/tests/__init__.py`, `benchmark/tests/__init__.py`
- Modify: `benchmark/requirements.txt`

- [ ] **Step 1: Create package markers**

```bash
mkdir -p scripts/v2/tests benchmark/tests
: > scripts/v2/__init__.py
: > scripts/v2/tests/__init__.py
: > benchmark/tests/__init__.py
```

- [ ] **Step 2: Add pytest to requirements**

Append to `benchmark/requirements.txt`:

```
pytest==8.3.3
```

- [ ] **Step 3: Install**

Run: `pip install -r benchmark/requirements.txt`
Expected: pytest installs (or "already satisfied").

- [ ] **Step 4: Verify pytest collects nothing yet**

Run: `cd scripts/v2 && python -m pytest -q`
Expected: "no tests ran".

- [ ] **Step 5: Commit**

```bash
git add scripts/v2 benchmark/tests benchmark/requirements.txt
git commit -m "chore: scaffold v2 generator package and pytest"
```

---

## Task 2: PSMark-F rate profile constants

**Files:**
- Create: `scripts/v2/psmark_f_profile.py`
- Test: `scripts/v2/tests/test_builders.py`

The 10 smart-factory device types × 4 instances = 40 publishers, with `pub_period_ms` from
`PSMark-MQTT-DAP/.../devices/smart_factory/*.device` (`publication_frequency_ms`). Payload fixed at 100 B.

- [ ] **Step 1: Write the failing test**

In `scripts/v2/tests/test_builders.py`:

```python
from scripts.v2 import psmark_f_profile as prof


def test_profile_expands_to_40_publishers():
    rows = prof.expand_publisher_rows()
    assert len(rows) == 40


def test_profile_rates_match_psmark_f():
    by_type = {r["device_type"]: r["pub_period_ms"] for r in prof.expand_publisher_rows()}
    assert by_type["robot_imu"] == 10
    assert by_type["robot_odometry"] == 10
    assert by_type["robot_nearmap"] == 20
    assert by_type["robot_farmap"] == 50
    assert by_type["robot_lidar"] == 100
    assert by_type["vibration_sensor"] == 60000
    assert by_type["machine_temperature_sensor"] == 1000


def test_op_vocabulary():
    assert prof.C1_REG_OPS == ["REGISTER-INFO"]
    assert prof.C1_OPS == []
    assert prof.C2_OPS == ["AUDIT", "HISTORY"]
    assert prof.C3_OPS == ["UPDATE", "DELETE", "RESTRICT"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: FAIL — `ModuleNotFoundError: scripts.v2.psmark_f_profile`.

- [ ] **Step 3: Write the implementation**

In `scripts/v2/psmark_f_profile.py`:

```python
"""PSMark-F (smart-factory) workload constants for the v2 correctness matrix.

Rates are taken from PSMark's smart_factory .device profiles
(publication_frequency_ms); payloads are fixed-small (correctness is
payload-independent — see spec sections 3-4). The publisher set mirrors the
PSMark dap_scale_40p_1n deployment: 10 device types x 4 instances = 40.
"""

# (device_type, pub_period_ms, instances_per_type)
PSMARK_F_DEVICES = [
    ("machine_temperature_sensor", 1000, 4),
    ("machine_speed_sensor", 1000, 4),
    ("machine_energy_consumption", 1000, 4),
    ("production_quality_sensor", 1000, 4),
    ("vibration_sensor", 60000, 4),
    ("robot_farmap", 50, 4),
    ("robot_nearmap", 20, 4),
    ("robot_imu", 10, 4),
    ("robot_odometry", 10, 4),
    ("robot_lidar", 100, 4),
]

FIXED_PAYLOAD_BYTES = 100

# Legacy unified operation vocabulary (matches set3/set4 configs).
C1_REG_OPS = ["REGISTER-INFO"]
C1_OPS = []
C2_OPS = ["AUDIT", "HISTORY"]
C3_OPS = ["UPDATE", "DELETE", "RESTRICT"]


def expand_publisher_rows():
    """Expand the device table into one row per publisher (40 total).

    Returns a list of dicts: {device_type, pub_period_ms} in deployment order.
    """
    rows = []
    for device_type, period_ms, count in PSMARK_F_DEVICES:
        for _ in range(count):
            rows.append({"device_type": device_type, "pub_period_ms": period_ms})
    return rows
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/psmark_f_profile.py scripts/v2/tests/test_builders.py
git commit -m "feat: PSMark-F rate profile constants for v2"
```

---

## Task 3: Publisher definitions + instances + purpose assignment

**Files:**
- Create: `scripts/v2/generate_v2_configs.py`
- Test: `scripts/v2/tests/test_builders.py`

Each publisher gets its own topic `device/devNN` (NN zero-padded, 01..40). Purpose assignment:
publisher `i` (1-indexed) → `p[((i-1) % min(N, 40)) + 1]`.

- [ ] **Step 1: Write the failing test**

Append to `scripts/v2/tests/test_builders.py`:

```python
from scripts.v2 import generate_v2_configs as gen


def test_publisher_definitions_have_unique_topics():
    defs = gen.build_publisher_definitions()
    assert len(defs) == 40
    topics = [d["topic"] for d in defs]
    assert topics[0] == "device/dev01"
    assert topics[-1] == "device/dev40"
    assert len(set(topics)) == 40
    # rate carried through from the profile; payload fixed at 100
    assert defs[0]["pub_period_ms"] == 1000
    assert defs[0]["min_payload_bytes"] == 100
    assert defs[0]["max_payload_bytes"] == 100


def test_publisher_purpose_assignment_n10():
    insts = gen.build_publisher_instances(10)
    assert len(insts) == 40
    # round-robin over p1..p10
    assert insts[0]["purpose_filter"] == "p1"
    assert insts[9]["purpose_filter"] == "p10"
    assert insts[10]["purpose_filter"] == "p1"
    used = {i["purpose_filter"] for i in insts}
    assert used == {f"p{k}" for k in range(1, 11)}


def test_publisher_purpose_assignment_n100_uses_only_40():
    insts = gen.build_publisher_instances(100)
    used = {i["purpose_filter"] for i in insts}
    # only min(100,40)=40 distinct purposes are published
    assert used == {f"p{k}" for k in range(1, 41)}


def test_publisher_purpose_assignment_n1():
    insts = gen.build_publisher_instances(1)
    assert {i["purpose_filter"] for i in insts} == {"p1"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: FAIL — `ModuleNotFoundError: scripts.v2.generate_v2_configs`.

- [ ] **Step 3: Write the implementation**

Create `scripts/v2/generate_v2_configs.py` with the constants block and these builders:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/generate_v2_configs.py scripts/v2/tests/test_builders.py
git commit -m "feat: v2 publisher definitions and purpose assignment"
```

---

## Task 4: Subscribers + purpose definitions

**Files:**
- Modify: `scripts/v2/generate_v2_configs.py`
- Test: `scripts/v2/tests/test_builders.py`

One subscriber definition with wildcard `device/+`; N instances each a distinct purpose `p1..pN`.

- [ ] **Step 1: Write the failing test**

Append to `scripts/v2/tests/test_builders.py`:

```python
def test_subscriber_definition_is_wildcard():
    sdef = gen.build_subscriber_definition()
    assert sdef["type"] == "subscriber"
    assert sdef["topic_filter"] == "device/+"


def test_subscriber_instances_one_per_purpose():
    insts = gen.build_subscriber_instances(100)
    assert len(insts) == 100
    assert insts[0]["purpose_filter"] == "p1"
    assert insts[99]["purpose_filter"] == "p100"
    assert insts[0]["device_def_id"] == "device_subscriber"
    assert insts[0]["instance_id"] == "device_subscriber_p1"


def test_purpose_definitions_count():
    pdefs = gen.build_purpose_definitions(10)
    assert len(pdefs) == 10
    assert pdefs[0] == {"id": "p1", "description": "Purpose 1"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: FAIL — `AttributeError: ... has no attribute 'build_subscriber_definition'`.

- [ ] **Step 3: Write the implementation**

Append to `scripts/v2/generate_v2_configs.py`:

```python
SUBSCRIBER_DEF_ID = "device_subscriber"


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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/generate_v2_configs.py scripts/v2/tests/test_builders.py
git commit -m "feat: v2 subscribers and purpose definitions"
```

---

## Task 5: Seeded 25% subset selection (round-half-up, min 1)

**Files:**
- Modify: `scripts/v2/generate_v2_configs.py`
- Test: `scripts/v2/tests/test_builders.py`

Subset size = round-half-up of `0.25 * count`, min 1. Selection is seeded and stable.

- [ ] **Step 1: Write the failing test**

Append to `scripts/v2/tests/test_builders.py`:

```python
def test_subset_size_round_half_up():
    assert gen.subset_size(40) == 10
    assert gen.subset_size(10) == 3   # round-half-up of 2.5
    assert gen.subset_size(100) == 25
    assert gen.subset_size(1) == 1    # min 1


def test_subset_selection_is_deterministic():
    ids = [f"dev{i:02d}" for i in range(1, 41)]
    a = gen.select_subset(ids, label="mp")
    b = gen.select_subset(ids, label="mp")
    assert a == b
    assert len(a) == 10
    assert set(a).issubset(set(ids))


def test_subset_selection_differs_by_label():
    ids = [f"dev{i:02d}" for i in range(1, 41)]
    assert gen.select_subset(ids, label="mp") != gen.select_subset(ids, label="sp")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: FAIL — `AttributeError: ... 'subset_size'`.

- [ ] **Step 3: Write the implementation**

Append to `scripts/v2/generate_v2_configs.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: PASS (13 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/generate_v2_configs.py scripts/v2/tests/test_builders.py
git commit -m "feat: seeded 25% subset selection (round-half-up)"
```

---

## Task 6: Dynamic change_purpose events

**Files:**
- Modify: `scripts/v2/generate_v2_configs.py`
- Test: `scripts/v2/tests/test_builders.py`

Each tick (10100..170100) emits one `change_purpose` event moving the whole subset to one shared
purpose, cycling `p1..pN`. Tick `t` (0-indexed) → `p[(t % N) + 1]`.

- [ ] **Step 1: Write the failing test**

Append to `scripts/v2/tests/test_builders.py`:

```python
def test_tick_times():
    ticks = gen.tick_times()
    assert ticks[0] == 10100
    assert ticks[-1] == 170100
    assert len(ticks) == 17


def test_change_purpose_events_cycle_purposes():
    subset = ["dev01", "dev05"]
    evs = gen.change_purpose_events(subset, n_purposes=10)
    assert len(evs) == 17
    assert evs[0]["time_ms"] == 10100
    assert evs[0]["type"] == "change_purpose"
    assert evs[0]["devices"] == ["dev01", "dev05"]
    assert evs[0]["new_purpose"] == "p1"
    assert evs[1]["new_purpose"] == "p2"
    # cycles after N
    assert evs[10]["new_purpose"] == "p1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: FAIL — `AttributeError: ... 'tick_times'`.

- [ ] **Step 3: Write the implementation**

Append to `scripts/v2/generate_v2_configs.py`:

```python
def tick_times():
    times = []
    t = TICK_START_MS
    while t < DURATION_MS - TICK_INTERVAL_MS:
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: PASS (15 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/generate_v2_configs.py scripts/v2/tests/test_builders.py
git commit -m "feat: dynamic change_purpose event generation"
```

---

## Task 7: Lifecycle + connectivity events

**Files:**
- Modify: `scripts/v2/generate_v2_configs.py`
- Test: `scripts/v2/tests/test_builders.py`

Every config starts with `connect_all`@0 + `start_publishing_all`@100 and ends with
`disconnect_all`@180100. Set v adds `disconnect`@60100 / `reconnect`@120100 for a seeded 25% of
subscriber instance-ids.

- [ ] **Step 1: Write the failing test**

Append to `scripts/v2/tests/test_builders.py`:

```python
def test_lifecycle_events():
    evs = gen.lifecycle_events()
    assert evs[0] == {"time_ms": 0, "type": "connect_all",
                      "description": "Connect all devices"}
    assert evs[1]["type"] == "start_publishing_all"
    assert evs[1]["time_ms"] == 100
    assert evs[-1] == {"time_ms": 180100, "type": "disconnect_all",
                       "description": "Disconnect all devices"}


def test_connectivity_events():
    sub_ids = [f"device_subscriber_p{k}" for k in range(1, 11)]
    evs = gen.connectivity_events(sub_ids)
    assert len(evs) == 2
    assert evs[0]["time_ms"] == 60100 and evs[0]["type"] == "disconnect"
    assert evs[1]["time_ms"] == 120100 and evs[1]["type"] == "reconnect"
    assert len(evs[0]["devices"]) == 3   # 25% of 10, round-half-up
    assert evs[0]["devices"] == evs[1]["devices"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: FAIL — `AttributeError: ... 'lifecycle_events'`.

- [ ] **Step 3: Write the implementation**

Append to `scripts/v2/generate_v2_configs.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_builders.py -q`
Expected: PASS (17 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/generate_v2_configs.py scripts/v2/tests/test_builders.py
git commit -m "feat: lifecycle and connectivity event generation"
```

---

## Task 8: Config assembly (header, ops block, full dict)

**Files:**
- Modify: `scripts/v2/generate_v2_configs.py`
- Test: `scripts/v2/tests/test_assembly.py`

`assemble_config(...)` returns the full top-level dict. `dynamic_side ∈ {None, "mp", "sp", "both"}`.

- [ ] **Step 1: Write the failing test**

Create `scripts/v2/tests/test_assembly.py`:

```python
from scripts.v2 import generate_v2_configs as gen


def _events_of_type(cfg, etype):
    return [e for e in cfg["test"]["scheduled_events"] if e["type"] == etype]


def test_header_keys_present():
    cfg = gen.assemble_config(set_id=1, variant="static", n_purposes=10,
                              dynamic_side=None, with_ops=False, connectivity=False)
    for key in ("node_name", "client_module_name", "output_dir",
                "purpose_management_method", "reg_by_msg_reg_topic",
                "reg_by_topic_pub_reg_topic", "or_topic_name", "ors_topic_name",
                "on_topic_name", "onp_topic_name", "osys_topic_name",
                "operational_response_topic_prefix", "operational_purpose"):
        assert key in cfg, f"missing {key}"
    assert cfg["purpose_management_method"] == 3
    assert cfg["test"]["data_qos"] == 0
    assert cfg["test"]["duration_ms"] == 180100


def test_set1_static_has_no_change_or_ops():
    cfg = gen.assemble_config(1, "static", 10, None, False, False)
    assert _events_of_type(cfg, "change_purpose") == []
    assert cfg["test"]["op_send_rate"] == 0
    assert len(cfg["test"]["device_instances"]) == 40 + 10


def test_set3_static_ops_block():
    cfg = gen.assemble_config(3, "static_ops", 10, None, True, False)
    assert cfg["test"]["op_send_rate"] == 10000
    assert cfg["test"]["c1_reg_ops"] == ["REGISTER-INFO"]
    assert cfg["test"]["c2_ops"] == ["AUDIT", "HISTORY"]
    assert cfg["test"]["c3_ops"] == ["UPDATE", "DELETE", "RESTRICT"]


def test_set2_dynamic_both_targets_pubs_and_subs():
    cfg = gen.assemble_config(2, "dynamic_both", 10, "both", False, False)
    changes = _events_of_type(cfg, "change_purpose")
    # both sides => 2 change events per tick
    assert len(changes) == 17 * 2
    devsets = {tuple(e["devices"]) for e in changes}
    assert len(devsets) == 2  # one pub subset, one sub subset


def test_set5_connectivity_has_disconnect_and_ops():
    cfg = gen.assemble_config(5, "connectivity", 10, None, True, True)
    assert len(_events_of_type(cfg, "disconnect")) == 1
    assert len(_events_of_type(cfg, "reconnect")) == 1
    assert cfg["test"]["op_send_rate"] == 10000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_assembly.py -q`
Expected: FAIL — `AttributeError: ... 'assemble_config'`.

- [ ] **Step 3: Write the implementation**

Append to `scripts/v2/generate_v2_configs.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_assembly.py -q`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/generate_v2_configs.py scripts/v2/tests/test_assembly.py
git commit -m "feat: v2 config assembly across sets"
```

---

## Task 9: Matrix enumeration + YAML emission + CLI

**Files:**
- Modify: `scripts/v2/generate_v2_configs.py`
- Test: `scripts/v2/tests/test_assembly.py`

`build_matrix()` returns the 20 `(set_dir, filename, cfg)` tuples. `main()` writes them or `--dry-run`.

- [ ] **Step 1: Write the failing test**

Append to `scripts/v2/tests/test_assembly.py`:

```python
def test_matrix_is_20_configs():
    matrix = gen.build_matrix()
    assert len(matrix) == 20
    names = sorted(fn for _, fn, _ in matrix)
    # spot-check coverage of each set
    assert any(n.startswith("v2_set1_static_1p") for n in names)
    assert any(n.startswith("v2_set1_static_100p") for n in names)
    assert sum(1 for n in names if n.startswith("v2_set2_")) == 6
    assert sum(1 for n in names if n.startswith("v2_set4_")) == 6
    assert sum(1 for n in names if n.startswith("v2_set5_")) == 2
    # set5 only at 10p and 100p (N=1 skipped)
    assert not any(n.startswith("v2_set5_") and "_1p_" in n for n in names)


def test_matrix_filenames_unique():
    matrix = gen.build_matrix()
    fns = [fn for _, fn, _ in matrix]
    assert len(set(fns)) == len(fns)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_assembly.py -q`
Expected: FAIL — `AttributeError: ... 'build_matrix'`.

- [ ] **Step 3: Write the implementation**

Append to `scripts/v2/generate_v2_configs.py`:

```python
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
            print(f"{subdir}/{filename}: "
                  f"{len([d for d in cfg['device_definitions'] if d['type']=='publisher'])} pubs, "
                  f"{len([i for i in t['device_instances'] if i['device_def_id']=='device_subscriber'])} subs, "
                  f"{len(cfg['purpose_definitions'])} purposes, "
                  f"{len(t['scheduled_events'])} events, "
                  f"ops={t['op_send_rate']}")
        return

    paths = write_matrix(args.out_dir)
    print(f"Wrote {len(paths)} configs to {args.out_dir}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test + dry-run**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_assembly.py -q`
Expected: PASS (7 tests).
Run: `python -m scripts.v2.generate_v2_configs --dry-run`
Expected: 20 lines, each showing 40 pubs and the right sub/purpose/event counts.

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/generate_v2_configs.py scripts/v2/tests/test_assembly.py
git commit -m "feat: v2 matrix enumeration, YAML emission, CLI"
```

---

## Task 10: Generate configs + ConfigParser round-trip validation

**Files:**
- Test: `scripts/v2/tests/test_assembly.py`
- Create (generated): `test-configs/v2/**`

Prove every generated config parses through the real `benchmark/ConfigParser.py` with correct counts.

- [ ] **Step 1: Write the failing test**

Append to `scripts/v2/tests/test_assembly.py`:

```python
import os
import sys
import tempfile
import importlib

import pytest

BENCH = os.path.join(os.path.dirname(__file__), "..", "..", "benchmark")


@pytest.fixture
def config_parser():
    sys.path.insert(0, os.path.abspath(BENCH))
    import GlobalDefs  # noqa: F401  (ConfigParser imports it)
    cp_mod = importlib.import_module("ConfigParser")
    yield cp_mod
    sys.path.remove(os.path.abspath(BENCH))


def test_all_configs_roundtrip_through_configparser(config_parser):
    matrix = gen.build_matrix()
    with tempfile.TemporaryDirectory() as tmp:
        gen.write_matrix(tmp)
        for subdir, filename, cfg in matrix:
            path = os.path.join(tmp, subdir, filename)
            parser = config_parser.ConfigParser()
            parser.the_config = config_parser.BenchmarkConfiguration()
            parser.the_config.test_list = []
            parsed = parser.parse_config(path)
            tc = parsed.test_list[-1]
            n = len(cfg["purpose_definitions"])
            assert len(tc.device_instances_config) == 40 + n
            assert tc.qos == 0
            assert tc.test_duration_ms == 180100
```

> Note: `ConfigParser` uses class-level mutable defaults and `sys.exit` on bad files. The fixture
> imports it from `benchmark/`; resetting `test_list = []` per call avoids cross-test accumulation.

- [ ] **Step 2: Run test to verify it fails (then drives any emission fixes)**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest scripts/v2/tests/test_assembly.py::test_all_configs_roundtrip_through_configparser -q`
Expected: FAIL initially if any key name mismatches the parser (e.g. `osys_topic_name`). Fix emission in `generate_v2_configs.py` until it passes — the parser is the source of truth for key names.

- [ ] **Step 3: Generate the real tree**

Run: `python -m scripts.v2.generate_v2_configs --out-dir test-configs/v2`
Expected: "Wrote 20 configs to test-configs/v2".

- [ ] **Step 4: Verify count on disk**

Run: `find test-configs/v2 -name '*.cfg' | wc -l`
Expected: `20`.

- [ ] **Step 5: Commit**

```bash
git add scripts/v2/generate_v2_configs.py scripts/v2/tests/test_assembly.py test-configs/v2
git commit -m "feat: generate and validate the 20 v2 configs"
```

---

## Task 11: Engine edit — pin operational requests to one seeded publisher

**Files:**
- Modify: `benchmark/TestExecutor.py` (`__init__` ~line 59; `_send_operational_requests_if_ready` ~line 329)
- Test: `benchmark/tests/test_op_issuer.py`

Spec §5/§7: the paper has a *single* publisher issue operations. Replace the per-op `random.choice`
with a single issuer selected once (seeded) and reused. Factor selection into `_select_op_issuer` so
it is unit-testable without a broker.

- [ ] **Step 1: Write the failing test**

Create `benchmark/tests/test_op_issuer.py`:

```python
import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import GlobalDefs
from TestExecutor import TestExecutor


def _fake_publisher(instance_id, connected=True):
    p = types.SimpleNamespace()
    p.instance_id = instance_id
    p.is_connected = connected
    return p


def _make_executor():
    return TestExecutor("obs", "localhost", 1883, GlobalDefs.PurposeManagementMethod.PM_UNIFIED)


def test_select_op_issuer_is_single_and_stable():
    ex = _make_executor()
    pubs = [_fake_publisher(f"dev{i:02d}") for i in range(1, 41)]
    first = ex._select_op_issuer(pubs)
    # same issuer on subsequent ticks
    assert ex._select_op_issuer(pubs) is first
    assert first in pubs


def test_select_op_issuer_deterministic_across_instances():
    pubs1 = [_fake_publisher(f"dev{i:02d}") for i in range(1, 41)]
    pubs2 = [_fake_publisher(f"dev{i:02d}") for i in range(1, 41)]
    a = _make_executor()._select_op_issuer(pubs1)
    b = _make_executor()._select_op_issuer(pubs2)
    assert a.instance_id == b.instance_id


def test_select_op_issuer_reselects_if_pinned_disconnected():
    ex = _make_executor()
    pubs = [_fake_publisher(f"dev{i:02d}") for i in range(1, 41)]
    pinned = ex._select_op_issuer(pubs)
    pinned.is_connected = False
    new = ex._select_op_issuer([p for p in pubs if p.is_connected])
    assert new.is_connected is True
    assert new is not pinned
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest benchmark/tests/test_op_issuer.py -q`
Expected: FAIL — `AttributeError: 'TestExecutor' object has no attribute '_select_op_issuer'`.

- [ ] **Step 3: Add the init field**

In `benchmark/TestExecutor.py` `__init__`, just after `self.all_operations = {}` (≈ line 60), add:

```python
        # Operational requests are issued by a single pinned publisher (seeded),
        # matching the paper's "a single publisher". Selected once on first use.
        self._op_issuer_id = None
        self._op_issuer_rng = random.Random(1074)
```

- [ ] **Step 4: Add the selection method**

In `benchmark/TestExecutor.py`, add a method near `_send_operational_requests_if_ready`:

```python
    def _select_op_issuer(self, publishers):
        """Return the single pinned op-issuing publisher (seeded, reused).

        Re-selects only if the previously pinned publisher is not in the given
        (connected) list. `publishers` is assumed to be connected publishers.
        """
        by_id = {p.instance_id: p for p in publishers}
        if self._op_issuer_id is not None and self._op_issuer_id in by_id:
            return by_id[self._op_issuer_id]
        chosen = self._op_issuer_rng.choice(sorted(publishers, key=lambda p: p.instance_id))
        self._op_issuer_id = chosen.instance_id
        return chosen
```

- [ ] **Step 5: Rewire the send loop**

In `_send_operational_requests_if_ready`, replace the per-op random pick:

```python
        # For each operation pick a random publisher
        for op, op_category in self.all_operations.items():

            # Pick a random publisher to send the request
            publisher = random.choice(publishers)

            # Send the operational request
            self._send_operational_request(publisher, op, op_category)
```

with the single pinned issuer:

```python
        # All operations are issued by a single pinned publisher (paper: "a single publisher")
        publisher = self._select_op_issuer(publishers)
        for op, op_category in self.all_operations.items():
            self._send_operational_request(publisher, op, op_category)
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd /home/nsamson/mqtt-dap-paper/MQTT-DAP-Benchmark && python -m pytest benchmark/tests/test_op_issuer.py -q`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add benchmark/TestExecutor.py benchmark/tests/test_op_issuer.py
git commit -m "feat: pin operational requests to a single seeded publisher"
```

---

## Task 12: Smoke-gate runbook (Gates B, W, F, C) on Paladin loopback

**Files:**
- Create: `docs/superpowers/runbooks/2026-06-03-v2-smoke-gates.md`

This task documents the manual/scripted gates from spec §6. It does **not** sweep — it produces a
runbook and records pass/fail. All runs use the unified broker on Paladin via loopback.

- [ ] **Step 1: Write the runbook**

Create `docs/superpowers/runbooks/2026-06-03-v2-smoke-gates.md` containing, for each gate, the exact
command and the pass criterion:

```markdown
# v2 Smoke Gates (Paladin loopback)

Prereq: unified DAP broker running locally (port 1883); `pip install -r benchmark/requirements.txt`.

## Gate B (HARD) — publisher MP re-register -> broker bump_count > 0
Run a dynamic-MP config and confirm the broker registers MP changes and routes correctly after.
  python benchmark/Benchmark.py run test-configs/v2/set2_dynamic/v2_set2_dynamic_mp_10p_unified.cfg localhost -v -o logs/gateB.log
PASS when: broker dap_broker_metrics.csv shows bump_count > 0 after the first 10100ms tick AND
  python benchmark/Benchmark.py analyze logs/gateB.log
reports, for a publisher in the MP subset, messages reaching only subscribers matching the NEW
purpose after the change (FAR=0, FRR=0 on the post-change window).

## Gate W (HARD) — wildcard scoring across 40 topics
  python benchmark/Benchmark.py run test-configs/v2/set1_static/v2_set1_static_10p_unified.cfg localhost -v -o logs/gateW.log
  python benchmark/Benchmark.py analyze logs/gateW.log
PASS when: each subscriber receives exactly the publishers whose purpose matches its filter across
all 40 device/devNN topics; FAR=FRR=0 and received-message counts are non-zero for matching purposes.

## Gate F (MUST PASS) — Paladin sustains configured msg/s at N=100
  python benchmark/Benchmark.py run test-configs/v2/set1_static/v2_set1_static_100p_unified.cfg localhost -v -o logs/gateF.log
PASS when: achieved aggregate publish rate is within ~10% of the configured ~1,136 msg/s (compute
from PUBLISH log-line counts / duration), with no growing send backlog. If it falls short, see spec O1.

## Gate C (confirm) — 140 clients clean lifecycle
Same N=100 run as Gate F. PASS when all 140 clients connect, the run completes, and disconnect_all
tears down cleanly with no paho connection errors in the log.
```

- [ ] **Step 2: Execute the gates (operator step, on Paladin)**

Run each command above against the live unified broker. Record results inline in the runbook
(append a "## Results <date>" section with the measured bump_count, FAR/FRR, achieved msg/s, and
client-teardown status).

- [ ] **Step 3: Decision checkpoint**

If Gate B or Gate W fails → STOP; the broker/wildcard path needs fixing before any dynamic sweep
(escalate; do not proceed). If Gate F falls short → apply spec O1 (scale fast AMR rates, document).

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/runbooks/2026-06-03-v2-smoke-gates.md
git commit -m "docs: v2 smoke-gate runbook (B/W/F/C) with results"
```

---

## Self-Review

**Spec coverage:**
- 40 pubs/own topic, sub-per-purpose wildcard, 1/10/100, QoS0, 180100ms → Tasks 3,4,8.
- Subscribers-define-count + pub purpose spread over min(N,40) → Task 3.
- 5 sets / 20 configs, set v at 10/100 only → Tasks 8,9.
- Fixed-seeded 25% shared-purpose cycling, round-half-up min 1 → Tasks 5,6,8.
- PSMark-F rates, fixed 100B payload → Tasks 2,3.
- Legacy unified ops, op_send_rate 10000 → Tasks 2,8.
- Op-issuer pinned to one seeded publisher → Task 11.
- Gates B(hard)/W(hard)/F/C on Paladin → Task 12.
- ConfigParser round-trip → Task 10.

**Placeholder scan:** none — every code/test step has concrete content.

**Type consistency:** builder names (`build_publisher_definitions`, `build_publisher_instances`,
`build_subscriber_definition`, `build_subscriber_instances`, `build_purpose_definitions`,
`subset_size`, `select_subset`, `tick_times`, `change_purpose_events`, `lifecycle_events`,
`connectivity_events`, `assemble_config`, `build_matrix`, `write_matrix`) are used consistently across
Tasks 3–10. `_select_op_issuer` / `_op_issuer_id` / `_op_issuer_rng` consistent in Task 11.

**Open dependency:** Gate F result may trigger spec O1 (rate scaling) — handled as a checkpoint, not a
silent assumption.
