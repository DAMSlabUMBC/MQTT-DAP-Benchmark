# v2 Smoke Gates (Paladin loopback)

Venue: `paladin.dams.lab`, unified DAP broker container `benchmark-mosquitto-unified`
on loopback `127.0.0.1:1883` (node exporter `:9100`). Use `python3` (no `python` on PATH).

**Broker teardown note:** the unified broker segfaults on run teardown (exit 139, leaves
1883 refused). **Restart it between runs:** `docker restart benchmark-mosquitto-unified`
then wait for `:1883` to reopen.

Prereqs: `pip install -r benchmark/requirements.txt` (paho-mqtt 2.x, PyYAML, rich, ischedule).

---

## Gate OP — op issuer is a single pinned publisher (HARD prerequisite for sets iii/iv/v)
The Task 11 engine edit must be proven at runtime before trusting any ops results.

    python3 benchmark/Benchmark.py run test-configs/v2/set3_static_ops/v2_set3_static_ops_10p_unified.cfg 127.0.0.1 -o logs/gateOP.log

PASS when: across all op ticks, every periodic operational request (AUDIT/HISTORY/UPDATE/
DELETE/RESTRICT) is issued by exactly ONE publisher client — not a rotating set:

    grep '^PUBLISH_OP@@' logs/gateOP.log \
      | awk -F'@@' '$7 ~ /^(AUDIT|HISTORY|UPDATE|DELETE|RESTRICT)$/ {print $4}' \
      | sort -u

Expected: exactly one distinct client id. (REGISTER-INFO is excluded — it is issued by
subscribers at setup, not the pinned publisher.)

## Gate B — publisher MP re-register -> broker bump_count > 0 (HARD)
    docker restart benchmark-mosquitto-unified   # then wait for :1883
    python3 benchmark/Benchmark.py run test-configs/v2/set2_dynamic/v2_set2_dynamic_mp_10p_unified.cfg 127.0.0.1 -o logs/gateB.log
    python3 benchmark/Benchmark.py analyze logs/gateB.log

PASS when: broker dap metrics show bump_count > 0 after the first 10100ms tick AND analyze
reports correct post-change routing (a re-registered publisher reaches only the new-purpose
subscribers; FAR/FRR clean on the post-change window).

## Gate W — wildcard scoring across 40 topics (HARD)
    docker restart benchmark-mosquitto-unified   # then wait for :1883
    python3 benchmark/Benchmark.py run test-configs/v2/set1_static/v2_set1_static_10p_unified.cfg 127.0.0.1 -o logs/gateW.log
    python3 benchmark/Benchmark.py analyze logs/gateW.log

PASS when: each subscriber receives exactly the publishers whose purpose matches its filter
across all 40 device/devNN topics; FAR=FRR=0, non-zero received counts for matching purposes.

## Gate F — Paladin sustains configured msg/s at N=100 (MUST PASS)
    docker restart benchmark-mosquitto-unified   # then wait for :1883
    python3 benchmark/Benchmark.py run test-configs/v2/set1_static/v2_set1_static_100p_unified.cfg 127.0.0.1 -o logs/gateF.log

PASS when: achieved aggregate publish rate is within ~10% of configured ~1,136 msg/s
(PUBLISH lines / duration), no growing backlog. Short → spec O1 (scale fast AMR rates).

## Gate C — 140 clients clean lifecycle (confirm on the Gate F run)
PASS when all 140 clients connect, the run completes, disconnect_all tears down cleanly with
no paho connection errors in logs/gateF.log.

---

## Results (2026-06-03, paladin.dams.lab, broker benchmark-mosquitto-unified)

**Root-cause finding first:** the legacy config header (copied into the v2 generator) used DAP
topics that don't match the current broker. The broker (`include/mosquitto/defs.h`) hardcodes
`$MP_REG`, `$OP_SYS`, `OP_REQ`, `OP_NOTIF`; the configs used `$DAP/purpose_management`, `$OSYS`,
`ORS`, `ONP`. Full-harness publishers were rejected ("No message purpose registered … rejecting")
and disconnected → RECV 0. Phase A never hit this (its publishers were PSMark, which hardcodes
`$MP_REG`). Fix applied: `reg_by_msg_reg_topic` → `$MP_REG` in the generator header. The op-topic
names are still stale (see below).

| Gate | Config | Result | Evidence |
|---|---|---|---|
| W (wildcard scoring) | set1_static_10p | **PASS** | FAR=FRR=0, 156,394 valid / 0 invalid, 869 msg/s, 0 rejects |
| B (MP re-register) | set2_dynamic_mp_10p | **PASS on routing; bump_count=0** | 17 MP changes fired; subset pub dev08 cycles purpose p8→p1→…→p10 per tick while dev01 stays p1; FAR=FRR=0 → every re-purposed msg routed to the new-purpose subscriber. bump_count=0 because QoS-0 delivery never holds msgs to re-verify (bump is for queued/SP-change). Literal "bump>0" criterion not applicable here. |
| C (140 clients) | set1_static_100p | **PASS** | 140 connect / 140 clean disconnect, 0 paho errors, 0 rejects |
| F (sustain msg/s) | set1_static_100p | **FAIL (literal)** | 845 msg/s achieved vs ~1,136 configured (~26% short). Correctness perfect (FAR=FRR=0, 151,920/151,920). Single-process Python can't hit the 10 ms AMR cadence. → spec O1. |
| OP (single op issuer) | set3_static_ops_10p | **PASS on criterion; op cycle does NOT complete** | After the op-topic fix: one pinned publisher **dev25** issued all 90 periodic ops (18 each AUDIT/HISTORY/UPDATE/DELETE/RESTRICT), zero rotation, 0 rejects. BUT op completion = 0 (90 issued / 0 completed; PUBLISH_OP_RESP=0; REGISTER-INFO 10 issued / 0 completed). Subscribers subscribe `OP_REQ/<sub>`, publishers `OP_NOTIF/<pub>`, but no op request is received/answered. |

**Open blocker for sets iii/iv/v (op correctness):** the operational request→forward→response cycle
does not complete (completion 0, coverage 0). Wiring/topics look right; likely cause is another
stale registration topic (`reg_by_topic_sub_reg_topic: "$DAP/SP_reg"` — no matching broker constant
found) or REGISTER-INFO not registering consumer data (its completion is also 0), so AUDIT/HISTORY/etc
have no registered data to act on. Needs investigation before any iii/iv/v sweep. Data-path sets
(i, ii) and Gate OP's single-issuer guarantee are unaffected.

**Decisions (resolved 2026-06-03):**
1. **Gate B → accept routing-proof.** `bump_count=0` is correct for QoS-0 (bump only re-verifies
   held/queued messages); MP re-registration is proven by per-tick purpose cycling + FAR/FRR=0.
   Spec criterion updated. **Coverage gap flagged:** the QoS-0 matrix never exercises the broker's
   bump/held-message re-verification path (§5 queuing model) — needs a QoS>0/held case elsewhere
   (separate work), not fixed here.
2. **Gate F → report achieved-vs-configured; do NOT scale rates.** Correctness is rate-independent
   (perfect here); the throughput story comes from PSMark on the Pis, not this harness. Log
   achieved vs configured msg/s per run (845 vs ~1,136 here).
3. **Op topics → fixed everywhere** (v2 generator + legacy set1-5 + dap_observed + phase_a +
   templates): `$OSYS`→`$OP_SYS`, `ORS`→`OP_REQ`, `ONP`→`OP_NOTIF`, confirmed against `defs.h`.
   (`or_topic_name`/`on_topic_name` = "OR"/"ON" left as-is — no broker constant, harmless.)

**Next:** Gate OP (confirm a single pinned publisher issues all periodic ops across all ticks),
then stop and report before any iii/iv/v sweep.
