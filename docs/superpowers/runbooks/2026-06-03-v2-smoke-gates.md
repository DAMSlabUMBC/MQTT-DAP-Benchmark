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

---

## Sets i & ii sweep (one rep each, 2026-06-03, Paladin loopback)

Freshness verified by log timestamps (NOT exit codes): per-device RECV↔PUBLISH join + analyze FAR/FRR.

| Config | span | join (median / min) | FAR | FRR | valid / invalid | msg/s | verdict |
|---|---|---|---|---|---|---|---|
| set1_static_1p (rerun) | 180s | 100% / 100% | 0 | 0 | 153516 / 0 | 853 | ✅ (1st try stopped @38s, not reproducible) |
| set1_static_10p | 180s | 100% / 100% | 0 | 0 | 147980 / 0 | 822 | ✅ |
| set1_static_100p | 180s | 100% / 100% | 0 | 0 | 130788 / 0 | 727 | ✅ |
| set2_dynamic_mp_10p | 180s | 100% / 100% | 0 | 0 | 144288 / 0 | 802 | ✅ |
| set2_dynamic_sp_10p | 180s | 116.7% / 0% (dev17) | 0 | 0 | 141466 / 2 | 786 | ✅ (churn) |
| set2_dynamic_both_10p | 180s | 131.4% / 0% (dev17) | 0 | 0.0001 | 213108 / 18 | 1186 | ✅ (churn) |
| set2_dynamic_mp_100p | 180s | 100% / 100% | 0 | 0 | 142476 / 0 | 792 | ✅ |
| set2_dynamic_sp_100p | 180s | 100% / 0% (dev20) | 0 | 0.0001 | 126494 / 0 | 703 | ✅ (churn) |
| set2_dynamic_both_100p | 180s | 139.3% / 0% (dev20) | 0 | 0 | 364239 / 4 | 2027 | ✅ (churn) |

**Verdict:** 8/9 valid full-180s runs. Static (i) 10p/100p and dynamic-MP (ii) are clean
(join 100%, FAR=FRR=0). Dynamic-SP/both show join>100% (over-delivery: multiple subscribers
transiently share a churned purpose) and per-device 0% gaps (some purposes momentarily have no
subscriber) — the expected signature of SP purpose churn, with FAR=0 and FRR≈0.

**set1_static_1p — PARTIAL (rerun needed):** publishing stopped at 38s though the process ran the
full 180s and exited rc=0 (broker monitor collected 165 samples). Under the 40-publishers→1-subscriber
fan-in the broker dropped the publishers with a non-zero reason code; `TestExecutor._on_disconnect`
only handles `reason_code==0`, so it neither logged the drop nor cleared `is_connected` — publishes
silently failed while the loop kept running. Latent harness gap (out of current scope); rerun the
1p cell. Confirms why exit codes are not trusted.

**Dynamic-SP over-delivery verified as genuine fan-out (not a scoring artifact):** for
set2_dynamic_sp_10p, reconstructed each subscriber's active SP from its `SUBSCRIBE` re-subscribe
timeline. Worked example: msg dev31/corr=501 (publisher MP=p1) delivered to subscribers p1, p5, p7 —
all with active SP=p1 (the 25% subset {p1,p5,p7} had cycled onto p1 together). Full scan: across
50,430 multi-delivered (receiver,message) pairs, only 2 receivers had a non-matching SP, and both
landed 4–6 ms after that subscriber's SP re-subscribe (an in-flight message crossing the
re-subscription boundary) — exactly the analyzer's `invalid=2`. Conclusion: >100% join = the subset
legitimately sharing a purpose; FAR=0 is real; the 2 boundary invalids are a sub-ms race at the
SP-change instant, not over-delivery to non-matching subscribers.

**set1_static_1p rerun:** full 180s, PUB=RECV=153516, join 100%/100%, FAR=FRR=0. The first attempt's
38s stop (silent 40→1 fan-in disconnect) was not reproducible. The 1p cell counts as valid.

---

## Operations re-enabled + verified (scope A+b, 2026-06-03)

**Root cause was upstream of the op_info mismatch:** `metadata_operation_handling` was hardcoded
false — its enabling directive `use_metadata_operation_support` was removed in broker commit
`e4ad561b` ("Config updates"), gating the entire op dispatch + `dr__record_recipient` off. Fix:
- broker `conf.c`: restored the `use_metadata_operation_support` directive (commit on branch
  `dap-reenable-operations`); `handle_publish.c`: `op_info = op_topic_filters` alias.
- harness `ClientInterface.py`: send `DAP-OpTFs`/`DAP-OpPFs` (="*") instead of the ignored `DAP-OpInfo`.
- `mosquitto-unified.conf`: `use_metadata_operation_support true`.

**ASan/UBSan verify** (Dockerfile.asan image, `--privileged` to dodge the kernel-6.8 ASLR-entropy
ASan-init crash — not a code issue): full set3 lifecycle (startup + 180s + ops + teardown) =
**0 ASan/UBSan reports**. All ops completed 100%.

**Production rebuild + teardown check:** unified broker rebuilt from the branch, starts clean (op
directive parses, no unknown-variable error), survived the run (Running, Exit 0, 1883 open, no
segfault).

**Production set3 per-operation completion (the official numbers):**

| Op | Issued | Completion |
|---|---|---|
| AUDIT | 17 | **100%** |
| DELETE | 17 | **100%** |
| HISTORY | 17 | **100%** |
| RESTRICT | 17 | **100%** |
| UPDATE | 17 | **100%** |
| REGISTER-INFO | 10 | n/a (fire-and-forget; no response by design) |

Overall completion 1.0, coverage 0.80, leakage 0.0. Op issuer pinned to one publisher (dev25).
Data path: FAR 0, FRR 0.0010, 154,887 valid / 0 invalid.

**AUDIT did NOT need a separate fix** — contrary to the (a)/(b) prediction. AUDIT's path depends on
REGISTER-INFO (`ri__register_info`), which was itself gated by the disabled flag; re-enabling the
flag fixed AUDIT too. All five core ops complete.

**To flag before any iii/iv/v sweep:** (1) coverage = 0.80 (not 1.0), deterministic across ASan and
production runs — worth understanding (per-op data-coverage semantics) though completion is 100%.
(2) set3 data FRR = 0.0010 (tiny; earlier ops-free static runs were 0) — likely op/data contention
or a startup-window miss. Neither blocks, but both warrant a look before committing to the full sweep.

**Follow-up logged (not done):** the dead shadowed AUDIT(731)/HISTORY(767) branches in
handle_publish.c should be removed so those ops use the dr__ paths — larger broker change, deferred
to Section 6 verification per the scope decision.
