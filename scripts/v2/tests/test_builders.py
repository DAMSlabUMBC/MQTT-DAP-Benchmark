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


from scripts.v2 import generate_v2_configs as gen


def test_publisher_definitions_have_unique_topics():
    defs = gen.build_publisher_definitions()
    assert len(defs) == 40
    topics = [d["topic"] for d in defs]
    assert topics[0] == "device/dev01"
    assert topics[-1] == "device/dev40"
    assert len(set(topics)) == 40
    assert defs[0]["pub_period_ms"] == 1000
    assert defs[0]["min_payload_bytes"] == 100
    assert defs[0]["max_payload_bytes"] == 100


def test_publisher_purpose_assignment_n10():
    insts = gen.build_publisher_instances(10)
    assert len(insts) == 40
    assert insts[0]["purpose_filter"] == "p1"
    assert insts[9]["purpose_filter"] == "p10"
    assert insts[10]["purpose_filter"] == "p1"
    used = {i["purpose_filter"] for i in insts}
    assert used == {f"p{k}" for k in range(1, 11)}


def test_publisher_purpose_assignment_n100_uses_only_40():
    insts = gen.build_publisher_instances(100)
    used = {i["purpose_filter"] for i in insts}
    assert used == {f"p{k}" for k in range(1, 41)}


def test_publisher_purpose_assignment_n1():
    insts = gen.build_publisher_instances(1)
    assert {i["purpose_filter"] for i in insts} == {"p1"}


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
    assert evs[10]["new_purpose"] == "p1"


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
