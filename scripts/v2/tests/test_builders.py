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
