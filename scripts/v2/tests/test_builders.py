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
