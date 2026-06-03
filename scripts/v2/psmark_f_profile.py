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
