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
    assert len(changes) == 17 * 2
    devsets = {tuple(e["devices"]) for e in changes}
    assert len(devsets) == 2


def test_set5_connectivity_has_disconnect_and_ops():
    cfg = gen.assemble_config(5, "connectivity", 10, None, True, True)
    assert len(_events_of_type(cfg, "disconnect")) == 1
    assert len(_events_of_type(cfg, "reconnect")) == 1
    assert cfg["test"]["op_send_rate"] == 10000


def test_matrix_is_20_configs():
    matrix = gen.build_matrix()
    assert len(matrix) == 20
    names = sorted(fn for _, fn, _ in matrix)
    assert any(n.startswith("v2_set1_static_1p") for n in names)
    assert any(n.startswith("v2_set1_static_100p") for n in names)
    assert sum(1 for n in names if n.startswith("v2_set2_")) == 6
    assert sum(1 for n in names if n.startswith("v2_set4_")) == 6
    assert sum(1 for n in names if n.startswith("v2_set5_")) == 2
    assert not any(n.startswith("v2_set5_") and "_1p_" in n for n in names)


def test_matrix_filenames_unique():
    matrix = gen.build_matrix()
    fns = [fn for _, fn, _ in matrix]
    assert len(set(fns)) == len(fns)
