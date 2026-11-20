import copy

import pytest
import yaml

from src.dq.config import DqConfigError, load_dq_config, parse_dq_config


@pytest.fixture
def raw_config():
    with open("config/dq_rules.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def test_dq_config_loads_repo_yaml():
    cfg = load_dq_config("config/dq_rules.yaml")
    assert cfg.pre_dq.field("amount").type == "number"
    assert cfg.pre_dq.field("card_token").pattern
    assert 1 in cfg.pre_dq.supported_schema_versions


def test_dq_config_reads_path_from_env(monkeypatch):
    monkeypatch.setenv("DQ_CONFIG_PATH", "config/dq_rules.yaml")
    assert load_dq_config().version == 1


def test_dq_config_rejects_missing_file():
    with pytest.raises(DqConfigError, match="not found"):
        load_dq_config("config/does_not_exist.yaml")


def test_dq_config_rejects_unknown_field_type(raw_config):
    bad = copy.deepcopy(raw_config)
    bad["pre_dq"]["fields"]["amount"]["type"] = "money"
    with pytest.raises(DqConfigError, match="type must be one of"):
        parse_dq_config(bad)


def test_dq_config_rejects_invalid_regex(raw_config):
    bad = copy.deepcopy(raw_config)
    bad["pre_dq"]["fields"]["card_token"]["pattern"] = "^tok_[0-9"
    with pytest.raises(DqConfigError, match="not a valid regex"):
        parse_dq_config(bad)


def test_dq_config_rejects_default_version_not_supported(raw_config):
    bad = copy.deepcopy(raw_config)
    bad["pre_dq"]["default_schema_version"] = 9
    with pytest.raises(DqConfigError, match="default_schema_version"):
        parse_dq_config(bad)


def test_dq_config_rejects_non_bool_required(raw_config):
    bad = copy.deepcopy(raw_config)
    bad["pre_dq"]["fields"]["amount"]["required"] = "yes"
    with pytest.raises(DqConfigError, match="required must be true or false"):
        parse_dq_config(bad)
