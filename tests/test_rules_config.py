from pathlib import Path

import pytest

from src.processor.rules_config import REQUIRED_RULES, load_rules

REPO_ROOT = Path(__file__).resolve().parents[1]
RULES_YML = REPO_ROOT / "src" / "processor" / "config" / "rules.yml"

VALID_YAML = """
version: 1
rules:
  velocity: {enabled: true, severity: HIGH, max_txn: 5, window_minutes: 1}
  impossible_travel: {enabled: true, severity: HIGH, min_distinct_countries: 2, window_minutes: 30}
  amount_spike: {enabled: true, severity: MEDIUM, multiplier: 5, baseline_days: 30}
  merchant_blacklist: {enabled: true, severity: HIGH}
"""


def write_yaml(tmp_path, text):
    path = tmp_path / "rules.yml"
    path.write_text(text, encoding="utf-8")
    return path


def test_rules_yml_loads_all_four_rules():
    rules = load_rules(RULES_YML)

    assert set(REQUIRED_RULES) <= set(rules)
    assert rules["velocity"]["max_txn"] == 5
    assert rules["amount_spike"]["severity"] == "MEDIUM"


def test_negative_threshold_is_rejected(tmp_path):
    path = write_yaml(tmp_path, VALID_YAML.replace("max_txn: 5", "max_txn: -5"))

    with pytest.raises(ValueError, match="max_txn"):
        load_rules(path)


def test_missing_severity_is_rejected(tmp_path):
    path = write_yaml(tmp_path, VALID_YAML.replace("severity: MEDIUM, ", ""))

    with pytest.raises(ValueError, match="amount_spike.*severity"):
        load_rules(path)
