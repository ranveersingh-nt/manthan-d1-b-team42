"""Load and validate the fraud rules config (src/processor/config/rules.yml).

Java analogy: this module is like a small ConfigLoader class with one static method,
load_rules(), that reads a YAML file into a Map<String, Map<String, Object>> and throws
IllegalArgumentException (Python: ValueError) if the config is invalid.
"""

import os

import yaml

# Like `static final Set<String>` constants in Java.
REQUIRED_RULES = ("velocity", "impossible_travel", "amount_spike", "merchant_blacklist")
VALID_SEVERITIES = {"HIGH", "MEDIUM", "LOW"}
NON_THRESHOLD_KEYS = {"enabled", "severity"}


def load_rules(path=None):
    """Return the rules as a dict: {rule_name: {setting: value}}.

    `path=None` is a default argument (Java would need an overloaded method).
    Falls back to the RULES_CONFIG_PATH env var, like System.getenv() with a default.
    """
    path = path or os.getenv("RULES_CONFIG_PATH", "src/processor/config/rules.yml")

    # `with` closes the file automatically, like Java's try-with-resources.
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)  # safe_load: plain data only, no arbitrary objects

    rules = config.get("rules") if isinstance(config, dict) else None
    if not isinstance(rules, dict):
        raise ValueError(f"{path}: missing top-level 'rules' section")

    for name in REQUIRED_RULES:
        rule = rules.get(name)
        if not isinstance(rule, dict):
            raise ValueError(f"{path}: rule '{name}' is missing")

        # isinstance() is Python's instanceof.
        if not isinstance(rule.get("enabled"), bool):
            raise ValueError(f"{path}: rule '{name}' needs 'enabled: true|false'")

        if rule.get("severity") not in VALID_SEVERITIES:
            raise ValueError(
                f"{path}: rule '{name}' needs 'severity' to be one of {sorted(VALID_SEVERITIES)}, "
                f"got {rule.get('severity')!r}"
            )

        # Every other key is a threshold and must be a positive number.
        # bool is a subclass of int in Python, so it is excluded explicitly.
        for key, value in rule.items():
            if key in NON_THRESHOLD_KEYS:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                raise ValueError(
                    f"{path}: rule '{name}' threshold '{key}' must be a number > 0, got {value!r}"
                )

    return rules
