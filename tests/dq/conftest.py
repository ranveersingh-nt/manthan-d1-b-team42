"""Shared fixtures for DQ tests. Uses a local SparkSession; no Kafka or Postgres needed."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from typing import Any, Callable

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.dq.config import DqConfig, load_dq_config

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("timestamp", TimestampType(), False),
    ]
)

VALID_EVENT: dict[str, Any] = {
    "event_id": "3f2b8c1e-9d4a-4e6b-8f1a-2c3d4e5f6a7b",
    "card_token": "tok_9f86d081884c7d65",
    "merchant_id": "m_000123",
    "merchant_category": "5411",
    "amount": 42.50,
    "currency": "USD",
    "country": "US",
    "channel": "pos",
    "event_ts": "2026-11-20T10:15:30.123Z",
    "status": "approved",
    "schema_version": 1,
}


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamguard-dq-tests")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def dq_config() -> DqConfig:
    return load_dq_config("config/dq_rules.yaml")


@pytest.fixture
def valid_event() -> Callable[..., dict[str, Any]]:
    """Return a fresh valid event; keyword args override fields, value `...` removes a field."""

    def _make(**overrides: Any) -> dict[str, Any]:
        event = copy.deepcopy(VALID_EVENT)
        for k, v in overrides.items():
            if v is ...:
                event.pop(k, None)
            else:
                event[k] = v
        return event

    return _make


@pytest.fixture
def make_raw(spark: SparkSession) -> Callable[[list[Any]], DataFrame]:
    """Build a DataFrame shaped like the Kafka source.

    Each item may be a dict (JSON-encoded), a str (UTF-8 encoded), bytes (as-is) or None.
    Offsets are 0..n-1 on partition 0 of payments.raw.
    """

    # Rows are built as a SQL VALUES list (a JVM-side local relation) instead of
    # spark.createDataFrame, which would start a slow Python worker for every query on Windows.
    def _make(values: list[Any]) -> DataFrame:
        rows = []
        for i, v in enumerate(values):
            if isinstance(v, dict):
                payload = json.dumps(v).encode("utf-8")
            elif isinstance(v, str):
                payload = v.encode("utf-8")
            else:
                payload = v
            value_sql = "CAST(NULL AS BINARY)" if payload is None else f"X'{payload.hex()}'"
            rows.append(
                f"(X'{b'tok_key'.hex()}', {value_sql}, 'payments.raw', 0, CAST({i} AS BIGINT), "
                "TIMESTAMP'2026-11-20 10:15:31')"
            )
        return spark.sql(
            "SELECT * FROM VALUES " + ", ".join(rows) + " AS t(key, value, topic, partition, offset, timestamp)"
        )

    return _make
