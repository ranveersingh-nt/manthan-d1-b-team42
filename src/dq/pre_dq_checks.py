"""Pre-DQ checks: fetch raw payment events from Kafka and apply strict structural checks.

Each event is checked on its own (Option A) and gets:
    dq_status    ACCEPTED or REJECTED
    dq_rule      first rule that failed (NULL when accepted)
    dq_reason    human-readable reason for dq_rule
    dq_failures  every failed rule as (rule, field, reason)

Flow: Kafka TOPIC_RAW -> checks -> per micro-batch, in one Postgres transaction:
    ACCEPTED -> payment_events_validated    (idempotent insert, keyed on event_id / source offset)
    REJECTED -> payment_events_invalidated  (idempotent insert, keyed on source offset, with reason)
Rejected events are also logged with their reasons. Routing rejects to the DLQ comes later.

Run:
    python -m src.dq.pre_dq_checks
Env (from .env): KAFKA_BOOTSTRAP, TOPIC_RAW (default raW_payments), CHECKPOINT_DIR, PG_URL, PG_USER, PG_PASSWORD,
    optional KAFKA_STARTING_OFFSETS (default latest), KAFKA_MAX_OFFSETS_PER_TRIGGER (default 5000),
    SPARK_LOCAL_DIR (default ./data/spark-tmp, keeps Spark temp files on the repo drive).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Callable, Optional

from dotenv import load_dotenv
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

# --------------------------------------------------------------------------- contract
# Mirrors docs/data-contract.md. Field types: string | number | integer | timestamp.

FIELDS: dict[str, dict] = {
    "event_id": {
        "type": "string",
        "required": True,
        "pattern": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    },
    "card_token": {"type": "string", "required": True, "pattern": r"^tok_[0-9a-f]{16,32}$"},
    "merchant_id": {"type": "string", "required": True},
    "merchant_category": {"type": "string", "required": True},
    "amount": {"type": "number", "required": True},
    "currency": {"type": "string", "required": True},
    "country": {"type": "string", "required": True},
    "channel": {"type": "string", "required": True},
    "event_ts": {"type": "timestamp", "required": True},
    "status": {"type": "string", "required": True},
    "schema_version": {"type": "integer", "required": False},
}

SUPPORTED_SCHEMA_VERSIONS = (1,)
DEFAULT_SCHEMA_VERSION = 1
AMOUNT_DECIMAL_TYPE = "decimal(18,2)"
TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$"  # UTC ISO-8601 only
PAN_PATTERNS = (  # card-number-like digits: 13-19 in a row, or 4-4-4-4 / 4-6-5 groups
    r"(?<![0-9A-Za-z_])\d{13,19}(?![0-9A-Za-z_])",
    r"(?<![0-9A-Za-z_])\d{4}[ -]\d{4}[ -]\d{4}[ -]\d{4}(?![0-9A-Za-z_])",
    r"(?<![0-9A-Za-z_])\d{4}[ -]\d{6}[ -]\d{5}(?![0-9A-Za-z_])",
)

NUMBER_TYPE_REGEX = r"^(TINYINT|SMALLINT|INT|BIGINT|FLOAT|DOUBLE|DECIMAL\(\d+,\d+\))$"
INTEGER_TYPE_REGEX = r"^(TINYINT|SMALLINT|INT|BIGINT)$"
BLANK_REGEX = r"^\s*$"  # trim() only strips spaces; this also catches tabs/newlines


# --------------------------------------------------------------------------- helpers


def _c(name: str) -> Column:
    return F.col(f"`{name}`")


def _failure(rule: str, reason: str, field: Optional[str] = None) -> Column:
    return F.struct(
        F.lit(rule).alias("rule"),
        F.lit(field).cast("string").alias("field"),
        F.lit(reason).alias("reason"),
    )


def _fail_when(condition: Column, rule: str, reason: str, field: Optional[str] = None) -> Column:
    """Failure struct when condition is true, else NULL (a NULL condition counts as false)."""
    return F.when(F.coalesce(condition, F.lit(False)), _failure(rule, reason, field))


def _typed_value(name: str, ftype: str) -> Column:
    if ftype == "string":
        return _c(f"_s_{name}")
    if ftype == "number":
        return F.try_variant_get(_c("_parsed"), f"$.{name}", AMOUNT_DECIMAL_TYPE)
    if ftype == "integer":
        return F.try_variant_get(_c("_parsed"), f"$.{name}", "int")
    if ftype == "timestamp":
        return _c(f"_s_{name}").try_cast("timestamp")
    raise ValueError(f"unknown field type: {ftype}")


def _type_ok(name: str, ftype: str) -> Column:
    t, v = _c(f"_t_{name}"), _c(f"_v_{name}")
    if ftype == "string":
        return t == "STRING"
    if ftype == "number":
        return t.rlike(NUMBER_TYPE_REGEX) & v.isNotNull()
    if ftype == "integer":
        return t.rlike(INTEGER_TYPE_REGEX) & v.isNotNull()
    return (t == "STRING") & _c(f"_s_{name}").rlike(TIMESTAMP_PATTERN) & v.isNotNull()


def prepare(raw_df: DataFrame) -> DataFrame:
    """Parse once and extract every field once, as helper columns the rules read.

    _value_str/_value_text  raw value as text (_text has invalid UTF-8 bytes replaced)
    _utf8_ok, _is_empty     encoding / emptiness flags
    _parsed, _is_object     VARIANT from try_parse_json, and whether it is a JSON object
    _struct_ok              all message-level checks passed (field rules run only then)
    _f_/_t_/_s_<field>      field VARIANT (NULL if absent), its JSON type, its string value
    _v_<field>              field cast to its contract type (NULL if not castable)
    _blank_<field>          JSON null or blank string
    _ok_<field>             has the contract type
    """
    value_str = F.col("value").cast("string")
    df = raw_df.withColumns(
        {
            "_value_str": value_str,
            "_value_text": F.make_valid_utf8(value_str),
            "_utf8_ok": F.coalesce(F.is_valid_utf8(value_str), F.lit(False)),
        }
    )
    df = df.withColumns({"_is_empty": F.col("value").isNull() | _c("_value_text").rlike(BLANK_REGEX)})
    df = df.withColumns({"_parsed": F.when(~_c("_is_empty") & _c("_utf8_ok"), F.try_parse_json(_c("_value_str")))})
    df = df.withColumns(
        {"_is_object": F.coalesce(F.schema_of_variant(_c("_parsed")).startswith("OBJECT"), F.lit(False))}
    )
    df = df.withColumns(
        {
            "_struct_ok": ~_c("_is_empty") & _c("_utf8_ok") & _c("_parsed").isNotNull() & _c("_is_object"),
            **{
                f"_f_{n}": F.when(_c("_is_object"), F.try_variant_get(_c("_parsed"), f"$.{n}", "variant"))
                for n in FIELDS
            },
        }
    )
    df = df.withColumns({f"_t_{n}": F.schema_of_variant(_c(f"_f_{n}")) for n in FIELDS})
    df = df.withColumns(
        {
            f"_s_{n}": F.when(_c(f"_t_{n}") == "STRING", F.try_variant_get(_c("_parsed"), f"$.{n}", "string"))
            for n in FIELDS
        }
    )
    df = df.withColumns(
        {
            **{f"_v_{n}": _typed_value(n, spec["type"]) for n, spec in FIELDS.items()},
            **{
                f"_blank_{n}": (_c(f"_t_{n}") == "VOID")
                | ((_c(f"_t_{n}") == "STRING") & _c(f"_s_{n}").rlike(BLANK_REGEX))
                for n in FIELDS
            },
        }
    )
    return df.withColumns({f"_ok_{n}": F.coalesce(_type_ok(n, s["type"]), F.lit(False)) for n, s in FIELDS.items()})


# --------------------------------------------------------------------------- message rules
# Fatal rules run in this order and at most one fires; if one does, field rules are skipped.


def rule_empty_message() -> Column:
    return _fail_when(_c("_is_empty"), "empty_message", "message value is null, empty or whitespace only")


def rule_bad_encoding() -> Column:
    return _fail_when(~_c("_is_empty") & ~_c("_utf8_ok"), "bad_encoding", "message value is not valid UTF-8")


def rule_malformed_json() -> Column:
    return _fail_when(
        ~_c("_is_empty") & _c("_utf8_ok") & _c("_parsed").isNull(),
        "malformed_json",
        "message value is not valid JSON",
    )


def rule_not_json_object() -> Column:
    return _fail_when(
        _c("_parsed").isNotNull() & ~_c("_is_object"),
        "not_json_object",
        "message value is valid JSON but not a JSON object",
    )


def rule_pan_detected() -> Column:
    hit = F.lit(False)
    for pattern in PAN_PATTERNS:
        hit = hit | F.coalesce(_c("_value_text").rlike(pattern), F.lit(False))
    return _fail_when(
        ~_c("_is_empty") & hit,
        "pan_detected",
        "payload contains a card-number-like digit sequence; only card tokens are allowed",
    )


def rule_unsupported_schema_version() -> Column:
    supported = ", ".join(str(v) for v in SUPPORTED_SCHEMA_VERSIONS)
    return _fail_when(
        _c("_struct_ok") & _c("_ok_schema_version") & ~_c("_v_schema_version").isin(*SUPPORTED_SCHEMA_VERSIONS),
        "unsupported_schema_version",
        f"schema_version is not supported (supported: {supported})",
        "schema_version",
    )


# --------------------------------------------------------------------------- field rules
# Per field at most one fires: missing -> null/blank -> wrong type -> wrong format.

_TYPE_REASONS = {
    "string": "field '{name}' must be a JSON string",
    "number": "field '{name}' must be a JSON number that fits " + AMOUNT_DECIMAL_TYPE,
    "integer": "field '{name}' must be a JSON integer",
    "timestamp": "field '{name}' must be a UTC ISO-8601 string like 2026-11-20T10:15:30Z with a real date and time",
}


def rule_missing_field(name: str, spec: dict) -> Optional[Column]:
    if not spec["required"]:
        return None
    return _fail_when(_c(f"_f_{name}").isNull(), "missing_field", f"required field '{name}' is missing", name)


def rule_null_or_blank_field(name: str, spec: dict) -> Optional[Column]:
    return _fail_when(
        _c(f"_f_{name}").isNotNull() & _c(f"_blank_{name}"),
        "null_or_blank_field",
        f"field '{name}' is null or blank",
        name,
    )


def rule_invalid_type(name: str, spec: dict) -> Optional[Column]:
    return _fail_when(
        _c(f"_f_{name}").isNotNull() & ~_c(f"_blank_{name}") & ~_c(f"_ok_{name}"),
        "invalid_type",
        _TYPE_REASONS[spec["type"]].format(name=name),
        name,
    )


def rule_invalid_format(name: str, spec: dict) -> Optional[Column]:
    pattern = spec.get("pattern")
    if not pattern:
        return None
    return _fail_when(
        _c(f"_ok_{name}") & ~_c(f"_blank_{name}") & ~_c(f"_s_{name}").rlike(pattern),
        "invalid_format",
        f"field '{name}' does not match the required format",
        name,
    )


# --------------------------------------------------------------------------- registry

FATAL_MESSAGE_RULES: dict[str, Callable[[], Column]] = {
    "empty_message": rule_empty_message,
    "bad_encoding": rule_bad_encoding,
    "malformed_json": rule_malformed_json,
    "not_json_object": rule_not_json_object,
}
FIELD_RULES: dict[str, Callable[[str, dict], Optional[Column]]] = {
    "missing_field": rule_missing_field,
    "null_or_blank_field": rule_null_or_blank_field,
    "invalid_type": rule_invalid_type,
    "invalid_format": rule_invalid_format,
}
OTHER_MESSAGE_RULES: dict[str, Callable[[], Column]] = {
    "pan_detected": rule_pan_detected,
    "unsupported_schema_version": rule_unsupported_schema_version,
}


def apply_pre_dq_checks(raw_df: DataFrame) -> DataFrame:
    """Run every rule on Kafka rows (key, value, topic, partition, offset, timestamp).

    Returns one row per input row with source metadata, the typed event fields and the
    dq_status / dq_rule / dq_reason / dq_failures columns. Nothing is dropped.
    """
    df = prepare(raw_df)
    rule_cols = [fn() for fn in FATAL_MESSAGE_RULES.values()]
    for name, spec in FIELDS.items():
        for fn in FIELD_RULES.values():
            col = fn(name, spec)
            if col is not None:
                rule_cols.append(F.when(_c("_struct_ok"), col))
    rule_cols += [fn() for fn in OTHER_MESSAGE_RULES.values()]

    df = df.withColumn("dq_failures", F.filter(F.array(*rule_cols), lambda x: x.isNotNull()))
    return df.select(
        F.col("topic").alias("src_topic"),
        F.col("partition").alias("src_partition"),
        F.col("offset").alias("src_offset"),
        F.col("timestamp").alias("src_kafka_ts"),
        F.col("key").alias("kafka_key"),
        *[_c(f"_v_{n}").alias(n) for n in FIELDS if n != "schema_version"],
        # schema_version is optional in the contract; absent means the default version.
        F.coalesce(_c("_v_schema_version"), F.lit(DEFAULT_SCHEMA_VERSION)).alias("schema_version"),
        F.when(F.size("dq_failures") == 0, F.lit("ACCEPTED")).otherwise(F.lit("REJECTED")).alias("dq_status"),
        # get() is ANSI-safe: NULL for accepted events (empty array) instead of an error.
        F.get("dq_failures", 0)["rule"].alias("dq_rule"),
        F.get("dq_failures", 0)["reason"].alias("dq_reason"),
        F.col("dq_failures"),
        _c("_value_text").alias("raw_payload"),
    )


# --------------------------------------------------------------------------- fetch from Kafka


def build_spark() -> SparkSession:
    import pyspark

    # Default: download the Kafka connector at startup (laptop runs). Docker images that already
    # ship the jars set SPARK_JARS_PACKAGES="" so no runtime download is attempted.
    kafka_pkg = os.getenv("SPARK_JARS_PACKAGES", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}")
    local_dir = os.getenv("SPARK_LOCAL_DIR", "./data/spark-tmp")
    builder = SparkSession.builder.appName("streamguard-pre-dq-checks")
    if kafka_pkg:
        builder = builder.config("spark.jars.packages", kafka_pkg).config(
            "spark.jars.ivy", "./data/ivy"  # keep downloaded jars on the repo drive
        )
    return (
        builder.config("spark.local.dir", local_dir)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


DEFAULT_RAW_TOPIC = "raw_payments"


def read_raw_events(spark: SparkSession) -> DataFrame:
    """Stream raw events from the Kafka topic named in .env (TOPIC_RAW). Bad messages never fail the read."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
        .option("subscribe", os.environ["TOPIC_RAW"])
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
        .option("maxOffsetsPerTrigger", os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000"))
        .option("failOnDataLoss", "false")
        .load()
    )


# --------------------------------------------------------------------------- store accepted events

VALIDATED_TABLE = "payment_events_validated"
VALIDATED_COLUMNS = (
    "event_id",
    "card_token",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "country",
    "channel",
    "event_ts",
    "status",
    "schema_version",
    "src_topic",
    "src_partition",
    "src_offset",
    "src_kafka_ts",
)
# DO NOTHING without a conflict target skips a row that clashes on ANY unique key:
# event_id (duplicate event) or (src_topic, src_partition, src_offset) (replayed message).
# The first stored copy wins, so replays never create duplicates.
INSERT_VALIDATED_SQL = (
    f"INSERT INTO {VALIDATED_TABLE} ({', '.join(VALIDATED_COLUMNS)}) VALUES %s "
    "ON CONFLICT DO NOTHING RETURNING event_id"
)
INVALIDATED_TABLE = "payment_events_invalidated"
INVALIDATED_COLUMNS = (
    "src_topic",
    "src_partition",
    "src_offset",
    "src_kafka_ts",
    "event_id",
    "card_token",
    "dq_rule",
    "reason",
    "dq_failures",
    "raw_payload",
)
# Keyed on the Kafka source offset: a malformed event may have no event_id.
INSERT_INVALIDATED_SQL = (
    f"INSERT INTO {INVALIDATED_TABLE} ({', '.join(INVALIDATED_COLUMNS)}) VALUES %s "
    "ON CONFLICT DO NOTHING RETURNING src_offset"
)
INVALIDATED_TEMPLATE = "(" + ", ".join("%s::jsonb" if c == "dq_failures" else "%s" for c in INVALIDATED_COLUMNS) + ")"

UTC_ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"  # e.g. 2026-11-20T10:15:30.123000Z
INSERT_PAGE_SIZE = 1000
PAN_MASK = "[REDACTED_PAN]"
MAX_PAYLOAD_CHARS = 8192
MAX_ID_CHARS = 128

logger = logging.getLogger("streamguard.dq.pre_dq")


def pg_connect():
    """psycopg2 connection from PG_URL (JDBC or libpq URI), PG_USER, PG_PASSWORD."""
    import psycopg2

    url = os.environ["PG_URL"]
    if url.startswith("jdbc:"):
        url = url[len("jdbc:"):]
    return psycopg2.connect(url, user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"))


def accepted_rows(checked_df: DataFrame) -> DataFrame:
    """ACCEPTED events shaped for the table, in VALIDATED_COLUMNS order.

    Timestamps are sent as UTC ISO-8601 strings (the session time zone is UTC). Collecting
    them as Python datetimes would shift them into the machine's local time zone.
    """
    return checked_df.filter(F.col("dq_status") == "ACCEPTED").select(
        *[
            F.date_format(c, UTC_ISO_FORMAT).alias(c) if c in ("event_ts", "src_kafka_ts") else F.col(c)
            for c in VALIDATED_COLUMNS
        ]
    )


def _safe_text(col: Column, max_chars: int) -> Column:
    """Make text safe to store: mask card-number-like digits, escape NUL (Postgres TEXT rejects it), cap length."""
    for pattern in PAN_PATTERNS:
        col = F.regexp_replace(col, pattern, PAN_MASK)
    col = F.regexp_replace(col, r"\x00", r"\\u0000")  # Java replacement: "\\" -> one literal backslash
    return F.substring(col, 1, max_chars)


def rejected_rows(checked_df: DataFrame) -> DataFrame:
    """REJECTED events shaped for payment_events_invalidated, in INVALIDATED_COLUMNS order.

    reason      every failure reason, joined with '; ' (dq_rule holds the first failed rule)
    dq_failures all failures as JSON [{rule, field, reason}, ...]
    event_id / card_token / raw_payload are stored as sent, after _safe_text.
    """
    return checked_df.filter(F.col("dq_status") == "REJECTED").select(
        F.col("src_topic"),
        F.col("src_partition"),
        F.col("src_offset"),
        F.date_format("src_kafka_ts", UTC_ISO_FORMAT).alias("src_kafka_ts"),
        _safe_text(F.col("event_id"), MAX_ID_CHARS).alias("event_id"),
        _safe_text(F.col("card_token"), MAX_ID_CHARS).alias("card_token"),
        F.col("dq_rule"),
        F.array_join(F.transform("dq_failures", lambda f: f["reason"]), "; ").alias("reason"),
        F.to_json("dq_failures").alias("dq_failures"),
        _safe_text(F.col("raw_payload"), MAX_PAYLOAD_CHARS).alias("raw_payload"),
    )


def insert_rows(cur, sql: str, rows, template: Optional[str] = None) -> int:
    """Insert rows (tuples in column order) in pages. Returns how many were newly stored."""
    from psycopg2.extras import execute_values

    inserted = 0
    page: list[tuple] = []
    for row in rows:
        page.append(tuple(row))
        if len(page) >= INSERT_PAGE_SIZE:
            inserted += len(execute_values(cur, sql, page, template=template, fetch=True))
            page = []
    if page:
        inserted += len(execute_values(cur, sql, page, template=template, fetch=True))
    return inserted


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch handler: ACCEPTED -> payment_events_validated, REJECTED -> payment_events_invalidated.

    Both inserts run in ONE transaction, so a batch is stored completely or not at all. If it
    fails, the error stops the query and Spark re-runs the same offsets on restart;
    ON CONFLICT DO NOTHING makes that replay safe. Each rejected event is also logged
    (offset, event_id, rule, reason; never the payload).
    """
    batch_df = batch_df.persist()
    try:
        total = batch_df.count()
        if total == 0:
            return

        accepted = accepted_rows(batch_df)
        rejected = rejected_rows(batch_df)
        accepted_count = accepted.count()
        rejected_count = total - accepted_count

        stored_valid = stored_invalid = 0
        conn = pg_connect()
        try:
            with conn.cursor() as cur:
                if accepted_count:
                    stored_valid = insert_rows(cur, INSERT_VALIDATED_SQL, accepted.toLocalIterator())
                if rejected_count:
                    stored_invalid = insert_rows(
                        cur, INSERT_INVALIDATED_SQL, rejected.toLocalIterator(), INVALIDATED_TEMPLATE
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        if rejected_count:
            for r in rejected.select("src_partition", "src_offset", "event_id", "dq_rule", "reason").toLocalIterator():
                logger.warning(json.dumps({"event": "pre_dq_reject", "batch_id": batch_id, **r.asDict()}, default=str))

        logger.info(
            json.dumps(
                {
                    "event": "pre_dq_batch",
                    "batch_id": batch_id,
                    "received": total,
                    "accepted": accepted_count,
                    "rejected": rejected_count,
                    "stored_validated": stored_valid,
                    "stored_invalidated": stored_invalid,
                    "skipped_duplicates": (accepted_count - stored_valid) + (rejected_count - stored_invalid),
                }
            )
        )
    finally:
        batch_df.unpersist()


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    checked = apply_pre_dq_checks(read_raw_events(spark))

    checkpoint = os.path.join(os.environ["CHECKPOINT_DIR"], "pre_dq_to_postgres")
    query = (
        checked.writeStream.queryName("pre_dq_to_postgres")
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
