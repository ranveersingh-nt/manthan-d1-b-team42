-- Pre-DQ rejected events: one row per rejected Kafka message (owner: Atindra).
-- Re-runnable. Keyed on the source offset because a malformed event may have no event_id.
-- Numbered 100+ to keep DQ objects apart from the core schema files (001_...).

CREATE TABLE IF NOT EXISTS dq_rejected_events (
    src_topic          TEXT        NOT NULL,
    src_partition      INTEGER     NOT NULL,
    src_offset         BIGINT      NOT NULL,
    event_id           TEXT,
    card_token         TEXT,
    dq_stage           TEXT        NOT NULL,
    dq_rule            TEXT        NOT NULL,
    dq_reason          TEXT        NOT NULL,
    dq_failures        JSONB       NOT NULL,
    raw_payload        TEXT,
    src_kafka_ts       TIMESTAMPTZ,
    dq_ts              TIMESTAMPTZ NOT NULL,
    dq_config_version  INTEGER     NOT NULL,
    first_rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_dq_rejected_events PRIMARY KEY (src_topic, src_partition, src_offset)
);

CREATE INDEX IF NOT EXISTS ix_dq_rejected_events_dq_ts   ON dq_rejected_events (dq_ts);
CREATE INDEX IF NOT EXISTS ix_dq_rejected_events_rule_ts ON dq_rejected_events (dq_rule, dq_ts);
CREATE INDEX IF NOT EXISTS ix_dq_rejected_events_event   ON dq_rejected_events (event_id);
