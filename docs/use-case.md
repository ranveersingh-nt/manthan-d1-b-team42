# Track B · Use Case: StreamGuard – Real-Time Payment Fraud Signals

**Problem statement.** A payments company reviews suspicious card transactions the next morning from a batch report, so fraud is caught hours too late. Build a streaming pipeline that ingests payment events, applies fraud rules in near real time, writes alerts and enriched data to a store, and gives analysts a live view and a feedback loop.

**Data consumers.** Fraud analyst, risk manager, platform on-call engineer.

**Sources (synthetic).** A producer written by the team with Claude emits payment events to Kafka at about 200 events per second, with bursts up to 1,000 per second. Each event has an event ID, card token, merchant ID, merchant category, amount, currency, country, channel, event time and status. Use tokens only. No real or realistic card numbers anywhere.

## Must-have scope

1. A Kafka topic for raw events, a topic for alerts and a dead-letter topic for malformed events.
2. A stream processor (Spark Structured Streaming or an equivalent consumer) that validates, deduplicates by event ID and handles out-of-order events with a 2-minute watermark.
3. At least four fraud rules: velocity (more than 5 transactions per minute per card), impossible travel (two countries within 30 minutes), amount spike (more than 5 times the card's 30-day average), and merchant blacklist.
4. Enriched transactions, one-minute aggregates and alerts written to PostgreSQL with idempotent upserts, so replays do not create duplicate alerts.
5. Rules stored as configuration (a table or YAML file) with thresholds that can be changed without code edits.
6. A feedback table where an analyst marks an alert as true or false positive, with a daily precision report per rule.
7. A Superset dashboard with alerts per minute, top triggered rules, alert backlog and a pipeline-health tab.

## Data quality rules

Amount positive and below a ceiling. Currency in an allowed list. Event time not more than 24 hours in the future. Failures go to the dead-letter topic with a reason.

## Phase-by-phase expectations

| Phase | Expected | Claude |
|-------|----------|--------|
| Architecture | Streaming architecture diagram, topic design (keys, partitions), state and watermark choices, ADR on delivery guarantees | Explain trade-offs of at-least-once with idempotent writes versus exactly-once, then decide |
| Insight layer (UI) | One live dashboard with defined alert metrics and a pipeline-health tab | Proposes the metric set; the team verifies counts against a known producer run |
| Development | Producer, processor, rule engine, sinks | Plan-first prompts, CLAUDE.md with message-schema rules, small verified commits |
| Testing | Unit tests per rule using crafted event sequences, a replay test (same input twice yields same alerts), malformed-event tests, a late-event test | Generates event sequences that should and should not trigger each rule; the team adds boundary values |
| Deployment | Docker Compose with broker, processor, database and Superset; CI with tests; one-command start | Writes Compose and CI; team proves a clean start |
| Support (SRE) | Metrics for consumer lag, processing latency, dead-letter rate and alerts per minute, an alert on lag, a replay runbook | Drafts the runbook; the team practises an offset reset and replay once |
