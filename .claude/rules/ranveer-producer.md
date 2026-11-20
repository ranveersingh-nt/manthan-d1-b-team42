---
paths:
  - "src/producer/**"
---
# Producer rules (owner: Ranveer)

- Every event must match the schema in `docs/data-contract.md`. Update the contract first, then the code.
- Use card tokens only (e.g. `tok_<hex>`). Never generate PAN-like numbers, even with Faker.
- Key Kafka messages by `card_token` so each card's events stay in order on one partition.
- Enable producer idempotence (`enable.idempotence=true`, `acks=all`).
- Give every event a unique `event_id` and a UTC ISO-8601 `event_ts`.
- Make the event rate, fraud-injection ratio and late-event ratio configurable through env or CLI args.
- Read the bootstrap servers and topic from `KAFKA_BOOTSTRAP` and `TOPIC_RAW`.
- Flush the producer on exit and handle delivery callbacks. Log failures without logging the payload.
