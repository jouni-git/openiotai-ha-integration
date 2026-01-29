# OpenIOTAI Polling-Based Data Export Integration for Home Assistant

This custom Home Assistant integration exports entity state snapshots using a
deterministic polling model and publishes them to OpenIOTAI via MQTT.
REST-based export is planned for a future version.

## Why polling?

Home Assistant’s event bus is not designed to provide a deterministic,
real-time event stream for external ingest pipelines. This integration
intentionally avoids event-based subscriptions and instead polls current
entity states at fixed intervals to ensure predictable, backfill-free
data delivery.

## Key characteristics

- Polling-based (no event bus usage)
- Snapshot semantics (current entity state only)
- MQTT-based export (initial implementation)
- REST export planned
- No history, no backfill, no analytics
- Designed for external ingest and analytics systems (OpenIOTAI)

## Status

Early development / MVP.

## License

MIT
