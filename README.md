# OpenIOTAI Polling-Based Data Export Integration for Home Assistant

This custom Home Assistant integration exports entity state snapshots to
OpenIOTAI using a high-frequency, deterministic polling model and publishes
only detected changes (deltas) via MQTT.

Although technically polling-based, the integration is designed to behave
functionally like an event-driven exporter while retaining the robustness
and predictability of polling.

REST-based export is planned for a future version.

## Why polling (instead of event subscriptions)?

Home Assistant’s internal event bus (`state_changed`) is optimized for
in-process automations, not for providing a deterministic, externally
consumed event stream.

This integration intentionally avoids direct event subscriptions and instead:

- polls current entity states at a fixed interval
- computes deltas locally
- publishes only when changes occur

This approach provides:

- predictable behavior
- immunity to missed events
- natural recovery after restarts
- simple reasoning about system state

With a 1-second polling interval, the system is functionally equivalent
to an event-driven exporter, with a bounded latency of ≤ 1 second.

## Design model

poll (fixed interval)
→ snapshot of current states
→ delta detection
→ publish changes via MQTT

Key properties:

- no event bus dependency
- no buffering or backfill
- no state history
- stateless export semantics

## Key characteristics

- high-frequency polling (default: 1 second, configurable at runtime)
- delta-based publishing (no data sent if nothing changed)
- snapshot semantics (current state only)
- MQTT-based export (TLS supported)
- runtime configuration updates (no integration reload)
- no history, no backfill, no local analytics
- designed for external ingest, SPC, and analytics pipelines (OpenIOTAI)

## Quasi-event behavior

Although polling-based, the integration behaves like an event-driven system:

- latency bounded by polling interval
- minimal payload sizes
- no redundant messages
- stable behavior across restarts

This makes the integration suitable for near-real-time ingest pipelines
without relying on Home Assistant’s internal event delivery guarantees.

## Intended use

- external analytics systems
- time-series databases
- SPC / anomaly detection pipelines
- deterministic data ingest
- systems where predictability is preferred over internal HA event coupling

## Status

Early development / MVP.

## License

MIT
