"""MQTT export for OpenIOTAI integration.

Design goals:
- Keep the MQTT connection stable (no unnecessary reconnect storms).
- Never block the Home Assistant event loop.
- Paho callbacks run in a background thread -> always signal asyncio via call_soon_threadsafe.
- Allow options updates without reloading the integration.
- Support optional delta publish (only changed keys since last successful publish).

Notes about stability:
- Only ONE place is allowed to (re)connect: the runtime loop (_run()).
- publish_* never calls disconnect() directly. It only requests a reconnect.
- update_options never disconnects immediately. It only requests a reconnect.
- loop_start/loop_stop are managed centrally; we never call loop_start twice without a loop_stop.

"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple

import paho.mqtt.client as mqtt

_LOGGER = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT_SEC = 10
DEFAULT_PUBLISH_TIMEOUT_SEC = 10
DEFAULT_KEEPALIVE_SEC = 30

RECONNECT_INITIAL_SEC = 2
RECONNECT_MAX_SEC = 60


# ---------------------------------------------------------------------
# Exceptions (used by config / options flow)
# ---------------------------------------------------------------------
class CannotConnect(Exception):
    """Raised when MQTT broker cannot be reached / connection fails."""


class InvalidAuth(Exception):
    """Raised when broker rejects credentials."""


class TlsError(Exception):
    """Raised when TLS handshake / certificate validation fails."""


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _json_safe(obj: Any) -> Any:
    """Convert objects to JSON-safe values."""
    if obj is None:
        return None
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode("utf-8")
        except Exception:
            return obj.decode("latin-1", errors="replace")
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    return str(obj)


def _stable_json(obj: Any) -> str:
    """Stable JSON serialization for change detection."""
    return json.dumps(_json_safe(obj), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class _ConnConfig:
    """Connection-relevant configuration."""
    broker: str
    port: int
    use_tls: bool
    ca_cert: str | None
    username: str | None
    password: str | None
    client_id: str
    keepalive: int


# ---------------------------------------------------------------------
# Exporter
# ---------------------------------------------------------------------
class OpenIOTAIMQTTExporter:
    """Exports OpenIOTAI snapshots to MQTT (TLS supported)."""

    def __init__(
        self,
        *,
        broker: str,
        port: int,
        topic: str,
        use_tls: bool,
        ca_cert: Optional[str],
        username: Optional[str],
        password: Optional[str],
        client_id: str,
        keepalive: int = DEFAULT_KEEPALIVE_SEC,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT_SEC,
        publish_timeout: int = DEFAULT_PUBLISH_TIMEOUT_SEC,
        publish_interval: int | None = None,
        # If True, publish_snapshot() will publish ONLY changes since last successful publish.
        # If False, publish_snapshot() publishes full snapshot.
        delta_publish: bool = True,
    ) -> None:
        # Public runtime state (diagnostics)
        self.connected: bool = False
        self.last_error: Optional[str] = None
        self.last_connect_attempt: Optional[str] = None

        # Non-connection config
        self._topic = str(topic)
        self._publish_interval = publish_interval
        self._delta_publish = bool(delta_publish)

        self._connect_timeout = int(connect_timeout)
        self._publish_timeout = int(publish_timeout)

        self._conn_cfg: _ConnConfig = _ConnConfig(
            broker=str(broker),
            port=int(port),
            use_tls=bool(use_tls),
            ca_cert=str(ca_cert) if ca_cert else None,
            username=str(username) if username else None,
            password=str(password) if password else None,
            client_id=str(client_id),
            keepalive=int(keepalive),
        )

        # Internal runtime
        self._client: Optional[mqtt.Client] = None
        self._tls_configured: bool = False
        self._loop_started: bool = False

        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False

        # Paho callbacks run in Paho network thread
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._conn_event: Optional[asyncio.Event] = None
        self._conn_rc: Optional[int] = None

        # Reconnect request flag
        self._reconnect_event = asyncio.Event()

        # Delta tracking (only updated after successful publish)
        self._last_sent_map: Dict[str, str] = {}  # key -> stable-json of that key's payload

        _LOGGER.info(
            "MQTT exporter initialized (broker=%s:%s, tls=%s, topic=%s, client_id=%s, delta=%s)",
            self._conn_cfg.broker,
            self._conn_cfg.port,
            self._conn_cfg.use_tls,
            self._topic,
            self._conn_cfg.client_id,
            self._delta_publish,
        )

    # ------------------------------------------------------------------
    # Options update (called from HA event loop)
    # ------------------------------------------------------------------
    def update_options(self, options: Dict[str, Any]) -> None:
        """Apply updated options without reloading the integration.

        Rules:
        - Topic / publish_interval / delta_publish changes do NOT require reconnect.
        - Broker/port/TLS/auth changes DO require reconnect.
        - We do NOT disconnect here; we only request reconnect and let _run() handle it.
        """
        # Allow both "const-like" keys and your earlier literal keys
        def _get(*keys: str) -> Any:
            for k in keys:
                if k in options:
                    return options.get(k)
            return None

        broker = _get("mqtt_broker")
        port = _get("mqtt_port")
        topic = _get("mqtt_topic")
        use_tls = _get("mqtt_tls")
        ca_cert = _get("mqtt_ca_cert")
        username = _get("mqtt_username")
        password = _get("mqtt_password")
        publish_interval = _get("publish_interval")
        delta_publish = _get("delta_publish")

        # Non-connection changes
        if topic is not None and str(topic) != self._topic:
            self._topic = str(topic)
            _LOGGER.debug("MQTT topic updated (topic=%s)", self._topic)

        if publish_interval is not None:
            try:
                pi = int(publish_interval)
                if pi != self._publish_interval:
                    self._publish_interval = pi
                    _LOGGER.debug("Publish interval updated (sec=%s)", pi)
            except Exception:
                pass

        if delta_publish is not None:
            try:
                dp = bool(delta_publish)
                if dp != self._delta_publish:
                    self._delta_publish = dp
                    # Reset delta state when switching modes
                    self._last_sent_map.clear()
                    _LOGGER.info("Delta publish updated (enabled=%s)", self._delta_publish)
            except Exception:
                pass

        # Connection-affecting changes -> request reconnect
        new_cfg = replace(
            self._conn_cfg,
            broker=str(broker) if broker is not None else self._conn_cfg.broker,
            port=int(port) if port is not None else self._conn_cfg.port,
            use_tls=bool(use_tls) if use_tls is not None else self._conn_cfg.use_tls,
            ca_cert=str(ca_cert) if ca_cert else (None if ca_cert is not None else self._conn_cfg.ca_cert),
            username=str(username) if username else (None if username is not None else self._conn_cfg.username),
            password=str(password) if password else (None if password is not None else self._conn_cfg.password),
        )

        if new_cfg != self._conn_cfg:
            self._conn_cfg = new_cfg
            self._tls_configured = False  # TLS context must be rebuilt
            self._reconnect_event.set()
            _LOGGER.info("Connection options changed -> reconnect requested")

    # ------------------------------------------------------------------
    # Runtime lifecycle
    # ------------------------------------------------------------------
    async def async_start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._loop = asyncio.get_running_loop()

        # 🔥 FORCE initial connect
        self._reconnect_event.set()

        self._task = asyncio.create_task(self._run(), name="openiotai_mqtt_runtime")
        _LOGGER.info("OpenIOTAI MQTT runtime started")



    async def async_stop(self) -> None:
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        if self._task:
            try:
                await self._task
            except Exception:
                pass

        await self._disconnect(hard=True)
        _LOGGER.info("OpenIOTAI MQTT runtime stopped")

    async def _run(self) -> None:
        backoff = RECONNECT_INITIAL_SEC

        while not self._stop_event.is_set():
            try:
                # If reconnect requested, force hard disconnect
                if self._reconnect_event.is_set():
                    self._reconnect_event.clear()
                    await self._disconnect(hard=True)

                # 🔥 ALWAYS try to ensure connection
                await self._ensure_connected()
                backoff = RECONNECT_INITIAL_SEC

                # Wait until stop or reconnect requested
                await self._wait_stop_or_reconnect(timeout=30)

            except InvalidAuth as err:
                self.connected = False
                self.last_error = str(err)
                _LOGGER.warning("MQTT auth rejected: %s", err)
                await self._disconnect(hard=True)
                await asyncio.sleep(RECONNECT_MAX_SEC)

            except (TlsError, CannotConnect) as err:
                self.connected = False
                self.last_error = str(err)
                _LOGGER.info("MQTT connect failed, retrying: %s", err)
                await self._disconnect(hard=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SEC)

            except Exception as err:
                self.connected = False
                self.last_error = str(err)
                _LOGGER.info("MQTT runtime error, retrying: %s", err)
                await self._disconnect(hard=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SEC)



    async def _wait_stop_or_reconnect(self, timeout: int) -> None:
        """Wait until stop event, reconnect request, or timeout."""
        stop_task = asyncio.create_task(self._stop_event.wait())
        reco_task = asyncio.create_task(self._reconnect_event.wait())
        try:
            done, pending = await asyncio.wait(
                {stop_task, reco_task},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
        except Exception:
            stop_task.cancel()
            reco_task.cancel()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------
    def _ensure_client(self) -> None:
        """Create and configure the Paho client if missing."""
        if self._client is not None:
            return

        # Use MQTT v3.1.1 explicitly to avoid protocol surprises
        client = mqtt.Client(client_id=self._conn_cfg.client_id, protocol=mqtt.MQTTv311)

        if self._conn_cfg.username:
            client.username_pw_set(self._conn_cfg.username, self._conn_cfg.password)

        # Important: disable built-in reconnect delays; we control reconnect from _run()
        try:
            client.reconnect_delay_set(min_delay=1, max_delay=1)
        except Exception:
            pass


    def _on_connect(_client, _userdata, _flags, rc, _properties=None):
        self._conn_rc = rc

        if rc == 0:
            self.connected = True
            self.last_error = None
            self._reported_runtime_error = False
            _LOGGER.info("MQTT connected (rc=%s)", rc)

        elif rc in (4, 5):
            self.connected = False
            self.last_error = f"connack rc={rc}"
            _LOGGER.info("MQTT connect rejected (auth) (rc=%s)", rc)

        else:
            self.connected = False
            self.last_error = f"connack rc={rc}"
            _LOGGER.info("MQTT connect rejected (rc=%s)", rc)

        if self._conn_event and self._loop:
            self._loop.call_soon_threadsafe(self._conn_event.set)



        def _on_disconnect(_client, _userdata, rc, _properties=None):
            self.connected = False
            _LOGGER.info("MQTT disconnected (rc=%s)", rc)

            # 🔥 ÄLÄ laukaise reconnectiä, jos disconnect oli tahallinen
            if rc != 0 and not self._intentional_disconnect and self._loop:
                self._loop.call_soon_threadsafe(self._reconnect_event.set)


        client.on_connect = _on_connect
        client.on_disconnect = _on_disconnect

        self._client = client
        self._tls_configured = False
        self._loop_started = False
        _LOGGER.debug("MQTT client created")



    async def _ensure_tls(self) -> None:
        """Ensure TLS is configured for the current MQTT client.

        This is called during connect / reconnect and must be safe to call multiple times.
        """
        # TLS not in use → nothing to do
        if not self._conn_cfg.use_tls:
            _LOGGER.debug(
                "MQTT TLS not enabled (broker=%s:%s)",
                self._conn_cfg.broker,
                self._conn_cfg.port,
            )
            return

        # Already configured for this client
        if self._tls_configured:
            _LOGGER.debug(
                "MQTT TLS already configured (broker=%s:%s)",
                self._conn_cfg.broker,
                self._conn_cfg.port,
            )
            return

        # Client must exist before TLS setup
        if self._client is None:
            msg = (
                f"MQTT client missing before TLS setup "
                f"(broker={self._conn_cfg.broker}:{self._conn_cfg.port})"
            )
            _LOGGER.warning(msg)
            raise CannotConnect(msg)

        _LOGGER.info(
            "Configuring MQTT TLS (broker=%s:%s, cafile=%s)",
            self._conn_cfg.broker,
            self._conn_cfg.port,
            self._conn_cfg.ca_cert or "<system>",
        )

        loop = asyncio.get_running_loop()

        def _build_context() -> ssl.SSLContext:
            ctx = ssl.create_default_context(
                cafile=self._conn_cfg.ca_cert if self._conn_cfg.ca_cert else None
            )
            ctx.check_hostname = True
            ctx.verify_mode = ssl.CERT_REQUIRED
            return ctx

        try:
            # Build SSL context off the event loop
            context = await loop.run_in_executor(None, _build_context)

            # Apply TLS context to MQTT client
            self._client.tls_set_context(context)

            self._tls_configured = True

            _LOGGER.info(
                "MQTT TLS configured successfully (broker=%s:%s)",
                self._conn_cfg.broker,
                self._conn_cfg.port,
            )

        except ssl.SSLError as exc:
            _LOGGER.error(
                "MQTT TLS SSL error (broker=%s:%s): %s",
                self._conn_cfg.broker,
                self._conn_cfg.port,
                exc,
            )
            raise TlsError(str(exc)) from exc

        except Exception as exc:
            _LOGGER.error(
                "MQTT TLS setup failed (broker=%s:%s): %s",
                self._conn_cfg.broker,
                self._conn_cfg.port,
                exc,
            )
            raise TlsError(str(exc)) from exc






    async def _connect(self) -> None:
        """Connect using connect_async + loop_start, then wait for CONNACK."""
        assert self._client is not None
        assert self._loop is not None

        self.last_connect_attempt = datetime.utcnow().isoformat()
        self._conn_event = asyncio.Event()
        self._conn_rc = None

        _LOGGER.info(
            "Connecting to MQTT broker (broker=%s:%s, tls=%s, client_id=%s, keepalive=%s)",
            self._conn_cfg.broker,
            self._conn_cfg.port,
            self._conn_cfg.use_tls,
            self._conn_cfg.client_id,
            self._conn_cfg.keepalive,
        )

        try:
            # Start network loop exactly once per client lifetime
            if not self._loop_started:
                self._client.loop_start()
                self._loop_started = True

            # Non-blocking connect; handshake happens in Paho network thread
            self._client.connect_async(
                self._conn_cfg.broker,
                self._conn_cfg.port,
                keepalive=self._conn_cfg.keepalive,
            )

            await asyncio.wait_for(self._conn_event.wait(), timeout=self._connect_timeout)

        except asyncio.TimeoutError as exc:
            raise CannotConnect("connect timeout") from exc
        except ssl.SSLError as exc:
            raise TlsError(str(exc)) from exc
        except OSError as exc:
            raise CannotConnect(str(exc)) from exc

        rc = self._conn_rc
        if rc is None:
            raise CannotConnect("no connack")
        if rc == 0:
            return
        if rc in (4, 5):
            raise InvalidAuth(f"connack rc={rc}")
        raise CannotConnect(f"connack rc={rc}")


    async def _ensure_connected(self) -> None:
        """Ensure MQTT client is fully connected (client → TLS → connect)."""
        async with self._lock:

            # 1️⃣ Ensure client exists FIRST
            if self._client is None:
                _LOGGER.info(
                    "Creating MQTT client (broker=%s:%s)",
                    self._conn_cfg.broker,
                    self._conn_cfg.port,
                )
                self._ensure_client()
                self._tls_configured = False
                self._loop_started = False

            # 2️⃣ Configure TLS only AFTER client exists
            await self._ensure_tls()

            # 3️⃣ Attempt connect if not connected
            if not self.connected:
                await self._connect()




    async def _disconnect(self, *, hard: bool) -> None:
        """Disconnect and stop network loop safely.

        hard=True:
          - fully stop loop thread and drop client
          - used for reconnect-after-options-change or unrecoverable errors

        hard=False:
          - request disconnect but keep client instance
        """
        client = self._client
        if not client:
            self.connected = False
            return

        loop = asyncio.get_running_loop()

        def _do() -> None:
            try:
                client.disconnect()
            except Exception:
                pass
            # Stop the network thread only on hard disconnect.
            if hard:
                try:
                    client.loop_stop()
                except Exception:
                    pass

        await loop.run_in_executor(None, _do)
        self.connected = False

        if hard:
            self._client = None
            self._tls_configured = False
            self._loop_started = False



    def _compute_delta_entities(self, snapshot: Dict[str, Any]) -> set[str]:
        """Return a set of entity_ids whose payload has changed since last publish.

        Uses stable JSON comparison per entity.
        Does NOT mutate internal state.
        """
        if not snapshot:
            return set()

        changed: set[str] = set()

        # Changed / new entities
        for entity_id, value in snapshot.items():
            key = str(entity_id)
            value_json = _stable_json(value)
            if self._last_sent_map.get(key) != value_json:
                changed.add(key)

        # Removed entities → publish tombstone (value=None)
        removed = set(self._last_sent_map.keys()) - {str(k) for k in snapshot.keys()}
        for key in removed:
            changed.add(key)

        return changed


    def _commit_entity_state(self, entity_id: str, value: Any) -> None:
        """Commit last sent state for a single entity after successful publish."""
        if value is None:
            self._last_sent_map.pop(entity_id, None)
        else:
            self._last_sent_map[entity_id] = _stable_json(value)


    def _build_entity_event(self, entity_id: str, state: Any) -> Dict[str, Any]:
        """Build a ha.entity.v1 event from raw HA state data."""
        now = datetime.utcnow().isoformat() + "Z"

        attributes = {}
        raw_state = None

        if isinstance(state, dict):
            raw_state = state.get("state")
            attributes = state.get("attributes", {})
            last_changed = state.get("last_changed")
            last_updated = state.get("last_updated")
        else:
            raw_state = state
            last_changed = None
            last_updated = None

        numeric_value: Optional[float] = None
        is_numeric = False

        try:
            numeric_value = float(raw_state)
            is_numeric = True
        except Exception:
            pass

        domain = entity_id.split(".", 1)[0] if "." in entity_id else "unknown"

        return {
            "source": "homeassistant",
            "schema": "ha.entity.v1",
            "domain": domain,
            "entity_id": entity_id,
            "event_type": "state_changed",

            "timestamps": {
                "last_changed": last_changed,
                "last_updated": last_updated,
                "ingest_received": now,
            },

            "state": {
                "raw": raw_state,
                "numeric": is_numeric,
                "value": numeric_value,
                "is_null": raw_state is None,
            },

            "attributes": attributes,
        }


    async def publish_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Publish entity events (one MQTT message per entity).

        Snapshot is used only as input; it is never published as-is.
        """
        if not self.connected or not self._client or not snapshot:
            return

        # Determine which entities to publish
        if self._delta_publish:
            entities = self._compute_delta_entities(snapshot)
            if not entities:
                return
        else:
            entities = set(snapshot.keys())

        loop = asyncio.get_running_loop()

        for entity_id in entities:
            value = snapshot.get(entity_id)  # may be None (tombstone)
            event = self._build_entity_event(entity_id, value)
            payload = json.dumps(_json_safe(event), ensure_ascii=False, separators=(",", ":"))

            def _publish_blocking() -> None:
                info = self._client.publish(
                    self._topic,
                    payload,
                    qos=1,
                    retain=False,
                )
                info.wait_for_publish(timeout=self._publish_timeout)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError(f"publish rc={info.rc}")

            try:
                await asyncio.wait_for(
                    loop.run_in_executor(None, _publish_blocking),
                    timeout=self._publish_timeout + 2,
                )
                self._commit_entity_state(str(entity_id), value)
                _LOGGER.debug("Published entity event (entity_id=%s)", entity_id)

            except Exception as exc:
                self.connected = False
                self.last_error = str(exc)
                _LOGGER.debug("MQTT publish failed (entity=%s): %s", entity_id, exc)
                self._reconnect_event.set()
                return
