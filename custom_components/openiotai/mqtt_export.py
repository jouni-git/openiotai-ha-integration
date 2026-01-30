"""MQTT export for OpenIOTAI integration.

Design goals:
- Keep the MQTT connection stable (no unnecessary reconnects).
- Never block the Home Assistant event loop.
- Paho callbacks run in a background thread -> always signal asyncio via call_soon_threadsafe.
- Allow options updates without reloading the integration.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Optional

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
        # Optional: allow runtime to store publish interval if you want to use it later
        publish_interval: int | None = None,
    ) -> None:
        # Public runtime state (can be used by diagnostics later)
        self.connected: bool = False
        self.last_error: Optional[str] = None
        self.last_connect_attempt: Optional[str] = None

        # Config
        self._topic = topic
        self._publish_interval = publish_interval

        self._connect_timeout = connect_timeout
        self._publish_timeout = publish_timeout

        self._conn_cfg = _ConnConfig(
            broker=broker,
            port=port,
            use_tls=use_tls,
            ca_cert=ca_cert,
            username=username,
            password=password,
            client_id=client_id,
            keepalive=keepalive,
        )

        # Internal runtime
        self._client: Optional[mqtt.Client] = None
        self._tls_configured: bool = False

        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False

        # Paho callbacks run in Paho network thread
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._conn_event: Optional[asyncio.Event] = None
        self._conn_rc: Optional[int] = None

        # Reconnect request flag (set by update_options on connection-affecting changes)
        self._reconnect_event = asyncio.Event()

        _LOGGER.info(
            "MQTT exporter initialized (broker=%s:%s, tls=%s, topic=%s, client_id=%s)",
            broker,
            port,
            use_tls,
            topic,
            client_id,
        )

    # ------------------------------------------------------------------
    # Options update (called from HA event loop)
    # ------------------------------------------------------------------
    def update_options(self, options: Dict[str, Any]) -> None:
        """Apply updated options without reloading the integration.

        Rules:
        - Topic / publish_interval changes do NOT require reconnect.
        - Broker/port/TLS/auth/client_id changes DO require reconnect.
        """
        # NOTE: Keep these keys aligned with your const.py
        broker = options.get("mqtt_broker")
        port = options.get("mqtt_port")
        topic = options.get("mqtt_topic")
        use_tls = options.get("mqtt_tls")
        ca_cert = options.get("mqtt_ca_cert")
        username = options.get("mqtt_username")
        password = options.get("mqtt_password")
        publish_interval = options.get("publish_interval")

        if topic is not None and topic != self._topic:
            self._topic = str(topic)
            _LOGGER.debug("MQTT topic updated (topic=%s)", self._topic)

        if publish_interval is not None:
            try:
                pi = int(publish_interval)
                if pi != self._publish_interval:
                    self._publish_interval = pi
                    _LOGGER.debug("Publish interval updated (sec=%s)", pi)
            except Exception:
                # Ignore invalid values here; config_flow should validate
                pass

        # Connection-affecting changes -> request reconnect
        new_cfg = _ConnConfig(
            broker=str(broker) if broker is not None else self._conn_cfg.broker,
            port=int(port) if port is not None else self._conn_cfg.port,
            use_tls=bool(use_tls) if use_tls is not None else self._conn_cfg.use_tls,
            ca_cert=str(ca_cert) if ca_cert else None,
            username=str(username) if username else None,
            password=str(password) if password else None,
            client_id=self._conn_cfg.client_id,
            keepalive=self._conn_cfg.keepalive,
        )

        if new_cfg != self._conn_cfg:
            self._conn_cfg = new_cfg
            self._tls_configured = False  # TLS context must be rebuilt
            # Recreate client on reconnect to avoid lingering state
            self._client = None
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
        self._task = asyncio.create_task(self._run(), name="openiotai_mqtt_runtime")
        _LOGGER.debug("OpenIOTAI MQTT runtime started")

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

        await self._disconnect()
        _LOGGER.debug("OpenIOTAI MQTT runtime stopped")

    async def _run(self) -> None:
        backoff = RECONNECT_INITIAL_SEC

        while not self._stop_event.is_set():
            try:
                # If options require reconnect, force disconnect once.
                if self._reconnect_event.is_set():
                    self._reconnect_event.clear()
                    await self._disconnect()

                await self._ensure_connected()
                backoff = RECONNECT_INITIAL_SEC

                # Sleep until stop OR reconnect request
                await self._wait_stop_or_reconnect(timeout=30)

            except InvalidAuth as err:
                self.connected = False
                self.last_error = str(err)
                _LOGGER.warning("MQTT auth rejected: %s", err)
                await self._disconnect()
                # Auth errors usually won't recover by retries -> wait longer
                await self._wait_stop_or_reconnect(timeout=RECONNECT_MAX_SEC)

            except (TlsError, CannotConnect) as err:
                self.connected = False
                self.last_error = str(err)
                _LOGGER.debug("MQTT connect attempt failed: %s", err)
                await self._disconnect()
                await self._wait_stop_or_reconnect(timeout=backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SEC)

            except Exception as err:
                # Unexpected error: keep it DEBUG to avoid log spam
                self.connected = False
                self.last_error = str(err)
                _LOGGER.debug("MQTT runtime error (will retry): %s", err)
                await self._disconnect()
                await self._wait_stop_or_reconnect(timeout=backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SEC)

    async def _wait_stop_or_reconnect(self, timeout: int) -> None:
        """Wait until stop event, reconnect request, or timeout."""
        async def _waiter() -> None:
            await asyncio.wait(
                [
                    asyncio.create_task(self._stop_event.wait()),
                    asyncio.create_task(self._reconnect_event.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

        try:
            await asyncio.wait_for(_waiter(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------
    def _ensure_client(self) -> None:
        """Create and configure the Paho client if missing."""
        if self._client is not None:
            return

        # NOTE: You can set protocol explicitly if you want MQTTv311:
        # client = mqtt.Client(client_id=self._conn_cfg.client_id, protocol=mqtt.MQTTv311)
        client = mqtt.Client(client_id=self._conn_cfg.client_id)

        if self._conn_cfg.username:
            client.username_pw_set(self._conn_cfg.username, self._conn_cfg.password)

        def _on_connect(_client, _userdata, _flags, rc, _properties=None):
            self._conn_rc = rc
            self.connected = rc == 0
            self.last_error = None if rc == 0 else f"connack rc={rc}"

            if rc == 0:
                _LOGGER.info("MQTT connected (rc=%s)", rc)
            elif rc in (4, 5):
                _LOGGER.info("MQTT connect rejected (auth) (rc=%s)", rc)
            else:
                _LOGGER.info("MQTT connect rejected (rc=%s)", rc)

            if self._conn_event and self._loop:
                self._loop.call_soon_threadsafe(self._conn_event.set)

        def _on_disconnect(_client, _userdata, rc, _properties=None):
            self.connected = False
            _LOGGER.info("MQTT disconnected (rc=%s)", rc)

        client.on_connect = _on_connect
        client.on_disconnect = _on_disconnect

        self._client = client
        _LOGGER.debug("MQTT client created")

    async def _ensure_tls(self) -> None:
        """Configure TLS context once per client."""
        if not self._conn_cfg.use_tls or self._tls_configured:
            return

        assert self._client is not None

        loop = asyncio.get_running_loop()

        def _build_context() -> ssl.SSLContext:
            ctx = ssl.create_default_context(
                cafile=self._conn_cfg.ca_cert if self._conn_cfg.ca_cert else None
            )
            ctx.check_hostname = True
            ctx.verify_mode = ssl.CERT_REQUIRED
            return ctx

        try:
            context = await loop.run_in_executor(None, _build_context)
            self._client.tls_set_context(context)
            self._tls_configured = True
            _LOGGER.info("MQTT TLS configured")
        except Exception as exc:
            raise TlsError(str(exc)) from exc

    async def _connect(self) -> None:
        """Connect using connect_async + loop_start, then wait for CONNACK.

        connect_async is designed to be used with loop_start().  (Paho docs)
        """
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
            # Non-blocking connect; network loop thread handles the handshake.
            self._client.connect_async(
                self._conn_cfg.broker,
                self._conn_cfg.port,
                keepalive=self._conn_cfg.keepalive,
            )
            self._client.loop_start()

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
        if self.connected:
            return

        async with self._lock:
            if self.connected:
                return

            self._ensure_client()
            await self._ensure_tls()
            await self._connect()

    async def _disconnect(self) -> None:
        """Disconnect and stop network loop safely."""
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
            try:
                client.loop_stop()
            except Exception:
                pass

        await loop.run_in_executor(None, _do)
        self.connected = False

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    async def publish_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Publish a snapshot. If not connected, silently drop."""
        if not self.connected or not self._client:
            return

        payload = json.dumps(
            _json_safe(snapshot),
            ensure_ascii=False,
            separators=(",", ":"),
        )

        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug("MQTT payload size=%d bytes", len(payload))

        loop = asyncio.get_running_loop()

        def _publish_blocking() -> None:
            info = self._client.publish(
                self._topic,
                payload,
                qos=1,
                retain=True,
            )
            # This is blocking -> run in executor
            info.wait_for_publish(timeout=self._publish_timeout)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"publish rc={info.rc}")

        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, _publish_blocking),
                timeout=self._publish_timeout + 2,
            )
            _LOGGER.debug("MQTT publish successful (keys=%d)", len(snapshot))
        except Exception as exc:
            # Treat publish failures as connection problems -> reconnect later
            self.connected = False
            self.last_error = str(exc)
            _LOGGER.debug("MQTT publish failed (will reconnect): %s", exc)
            await self._disconnect()
