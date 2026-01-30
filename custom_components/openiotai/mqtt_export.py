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

        await self._disconnect(hard=True)
        _LOGGER.debug("OpenIOTAI MQTT runtime stopped")

    async def _run(self) -> None:
        backoff = RECONNECT_INITIAL_SEC

        while not self._stop_event.is_set():
            try:
                # If options require reconnect, do it once here.
                if self._reconnect_event.is_set():
                    self._reconnect_event.clear()
                    await self._disconnect(hard=True)

                await self._ensure_connected()
                backoff = RECONNECT_INITIAL_SEC

                # Wait for stop or reconnect request
                await self._wait_stop_or_reconnect(timeout=30)

            except InvalidAuth as err:
                self.connected = False
                self.last_error = str(err)
                _LOGGER.warning("MQTT auth rejected: %s", err)
                await self._disconnect(hard=True)
                await self._wait_stop_or_reconnect(timeout=RECONNECT_MAX_SEC)

            except (TlsError, CannotConnect) as err:
                self.connected = False
                self.last_error = str(err)
                _LOGGER.debug("MQTT connect attempt failed: %s", err)
                await self._disconnect(hard=True)
                await self._wait_stop_or_reconnect(timeout=backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SEC)

            except Exception as err:
                self.connected = False
                self.last_error = str(err)
                _LOGGER.debug("MQTT runtime error (will retry): %s", err)
                await self._disconnect(hard=True)
                await self._wait_stop_or_reconnect(timeout=backoff)
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
            # Paho uses rc=0 for clean disconnect; nonzero means unexpected (network)
            self.connected = False
            _LOGGER.info("MQTT disconnected (rc=%s)", rc)

            # If disconnect was unexpected, request a reconnect.
            # (Do NOT reconnect here; just signal the runtime loop.)
            if rc != 0 and self._loop:
                self._loop.call_soon_threadsafe(self._reconnect_event.set)

        client.on_connect = _on_connect
        client.on_disconnect = _on_disconnect

        self._client = client
        self._tls_configured = False
        self._loop_started = False
        _LOGGER.debug("MQTT client created")

    async def _ensure_tls(self) -> None:
        """Configure TLS context once per client."""
        if not self._conn_cfg.use_tls or self._tls_configured:
            return

        assert self._client is not None

        loop = asyncio.get_running_loop()

        def _build_context() -> ssl.SSLContext:
            ctx = ssl.create_default_context(cafile=self._conn_cfg.ca_cert if self._conn_cfg.ca_cert else None)
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
        if self.connected:
            return

        async with self._lock:
            if self.connected:
                return

            self._ensure_client()
            await self._ensure_tls()
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

    # ------------------------------------------------------------------
    # Delta computation
    # ------------------------------------------------------------------
    def _compute_delta(self, snapshot: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        """Return (delta_payload, changed_count). Also updates nothing (commit happens after successful publish).

        Snapshot is expected to be a dict mapping keys -> values.
        We consider a key changed if its stable JSON differs from last sent.
        """
        if not snapshot:
            return {}, 0

        delta: Dict[str, Any] = {}
        changed = 0

        # Changed / new keys
        for k, v in snapshot.items():
            ks = str(k)
            vs = _stable_json(v)
            if self._last_sent_map.get(ks) != vs:
                delta[ks] = v
                changed += 1

        # Removed keys (optional): if something disappeared, publish null
        # (You can disable this if you never need "deletions".)
        removed = set(self._last_sent_map.keys()) - {str(k) for k in snapshot.keys()}
        for ks in removed:
            delta[ks] = None
            changed += 1

        return delta, changed

    def _commit_delta_state(self, snapshot: Dict[str, Any]) -> None:
        """Commit last sent state after successful publish."""
        self._last_sent_map = {str(k): _stable_json(v) for k, v in snapshot.items()}

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    async def publish_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Publish a snapshot (full or delta depending on configuration).

        If not connected, silently drop (we do NOT force reconnect here).
        If publish fails, request reconnect (handled by _run()).
        """
        if not self.connected or not self._client:
            return

        # Choose payload
        to_send: Dict[str, Any]
        changed_count: int

        if self._delta_publish:
            to_send, changed_count = self._compute_delta(snapshot)
            if changed_count == 0:
                # Nothing changed -> no publish
                return
        else:
            to_send = snapshot
            changed_count = len(snapshot)

        payload = json.dumps(_json_safe(to_send), ensure_ascii=False, separators=(",", ":"))

        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug("MQTT payload size=%d bytes", len(payload))

        loop = asyncio.get_running_loop()

        def _publish_blocking() -> None:
            # qos=1 to get delivery attempt tracking; retain for latest snapshot/delta
            info = self._client.publish(self._topic, payload, qos=1, retain=True)
            info.wait_for_publish(timeout=self._publish_timeout)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"publish rc={info.rc}")

        try:
            await asyncio.wait_for(loop.run_in_executor(None, _publish_blocking), timeout=self._publish_timeout + 2)
            _LOGGER.debug("MQTT publish successful (keys=%d)", changed_count)

            # Only commit delta baseline after a successful publish
            if self._delta_publish:
                self._commit_delta_state(snapshot)

        except Exception as exc:
            self.connected = False
            self.last_error = str(exc)
            _LOGGER.debug("MQTT publish failed (will reconnect): %s", exc)

            # Do NOT disconnect here; just request reconnect and let _run() do the teardown cleanly.
            self._reconnect_event.set()
