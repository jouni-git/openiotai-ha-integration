"""MQTT export for OpenIOTAI integration."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from datetime import date, datetime
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt

_LOGGER = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT_SEC = 10
DEFAULT_PUBLISH_TIMEOUT_SEC = 10
DEFAULT_KEEPALIVE_SEC = 30

# Reconnect strategy (exponential backoff with cap)
RECONNECT_INITIAL_SEC = 2
RECONNECT_MAX_SEC = 60


# ---------------------------------------------------------------------
# Exceptions used by config/options flow
# ---------------------------------------------------------------------
class CannotConnect(Exception):
    """Raised when MQTT broker cannot be reached / connection fails."""


class InvalidAuth(Exception):
    """Raised when broker rejects credentials (CONNACK auth related)."""


class TlsError(Exception):
    """Raised when TLS handshake / certificate validation fails."""


def _json_safe(obj: Any) -> Any:
    """Recursively convert objects into JSON-serializable primitives."""
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


class OpenIOTAIMQTTExporter:
    """Exports OpenIOTAI snapshots to MQTT (TLS supported).

    This class is safe to instantiate during HA setup. It does NOT do
    network I/O unless you call async_test_connection() (options flow)
    or async_start()/publish_snapshot() (runtime).
    """

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
    ) -> None:
        # Config
        self._broker = broker
        self._port = port
        self._topic = topic
        self._use_tls = use_tls
        self._ca_cert = ca_cert
        self._username = username
        self._password = password
        self._client_id = client_id

        self._keepalive = keepalive
        self._connect_timeout = connect_timeout
        self._publish_timeout = publish_timeout

        # Runtime state for diagnostics / sensors
        self.connected: bool = False
        self.last_error: Optional[str] = None
        self.last_connect_attempt: Optional[str] = None

        # Internal state
        self._client: Optional[mqtt.Client] = None
        self._tls_configured: bool = False
        self._lock = asyncio.Lock()

        self._conn_event: Optional[asyncio.Event] = None
        self._conn_rc: Optional[int] = None
        self._disc_rc: Optional[int] = None

        self._running: bool = False
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

        _LOGGER.info(
            "MQTT exporter initialized (broker=%s:%s, topic=%s, tls=%s, client_id=%s)",
            broker,
            port,
            topic,
            use_tls,
            client_id,
        )

    # ------------------------------------------------------------------
    # Public properties for diagnostics
    # ------------------------------------------------------------------
    @property
    def broker(self) -> str:
        return self._broker

    @property
    def port(self) -> int:
        return self._port

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def tls(self) -> bool:
        return self._use_tls

    # ------------------------------------------------------------------
    # OPTIONS-FLOW CONNECTION TEST (real connect + CONNACK)
    # ------------------------------------------------------------------
    async def async_test_connection(self) -> None:
        """Test MQTT connection and authentication (options flow).

        Performs a real CONNECT + CONNACK round-trip.
        Raises:
          - CannotConnect
          - InvalidAuth
          - TlsError
        """
        loop = asyncio.get_running_loop()
        connected = asyncio.Event()
        result: dict[str, Any] = {"rc": None, "err": None}

        def _on_connect(_client, _userdata, _flags, rc, _properties=None):
            result["rc"] = rc
            loop.call_soon_threadsafe(connected.set)

        client = mqtt.Client(client_id=f"{self._client_id}-test")

        if self._username:
            client.username_pw_set(self._username, self._password)

        client.on_connect = _on_connect

        if self._use_tls:
            try:
                def _build_context() -> ssl.SSLContext:
                    return ssl.create_default_context(
                        cafile=self._ca_cert if self._ca_cert else None
                    )

                context = await loop.run_in_executor(None, _build_context)
                client.tls_set_context(context)
            except ssl.SSLError as e:
                raise TlsError(f"TLS configuration failed: {e}") from e
            except Exception as e:
                raise TlsError(f"TLS configuration failed: {e}") from e

        try:
            # Note: connect() itself can raise socket errors immediately
            try:
                client.connect(self._broker, self._port, keepalive=10)
            except ssl.SSLError as e:
                raise TlsError(f"TLS handshake failed: {e}") from e
            except OSError as e:
                raise CannotConnect(f"MQTT connect failed: {e}") from e
            except Exception as e:
                raise CannotConnect(f"MQTT connect failed: {e}") from e

            client.loop_start()

            try:
                await asyncio.wait_for(connected.wait(), timeout=self._connect_timeout)
            except asyncio.TimeoutError as e:
                raise CannotConnect("MQTT connection timeout") from e

            rc = result.get("rc")
            # Paho rc meanings: 0=success, 4/5 typically auth failure, others may vary
            if rc == 0:
                return
            if rc in (4, 5):
                raise InvalidAuth(f"MQTT authentication failed (rc={rc})")
            raise CannotConnect(f"MQTT connection rejected (rc={rc})")

        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Runtime lifecycle
    # ------------------------------------------------------------------
    async def async_start(self) -> None:
        """Start background runtime connection management.

        Safe to call during HA setup; does not block setup.
        """
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="openiotai_mqtt_runtime")

    async def async_stop(self) -> None:
        """Stop background runtime and disconnect."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._task:
            try:
                await self._task
            except Exception:
                _LOGGER.exception("Runtime task failed on stop (ignored)")
        await self._disconnect()

    async def _run(self) -> None:
        """Background loop: keep MQTT connection alive with backoff."""
        backoff = RECONNECT_INITIAL_SEC
        while not self._stop_event.is_set():
            try:
                await self._ensure_connected()
                # Once connected, wait until disconnected or stop requested.
                # We poll state lightly; paho callbacks will flip connected flag.
                backoff = RECONNECT_INITIAL_SEC
                await self._sleep_or_stop(5)
                continue

            except Exception as err:
                # Connection attempt failed -> record error and backoff
                self.connected = False
                self.last_error = str(err)
                _LOGGER.warning("MQTT runtime connect failed: %s", err)

                await self._disconnect()

                await self._sleep_or_stop(backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SEC)

    async def _sleep_or_stop(self, seconds: int) -> None:
        """Sleep but wake early if stop is requested."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    # ------------------------------------------------------------------
    # Lazy init helpers
    # ------------------------------------------------------------------
    def _ensure_client(self) -> None:
        """Create Paho client and callbacks (no network I/O)."""
        if self._client is not None:
            return

        client = mqtt.Client(client_id=self._client_id)

        if self._username:
            client.username_pw_set(self._username, self._password)

        def _on_connect(_client, _userdata, _flags, rc, _properties=None):
            self._conn_rc = rc
            if rc == 0:
                self.connected = True
                self.last_error = None
                _LOGGER.info("MQTT connected (rc=%s)", rc)
            else:
                self.connected = False
                _LOGGER.warning("MQTT connect rejected (rc=%s)", rc)

            # Signal waiting coroutines (thread-safe)
            if self._conn_event:
                try:
                    asyncio.get_running_loop().call_soon_threadsafe(self._conn_event.set)
                except RuntimeError:
                    # Not in an event loop thread; fall back (still ok)
                    pass

        def _on_disconnect(_client, _userdata, rc, _properties=None):
            self._disc_rc = rc
            self.connected = False
            _LOGGER.warning("MQTT disconnected (rc=%s)", rc)

        client.on_connect = _on_connect
        client.on_disconnect = _on_disconnect

        self._client = client
        _LOGGER.debug("MQTT client created")

    async def _ensure_tls(self) -> None:
        """Configure TLS on the client (no network I/O)."""
        if not self._use_tls or self._tls_configured:
            return

        assert self._client is not None

        _LOGGER.info(
            "Configuring MQTT TLS (ca_cert=%s)",
            self._ca_cert or "system default",
        )

        loop = asyncio.get_running_loop()

        def _build_context() -> ssl.SSLContext:
            ctx = ssl.create_default_context(
                cafile=self._ca_cert if self._ca_cert else None
            )
            ctx.check_hostname = True
            ctx.verify_mode = ssl.CERT_REQUIRED
            return ctx

        try:
            context = await loop.run_in_executor(None, _build_context)
            self._client.tls_set_context(context)
        except ssl.SSLError as e:
            raise TlsError(f"TLS configuration failed: {e}") from e
        except Exception as e:
            raise TlsError(f"TLS configuration failed: {e}") from e

        self._tls_configured = True
        _LOGGER.info("MQTT TLS configured successfully")

    async def _connect(self) -> None:
        """Connect and wait for CONNACK via callback."""
        assert self._client is not None

        self.last_connect_attempt = datetime.utcnow().isoformat()
        self._conn_rc = None
        self._conn_event = asyncio.Event()

        loop = asyncio.get_running_loop()

        def _do_connect() -> None:
            # Start network loop first, then connect.
            # Paho does socket I/O in its background thread.
            self._client.loop_start()
            rc = self._client.connect(
                self._broker,
                self._port,
                keepalive=self._keepalive,
            )
            if rc != mqtt.MQTT_ERR_SUCCESS:
                raise CannotConnect(f"MQTT connect failed (rc={rc})")

        _LOGGER.info("Connecting to MQTT broker (broker=%s:%s)", self._broker, self._port)

        try:
            await asyncio.wait_for(loop.run_in_executor(None, _do_connect), timeout=self._connect_timeout)
        except asyncio.TimeoutError as e:
            raise CannotConnect(f"MQTT connect timed out after {self._connect_timeout}s") from e
        except TlsError:
            raise
        except ssl.SSLError as e:
            raise TlsError(f"TLS handshake failed: {e}") from e
        except OSError as e:
            raise CannotConnect(f"MQTT connect failed: {e}") from e
        except CannotConnect:
            raise
        except Exception as e:
            raise CannotConnect(f"MQTT connect failed: {e}") from e

        # Wait for on_connect callback to confirm CONNACK
        try:
            await asyncio.wait_for(self._conn_event.wait(), timeout=self._connect_timeout)
        except asyncio.TimeoutError as e:
            raise CannotConnect("MQTT CONNACK timeout") from e

        rc = self._conn_rc
        if rc == 0:
            self.connected = True
            self.last_error = None
            return

        self.connected = False
        if rc in (4, 5):
            raise InvalidAuth(f"MQTT authentication failed (rc={rc})")
        raise CannotConnect(f"MQTT connection rejected (rc={rc})")

    async def _ensure_connected(self) -> None:
        """Ensure MQTT client is created and connected."""
        if self.connected:
            return

        async with self._lock:
            if self.connected:
                return

            self._ensure_client()
            await self._ensure_tls()
            await self._connect()

    async def _disconnect(self) -> None:
        """Disconnect the client and stop its loop."""
        if self._client is None:
            self.connected = False
            return

        client = self._client
        loop = asyncio.get_running_loop()

        def _do_disconnect() -> None:
            try:
                client.disconnect()
            except Exception:
                pass
            try:
                client.loop_stop()
            except Exception:
                pass

        try:
            await loop.run_in_executor(None, _do_disconnect)
        except Exception:
            _LOGGER.exception("MQTT disconnect failed (ignored)")
        finally:
            self.connected = False

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    async def publish_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Publish a snapshot to MQTT.

        If not connected, this will try to connect first. On publish failure,
        exporter disconnects so the runtime loop can reconnect cleanly.
        """
        await self._ensure_connected()
        assert self._client is not None

        safe_snapshot = _json_safe(snapshot)

        loop = asyncio.get_running_loop()

        def _serialize() -> str:
            return json.dumps(
                safe_snapshot,
                ensure_ascii=False,
                separators=(",", ":"),
            )

        payload = await loop.run_in_executor(None, _serialize)

        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug("MQTT payload size=%d bytes", len(payload))

        def _do_publish() -> None:
            info = self._client.publish(
                self._topic,
                payload,
                qos=1,
                retain=True,
            )

            # wait_for_publish may raise RuntimeError if not connected
            info.wait_for_publish(timeout=self._publish_timeout)

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed (rc={info.rc})")

        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, _do_publish),
                timeout=self._publish_timeout + 2,
            )
            _LOGGER.debug("MQTT publish successful (keys=%d)", len(snapshot))
        except Exception as e:
            self.connected = False
            self.last_error = str(e)
            _LOGGER.warning("MQTT publish failed, will reconnect: %s", e)
            await self._disconnect()
            # Let caller decide whether to retry later; runtime loop will reconnect.
            raise
