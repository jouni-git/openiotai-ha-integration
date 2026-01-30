"""MQTT export for OpenIOTAI integration."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import date, datetime
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt

_LOGGER = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT_SEC = 10
DEFAULT_PUBLISH_TIMEOUT_SEC = 10
DEFAULT_KEEPALIVE_SEC = 30

RECONNECT_INITIAL_SEC = 2
RECONNECT_MAX_SEC = 60


class CannotConnect(Exception):
    """Raised when MQTT broker cannot be reached / connection fails."""


class InvalidAuth(Exception):
    """Raised when broker rejects credentials."""


class TlsError(Exception):
    """Raised when TLS handshake / certificate validation fails."""


def _json_safe(obj: Any) -> Any:
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
    ) -> None:
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

        self.connected = False
        self.last_error: Optional[str] = None
        self.last_connect_attempt: Optional[str] = None

        self._client: Optional[mqtt.Client] = None
        self._tls_configured = False
        self._lock = asyncio.Lock()

        self._conn_event: Optional[asyncio.Event] = None
        self._conn_rc: Optional[int] = None

        _LOGGER.info(
            "MQTT exporter initialized "
            "(broker=%s:%s, tls=%s, topic=%s, client_id=%s)",
            broker,
            port,
            use_tls,
            topic,
            client_id,
        )

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------
    def _ensure_client(self) -> None:
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
                _LOGGER.info("MQTT connected (rc=0)")
            else:
                self.connected = False
                self.last_error = f"connect rejected rc={rc}"
                _LOGGER.warning("MQTT connect rejected (rc=%s)", rc)

            if self._conn_event:
                try:
                    asyncio.get_running_loop().call_soon_threadsafe(
                        self._conn_event.set
                    )
                except RuntimeError:
                    pass

        def _on_disconnect(_client, _userdata, rc, _properties=None):
            self.connected = False
            if rc != 0:
                self.last_error = f"disconnected rc={rc}"
            _LOGGER.info("MQTT disconnected (rc=%s)", rc)

        client.on_connect = _on_connect
        client.on_disconnect = _on_disconnect

        self._client = client
        _LOGGER.debug("MQTT client created")

    async def _ensure_tls(self) -> None:
        if not self._use_tls or self._tls_configured:
            return

        assert self._client is not None

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
            self._tls_configured = True
            _LOGGER.info("MQTT TLS configured")
        except Exception as e:
            raise TlsError(str(e)) from e

    async def _connect(self) -> None:
        assert self._client is not None

        self.last_connect_attempt = datetime.utcnow().isoformat()
        self._conn_event = asyncio.Event()
        self._conn_rc = None

        loop = asyncio.get_running_loop()

        def _do_connect() -> None:
            self._client.loop_start()
            rc = self._client.connect(
                self._broker,
                self._port,
                keepalive=self._keepalive,
            )
            if rc != mqtt.MQTT_ERR_SUCCESS:
                raise CannotConnect(f"connect rc={rc}")

        _LOGGER.info(
            "Connecting to MQTT broker "
            "(broker=%s:%s, tls=%s, client_id=%s, keepalive=%s)",
            self._broker,
            self._port,
            self._use_tls,
            self._client_id,
            self._keepalive,
        )

        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, _do_connect),
                timeout=self._connect_timeout,
            )
            await asyncio.wait_for(
                self._conn_event.wait(),
                timeout=self._connect_timeout,
            )
        except Exception as e:
            self.connected = False
            self.last_error = str(e)
            raise CannotConnect(str(e)) from e

        if self._conn_rc != 0:
            raise CannotConnect(f"connack rc={self._conn_rc}")

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
        if not self._client:
            self.connected = False
            return

        client = self._client
        loop = asyncio.get_running_loop()

        def _do():
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
        #await self._ensure_connected()
        if not self.connected:
            return

        assert self._client is not None

        payload = json.dumps(
            _json_safe(snapshot),
            ensure_ascii=False,
            separators=(",", ":"),
        )

        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug("MQTT payload size=%d bytes", len(payload))

        loop = asyncio.get_running_loop()

        def _publish() -> None:
            info = self._client.publish(
                self._topic,
                payload,
                qos=1,
                retain=True,
            )
            info.wait_for_publish(timeout=self._publish_timeout)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"publish rc={info.rc}")

        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, _publish),
                timeout=self._publish_timeout + 2,
            )
            _LOGGER.debug(
                "MQTT publish successful (keys=%d)",
                len(snapshot),
            )
        except Exception as e:
            self.connected = False
            self.last_error = str(e)
            _LOGGER.warning("MQTT publish failed, will retry later")
            await self._disconnect()
            raise
