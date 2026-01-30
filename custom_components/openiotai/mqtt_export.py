"""MQTT export for OpenIOTAI integration."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import date, datetime
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt
from homeassistant.exceptions import ConfigEntryNotReady

_LOGGER = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT_SEC = 10
DEFAULT_PUBLISH_TIMEOUT_SEC = 10
DEFAULT_KEEPALIVE_SEC = 30


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

        self._client: Optional[mqtt.Client] = None
        self._tls_configured = False
        self._connected = False

        self._lock = asyncio.Lock()

        _LOGGER.info(
            "MQTT exporter initialized (broker=%s:%s, topic=%s, tls=%s, client_id=%s)",
            broker,
            port,
            topic,
            use_tls,
            client_id,
        )

    # ------------------------------------------------------------------
    # SETUP-TIME CONNECTION TEST
    # ------------------------------------------------------------------
    async def async_test_connection(self) -> None:
        """Test MQTT connection and authentication during setup.

        Performs a real CONNECT + CONNACK round-trip.
        Raises ConfigEntryNotReady on failure.
        """
        loop = asyncio.get_running_loop()
        connected = asyncio.Event()
        error: dict[str, Optional[str]] = {"reason": None}

        def _on_connect(_client, _userdata, _flags, rc, _properties=None):
            if rc == 0:
                loop.call_soon_threadsafe(connected.set)
            else:
                error["reason"] = f"MQTT authentication failed (rc={rc})"
                loop.call_soon_threadsafe(connected.set)

        client = mqtt.Client(client_id=f"{self._client_id}-test")

        if self._username:
            client.username_pw_set(self._username, self._password)

        client.on_connect = _on_connect

        if self._use_tls:
            def _build_context() -> ssl.SSLContext:
                return ssl.create_default_context(
                    cafile=self._ca_cert if self._ca_cert else None
                )

            context = await loop.run_in_executor(None, _build_context)
            client.tls_set_context(context)

        try:
            client.connect(self._broker, self._port, keepalive=10)
            client.loop_start()

            try:
                await asyncio.wait_for(
                    connected.wait(),
                    timeout=self._connect_timeout,
                )
            except asyncio.TimeoutError as e:
                raise ConfigEntryNotReady("MQTT connection timeout") from e

            if error["reason"]:
                raise ConfigEntryNotReady(error["reason"])

        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Lazy init helpers
    # ------------------------------------------------------------------
    def _ensure_client(self) -> None:
        if self._client is not None:
            return

        client = mqtt.Client(client_id=self._client_id)

        if self._username:
            client.username_pw_set(self._username, self._password)

        def _on_connect(_client, _userdata, _flags, rc, _properties=None):
            _LOGGER.info("MQTT on_connect rc=%s", rc)

        def _on_disconnect(_client, _userdata, rc, _properties=None):
            _LOGGER.info("MQTT on_disconnect rc=%s", rc)

        client.on_connect = _on_connect
        client.on_disconnect = _on_disconnect

        self._client = client
        _LOGGER.debug("MQTT client created")

    async def _ensure_tls(self) -> None:
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

        context = await loop.run_in_executor(None, _build_context)

        self._client.tls_set_context(context)
        self._tls_configured = True
        _LOGGER.info("MQTT TLS configured successfully")

    async def _connect(self) -> None:
        assert self._client is not None

        loop = asyncio.get_running_loop()

        def _do_connect() -> None:
            rc = self._client.connect(
                self._broker,
                self._port,
                keepalive=self._keepalive,
            )
            if rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT connect failed (rc={rc})")

            self._client.loop_start()

        _LOGGER.info(
            "Connecting to MQTT broker (broker=%s:%s)",
            self._broker,
            self._port,
        )

        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, _do_connect),
                timeout=self._connect_timeout,
            )
        except asyncio.TimeoutError as e:
            raise RuntimeError(
                f"MQTT connect timed out after {self._connect_timeout}s"
            ) from e

        self._connected = True
        _LOGGER.info("MQTT connection established")

    async def _ensure_connected(self) -> None:
        if self._connected:
            return

        async with self._lock:
            if self._connected:
                return

            self._ensure_client()
            await self._ensure_tls()
            await self._connect()

    async def _disconnect(self) -> None:
        if self._client is None:
            self._connected = False
            return

        client = self._client
        loop = asyncio.get_running_loop()

        def _do_disconnect() -> None:
            try:
                client.disconnect()
            finally:
                try:
                    client.loop_stop()
                except Exception:
                    pass

        try:
            await loop.run_in_executor(None, _do_disconnect)
        except Exception:
            _LOGGER.exception("MQTT disconnect failed (ignored)")
        finally:
            self._connected = False

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    async def publish_snapshot(self, snapshot: Dict[str, Any]) -> None:
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

        _LOGGER.info(
            "MQTT publishing to topic=%s broker=%s:%s tls=%s user=%s",
            self._topic,
            self._broker,
            self._port,
            self._use_tls,
            self._username,
        )

        def _do_publish() -> None:
            info = self._client.publish(
                self._topic,
                payload,
                qos=1,
                retain=True,
            )

            try:
                info.wait_for_publish(timeout=self._publish_timeout)
            except RuntimeError:
                return

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed (rc={info.rc})")

        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, _do_publish),
                timeout=self._publish_timeout + 2,
            )
            _LOGGER.debug(
                "MQTT publish successful (entities=%d)",
                len(snapshot),
            )
        except RuntimeError as e:
            _LOGGER.warning(
                "MQTT publish did not fully complete (will retry next cycle): %s",
                e,
            )
            await self._disconnect()
