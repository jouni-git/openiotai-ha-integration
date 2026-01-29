"""MQTT export for OpenIOTAI integration."""

from __future__ import annotations

import json
import logging
import ssl
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt

_LOGGER = logging.getLogger(__name__)


class OpenIOTAIMQTTExporter:
    """Exports OpenIOTAI snapshots to MQTT."""

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
    ) -> None:
        self._broker = broker
        self._port = port
        self._topic = topic
        self._use_tls = use_tls
        self._ca_cert = ca_cert
        self._username = username
        self._password = password
        self._client_id = client_id

        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._tls_configured = False

        _LOGGER.info(
            "MQTT exporter initialized (broker=%s:%s, topic=%s, tls=%s)",
            broker,
            port,
            topic,
            use_tls,
        )

    # ------------------------------------------------------------------
    # Lazy init helpers
    # ------------------------------------------------------------------
    def _ensure_client(self) -> None:
        if self._client:
            return

        self._client = mqtt.Client(client_id=self._client_id)

        if self._username:
            self._client.username_pw_set(self._username, self._password)

        _LOGGER.debug("MQTT client created")

    def _ensure_tls(self) -> None:
        if not self._use_tls or self._tls_configured:
            return

        assert self._client is not None

        _LOGGER.info(
            "Configuring MQTT TLS (ca_cert=%s)",
            self._ca_cert or "system default",
        )

        # Run blocking TLS setup outside event loop
        context = asyncio.get_running_loop().run_until_complete(
            asyncio.get_running_loop().run_in_executor(
                None,
                lambda: ssl.create_default_context(
                    cafile=self._ca_cert if self._ca_cert else None
                ),
            )
        )

        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED

        self._client.tls_set_context(context)
        self._tls_configured = True



    def _ensure_connected(self) -> None:
        if self._connected:
            return

        self._ensure_client()
        self._ensure_tls()

        assert self._client is not None

        _LOGGER.info(
            "Connecting to MQTT broker (broker=%s:%s)",
            self._broker,
            self._port,
        )

        result = self._client.connect(self._broker, self._port, keepalive=30)

        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT connect failed (rc={result})")

        self._connected = True
        _LOGGER.info("MQTT connection established")

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    def publish_snapshot(self, snapshot: Dict[str, Any]) -> None:
        try:
            self._ensure_connected()

            payload = json.dumps(snapshot)
            info = self._client.publish(self._topic, payload)

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed (rc={info.rc})")

            _LOGGER.debug(
                "MQTT publish successful (entities=%d)",
                len(snapshot),
            )

        except Exception:
            self._connected = False
            _LOGGER.exception("MQTT publish failed")
            raise
