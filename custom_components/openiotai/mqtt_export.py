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

        self._client = mqtt.Client(client_id=client_id)
        self._connected = False

        if self._username:
            self._client.username_pw_set(self._username, self._password)

        if self._use_tls:
            self._configure_tls()

        _LOGGER.info(
            "MQTT exporter initialized (broker=%s:%s, topic=%s, tls=%s, auth=%s)",
            broker,
            port,
            topic,
            use_tls,
            "enabled" if username else "disabled",
        )

    # ------------------------------------------------------------------
    # TLS
    # ------------------------------------------------------------------
    def _configure_tls(self) -> None:
        context = ssl.create_default_context(
            cafile=self._ca_cert if self._ca_cert else None
        )
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        self._client.tls_set_context(context)

    # ------------------------------------------------------------------
    # Connection handling (LAZY)
    # ------------------------------------------------------------------
    def _ensure_connected(self) -> None:
        if self._connected:
            return

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
        """Publish a snapshot to MQTT."""
        try:
            self._ensure_connected()

            payload = json.dumps(snapshot)
            info = self._client.publish(self._topic, payload)

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed (rc={info.rc})")

            _LOGGER.debug(
                "MQTT publish successful (entities=%d, topic=%s)",
                len(snapshot),
                self._topic,
            )

        except Exception:
            # Connection is no longer valid → force reconnect next time
            self._connected = False
            _LOGGER.exception("MQTT publish failed")
            raise
