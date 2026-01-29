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

        _LOGGER.info(
            "Initializing MQTT exporter "
            "(broker=%s:%s, topic=%s, tls=%s, auth=%s, client_id=%s)",
            broker,
            port,
            topic,
            use_tls,
            "enabled" if username else "disabled",
            client_id,
        )

        self._client = mqtt.Client(client_id=client_id)

        # ------------------------------------------------------------------
        # Authentication (username/password)
        # ------------------------------------------------------------------
        if self._username:
            self._client.username_pw_set(self._username, self._password)
            _LOGGER.info(
                "MQTT authentication enabled (username=%s)",
                self._username,
            )
        else:
            _LOGGER.info("MQTT authentication not configured")

        # ------------------------------------------------------------------
        # TLS configuration
        # ------------------------------------------------------------------
        if self._use_tls:
            self._configure_tls()
        else:
            _LOGGER.info("MQTT TLS disabled, using plaintext connection")

    # ------------------------------------------------------------------
    # TLS configuration
    # ------------------------------------------------------------------
    def _configure_tls(self) -> None:
        """Configure TLS for the MQTT client."""
        _LOGGER.info(
            "Enabling MQTT TLS (ca_cert=%s)",
            self._ca_cert if self._ca_cert else "system default",
        )

        try:
            context = ssl.create_default_context(
                cafile=self._ca_cert if self._ca_cert else None
            )

            # Explicitly require certificate validation
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED

            self._client.tls_set_context(context)

            if self._port == 1883:
                _LOGGER.warning(
                    "MQTT TLS enabled but port is 1883 "
                    "(this is unusual and likely misconfigured)"
                )

        except Exception:
            _LOGGER.exception("Failed to configure MQTT TLS")
            raise

    # ------------------------------------------------------------------
    # Connection handling
    # ------------------------------------------------------------------
    def connect(self) -> None:
        """Connect to the MQTT broker."""
        _LOGGER.info(
            "Connecting to MQTT broker (broker=%s:%s)",
            self._broker,
            self._port,
        )

        try:
            result = self._client.connect(self._broker, self._port)

            if result != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(
                    f"MQTT connect failed with result code {result}"
                )

            _LOGGER.info("MQTT connection established")

        except Exception:
            _LOGGER.exception("MQTT connection failed")
            raise

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        _LOGGER.info("Disconnecting from MQTT broker")

        try:
            self._client.disconnect()
            _LOGGER.info("MQTT disconnected successfully")
        except Exception:
            _LOGGER.exception("MQTT disconnect failed")

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    def publish_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Publish a snapshot to MQTT."""
        entity_count = len(snapshot)

        _LOGGER.info(
            "Publishing snapshot to MQTT "
            "(entities=%d, topic=%s)",
            entity_count,
            self._topic,
        )

        try:
            payload = json.dumps(snapshot)
        except Exception:
            _LOGGER.exception("Failed to serialize snapshot to JSON")
            raise

        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug(
                "MQTT payload size=%d bytes",
                len(payload),
            )
            _LOGGER.debug(
                "MQTT snapshot sample entity_ids=%s",
                list(snapshot.keys())[:3],
            )

        try:
            info = self._client.publish(self._topic, payload)

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(
                    f"MQTT publish failed with rc={info.rc}"
                )

            _LOGGER.debug("MQTT publish successful")

        except Exception:
            _LOGGER.exception("MQTT publish failed")
            raise
