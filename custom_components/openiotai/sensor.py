"""Sensor platform for OpenIOTAI integration.

This platform does not create Home Assistant sensor entities.
It initializes the polling coordinator and exports snapshots via MQTT.
"""

from __future__ import annotations

import logging
from typing import Optional

from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry

from .const import (
    DOMAIN,
    CONF_MQTT_BROKER,
    CONF_MQTT_PORT,
    CONF_MQTT_TOPIC,
    CONF_MQTT_TLS,
    CONF_MQTT_CA_CERT,
    DEFAULT_MQTT_PORT,
    DEFAULT_MQTT_TOPIC,
    DEFAULT_MQTT_TLS,
)
from .coordinator import OpenIOTAIDataCoordinator
from .mqtt_export import OpenIOTAIMQTTExporter

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the OpenIOTAI sensor platform."""
    entry_id = entry.entry_id

    _LOGGER.info(
        "Setting up OpenIOTAI sensor platform (entry_id=%s)",
        entry_id,
    )

    # ------------------------------------------------------------------
    # 1. Initialize polling coordinator
    # ------------------------------------------------------------------
    coordinator = OpenIOTAIDataCoordinator(hass)

    _LOGGER.info(
        "Starting initial OpenIOTAI polling refresh (entry_id=%s)",
        entry_id,
    )

    await coordinator.async_config_entry_first_refresh()

    entity_count = len(coordinator.data or {})

    _LOGGER.info(
        "Initial OpenIOTAI polling completed "
        "(entities=%d, entry_id=%s)",
        entity_count,
        entry_id,
    )

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry_id] = coordinator

    # ------------------------------------------------------------------
    # 2. Resolve MQTT configuration from options
    # ------------------------------------------------------------------
    options = entry.options

    mqtt_broker: Optional[str] = options.get(CONF_MQTT_BROKER)
    mqtt_port: int = options.get(CONF_MQTT_PORT, DEFAULT_MQTT_PORT)
    mqtt_topic: str = options.get(CONF_MQTT_TOPIC, DEFAULT_MQTT_TOPIC)
    mqtt_tls: bool = options.get(CONF_MQTT_TLS, DEFAULT_MQTT_TLS)
    mqtt_ca_cert: Optional[str] = options.get(CONF_MQTT_CA_CERT)

    if not mqtt_broker:
        _LOGGER.warning(
            "MQTT broker not configured, OpenIOTAI export disabled "
            "(entry_id=%s)",
            entry_id,
        )
        return

    _LOGGER.info(
        "OpenIOTAI MQTT configuration resolved "
        "(broker=%s:%s, topic=%s, tls=%s, ca_cert=%s, entry_id=%s)",
        mqtt_broker,
        mqtt_port,
        mqtt_topic,
        mqtt_tls,
        mqtt_ca_cert or "system default",
        entry_id,
    )

    # ------------------------------------------------------------------
    # 3. Initialize MQTT exporter
    # ------------------------------------------------------------------
    exporter = OpenIOTAIMQTTExporter(
        broker=mqtt_broker,
        port=mqtt_port,
        topic=mqtt_topic,
        use_tls=mqtt_tls,
        ca_cert=mqtt_ca_cert,
        client_id=f"openiotai-ha-{entry_id}",
    )

    try:
        exporter.connect()
    except Exception:
        _LOGGER.exception(
            "MQTT connection failed, OpenIOTAI export disabled "
            "(entry_id=%s)",
            entry_id,
        )
        return

    _LOGGER.info(
        "OpenIOTAI MQTT connection established (entry_id=%s)",
        entry_id,
    )

    # ------------------------------------------------------------------
    # 4. Export snapshot after each polling update
    # ------------------------------------------------------------------
    async def _export_after_update() -> None:
        snapshot = coordinator.data or {}

        _LOGGER.info(
            "Exporting OpenIOTAI snapshot to MQTT "
            "(entities=%d, entry_id=%s)",
            len(snapshot),
            entry_id,
        )

        try:
            exporter.publish_snapshot(snapshot)
        except Exception:
            _LOGGER.exception(
                "OpenIOTAI MQTT export failed (entry_id=%s)",
                entry_id,
            )

    # Register listener for coordinator updates
    coordinator.async_add_listener(
        lambda: hass.async_create_task(_export_after_update())
    )

    _LOGGER.info(
        "OpenIOTAI MQTT export pipeline activated (entry_id=%s)",
        entry_id,
    )
