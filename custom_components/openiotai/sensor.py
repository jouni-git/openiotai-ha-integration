"""Sensor platform for OpenIOTAI integration.

Initializes polling coordinator and exports snapshots via MQTT.
No Home Assistant sensor entities are created.
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
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
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
    """Set up OpenIOTAI sensor platform."""
    entry_id = entry.entry_id

    _LOGGER.info(
        "Setting up OpenIOTAI sensor platform (entry_id=%s)",
        entry_id,
    )

    # ------------------------------------------------------------------
    # 1. Polling coordinator
    # ------------------------------------------------------------------
    coordinator = OpenIOTAIDataCoordinator(hass)
    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry_id] = coordinator

    # ------------------------------------------------------------------
    # 2. Resolve MQTT configuration (OPTIONS → DATA fallback)
    # ------------------------------------------------------------------
    cfg = entry.options or entry.data

    mqtt_broker: Optional[str] = cfg.get(CONF_MQTT_BROKER)
    mqtt_port: int = cfg.get(CONF_MQTT_PORT, DEFAULT_MQTT_PORT)
    mqtt_topic: str = cfg.get(CONF_MQTT_TOPIC, DEFAULT_MQTT_TOPIC)
    mqtt_tls: bool = cfg.get(CONF_MQTT_TLS, DEFAULT_MQTT_TLS)
    mqtt_ca_cert: Optional[str] = cfg.get(CONF_MQTT_CA_CERT)
    mqtt_username: Optional[str] = cfg.get(CONF_MQTT_USERNAME)
    mqtt_password: Optional[str] = cfg.get(CONF_MQTT_PASSWORD)

    if not mqtt_broker:
        _LOGGER.warning(
            "MQTT broker not configured, OpenIOTAI export disabled "
            "(entry_id=%s)",
            entry_id,
        )
        return

    exporter = OpenIOTAIMQTTExporter(
        broker=mqtt_broker,
        port=mqtt_port,
        topic=mqtt_topic,
        use_tls=mqtt_tls,
        ca_cert=mqtt_ca_cert,
        username=mqtt_username,
        password=mqtt_password,
        client_id=f"openiotai-ha-{entry_id}",
    )

    _LOGGER.info(
        "OpenIOTAI MQTT export initialized (lazy connect, entry_id=%s)",
        entry_id,
    )

    # ------------------------------------------------------------------
    # 3. Export snapshot after each polling update (runtime)
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
            await exporter.publish_snapshot(snapshot)
        except Exception:
            _LOGGER.exception(
                "OpenIOTAI MQTT export failed (entry_id=%s)",
                entry_id,
            )

    coordinator.async_add_listener(
        lambda: hass.async_create_task(_export_after_update())
    )

    _LOGGER.info(
        "OpenIOTAI MQTT export pipeline activated (entry_id=%s)",
        entry_id,
    )
