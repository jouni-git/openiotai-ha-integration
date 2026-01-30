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

from .const import DOMAIN
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
    # 1. Polling coordinator (data source)
    # ------------------------------------------------------------------
    coordinator = OpenIOTAIDataCoordinator(hass)
    await coordinator.async_config_entry_first_refresh()

    # ------------------------------------------------------------------
    # 2. Get MQTT exporter created during async_setup_entry
    # ------------------------------------------------------------------
    domain_data = hass.data.get(DOMAIN, {})
    exporter: Optional[OpenIOTAIMQTTExporter] = domain_data.get(entry_id)

    if exporter is None:
        _LOGGER.error(
            "OpenIOTAI MQTT exporter not found (entry_id=%s) – "
            "snapshot export disabled",
            entry_id,
        )
        return

    _LOGGER.info(
        "OpenIOTAI MQTT exporter resolved (entry_id=%s)",
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
            # Do NOT raise – runtime reconnect logic handles recovery
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
