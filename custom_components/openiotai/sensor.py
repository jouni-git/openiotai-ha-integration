"""Sensor platform for OpenIOTAI integration.

Initializes polling coordinator and exports snapshots via MQTT.
No Home Assistant sensor entities are created.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry

from .const import DOMAIN
from .coordinator import OpenIOTAIDataCoordinator
from .mqtt_export import OpenIOTAIMQTTExporter

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Snapshot delta helper (optional, safe to keep)
# ---------------------------------------------------------------------
class SnapshotDelta:
    """Computes delta between successive snapshots."""

    def __init__(self) -> None:
        self._last: Dict[str, Any] | None = None

    def compute(self, current: Dict[str, Any]) -> Dict[str, Any]:
        if self._last is None:
            self._last = current
            return current

        delta: Dict[str, Any] = {
            k: v for k, v in current.items()
            if self._last.get(k) != v
        }

        self._last = current
        return delta


# ---------------------------------------------------------------------
# Platform setup
# ---------------------------------------------------------------------
async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    entry_id = entry.entry_id

    _LOGGER.info(
        "Setting up OpenIOTAI sensor platform (entry_id=%s)",
        entry_id,
    )

    # ---------------------------------------------------------
    # 1. Create coordinator (poll interval set elsewhere!)
    # ---------------------------------------------------------
    coordinator = OpenIOTAIDataCoordinator(hass)

    # 🔑 TÄRKEÄÄ:
    # Pollausväli asetetaan ja päivitetään __init__.py:ssä
    # Options listener hoitaa runtime-muutokset

    await coordinator.async_config_entry_first_refresh()

    # Store coordinator for runtime updates
    hass.data[DOMAIN]["coordinators"][entry_id] = coordinator

    # ---------------------------------------------------------
    # 2. Get MQTT exporter
    # ---------------------------------------------------------
    exporter: OpenIOTAIMQTTExporter | None = hass.data[DOMAIN].get(entry_id)
    if not exporter:
        _LOGGER.error(
            "MQTT exporter missing (entry_id=%s) – export disabled",
            entry_id,
        )
        return

    # ---------------------------------------------------------
    # 3. Publish snapshot after each poll
    # ---------------------------------------------------------
    delta = SnapshotDelta()

    async def _export_after_update() -> None:
        snapshot = coordinator.data or {}
        payload = delta.compute(snapshot)

        if not payload:
            return  # No changes → nothing to publish

        try:
            await exporter.publish_snapshot(payload)
        except Exception as err:
            _LOGGER.debug(
                "MQTT publish skipped/failed (entry_id=%s): %s",
                entry_id,
                err,
            )

    coordinator.async_add_listener(
        lambda: hass.async_create_task(_export_after_update())
    )

    _LOGGER.info(
        "OpenIOTAI poll→publish pipeline active (entry_id=%s)",
        entry_id,
    )
