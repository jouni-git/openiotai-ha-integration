"""Sensor platform for OpenIOTAI integration.

Initializes polling coordinator and exports snapshots via MQTT.
No Home Assistant sensor entities are created.
"""

from __future__ import annotations

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
# Snapshot delta helper
# ---------------------------------------------------------------------
class SnapshotDelta:
    """Compute delta between successive snapshots."""

    def __init__(self) -> None:
        self._last: Dict[str, Any] | None = None

    def compute(self, current: Dict[str, Any]) -> Dict[str, Any]:
        if self._last is None:
            self._last = current
            return current

        delta = {
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
    """Set up OpenIOTAI polling + MQTT export pipeline."""
    entry_id = entry.entry_id

    _LOGGER.info(
        "Setting up OpenIOTAI sensor platform (entry_id=%s)",
        entry_id,
    )

    # -----------------------------------------------------------------
    # 1. Create polling coordinator
    # -----------------------------------------------------------------
    coordinator = OpenIOTAIDataCoordinator(hass)

    # NOTE:
    # - update_interval is set in coordinator __init__
    # - runtime changes are applied via __init__.py options listener
    await coordinator.async_config_entry_first_refresh()

    # Register coordinator for runtime access (CRITICAL)
    hass.data[DOMAIN]["coordinators"][entry_id] = coordinator

    _LOGGER.info(
        "OpenIOTAI coordinator registered (entry_id=%s)",
        entry_id,
    )

    # -----------------------------------------------------------------
    # 2. Publish snapshot delta after each poll
    # -----------------------------------------------------------------
    delta = SnapshotDelta()

    async def _export_after_update() -> None:
        snapshot = coordinator.data or {}
        payload = delta.compute(snapshot)

        if not payload:
            return  # No changes → nothing to publish

        exporter: OpenIOTAIMQTTExporter | None = hass.data[DOMAIN].get(entry_id)
        if not exporter:
            _LOGGER.info(
                "OpenIOTAI MQTT exporter not ready yet (entry_id=%s) – waiting",
                entry_id,
            )
            return

        try:
            await exporter.publish_snapshot(payload)
        except Exception as err:
            _LOGGER.debug(
                "OpenIOTAI MQTT publish failed (entry_id=%s): %s",
                entry_id,
                err,
            )

    coordinator.async_add_listener(
        lambda: hass.async_create_task(_export_after_update())
    )

    _LOGGER.info(
        "OpenIOTAI poll → MQTT publish pipeline active (entry_id=%s)",
        entry_id,
    )
