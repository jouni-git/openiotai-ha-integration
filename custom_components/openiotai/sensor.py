"""Sensor platform for OpenIOTAI integration.

Initializes polling coordinator and exports snapshots via MQTT.
No Home Assistant sensor entities are created.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry

from .const import (
    DOMAIN,
    CONF_PUBLISH_INTERVAL,
    DEFAULT_PUBLISH_INTERVAL,
)
from .coordinator import OpenIOTAIDataCoordinator
from .mqtt_export import OpenIOTAIMQTTExporter, CannotConnect

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Snapshot delta helper
# ---------------------------------------------------------------------
class SnapshotDelta:
    """Computes delta between successive snapshots."""

    def __init__(self) -> None:
        self._last: Dict[str, Any] | None = None

    def compute(self, current: Dict[str, Any]) -> Dict[str, Any]:
        # First publish → full snapshot
        if self._last is None:
            self._last = current
            return current

        delta: Dict[str, Any] = {}

        for key, value in current.items():
            if self._last.get(key) != value:
                delta[key] = value

        self._last = current
        return delta


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
    # 1. Resolve publish interval (seconds)
    # ------------------------------------------------------------------
    cfg = entry.options or entry.data

    interval_sec = cfg.get(
        CONF_PUBLISH_INTERVAL,
        DEFAULT_PUBLISH_INTERVAL,
    )

    try:
        interval = timedelta(seconds=int(interval_sec))
    except Exception:
        interval = timedelta(seconds=DEFAULT_PUBLISH_INTERVAL)

    _LOGGER.info(
        "OpenIOTAI publish interval set to %s seconds (entry_id=%s)",
        int(interval.total_seconds()),
        entry_id,
    )

    # ------------------------------------------------------------------
    # 2. Polling coordinator (data source)
    # ------------------------------------------------------------------
    coordinator = OpenIOTAIDataCoordinator(hass)

    # ⚠️ Important: set interval AFTER construction
    coordinator.update_interval = interval

    await coordinator.async_config_entry_first_refresh()

    # ------------------------------------------------------------------
    # 3. Get MQTT exporter created during async_setup_entry
    # ------------------------------------------------------------------
    exporter: Optional[OpenIOTAIMQTTExporter] = hass.data.get(DOMAIN, {}).get(entry_id)

    if exporter is None:
        _LOGGER.error(
            "OpenIOTAI MQTT exporter not found (entry_id=%s) – export disabled",
            entry_id,
        )
        return

    # ------------------------------------------------------------------
    # 4. Delta computation state
    # ------------------------------------------------------------------
    delta_builder = SnapshotDelta()
    first_publish = True

    # ------------------------------------------------------------------
    # 5. Export after each polling update
    # ------------------------------------------------------------------
    async def _export_after_update() -> None:
        nonlocal first_publish

        snapshot = coordinator.data or {}

        # Compute delta
        delta = delta_builder.compute(snapshot)

        if not delta:
            _LOGGER.debug(
                "OpenIOTAI delta empty → skip publish (entry_id=%s)",
                entry_id,
            )
            return

        payload = {
            "_type": "full" if first_publish else "delta",
            "_ts": datetime.utcnow().isoformat(),
            "data": delta,
        }

        try:
            await exporter.publish_snapshot(payload)
            first_publish = False

        except CannotConnect:
            # Expected transient condition
            _LOGGER.debug(
                "OpenIOTAI MQTT export skipped (connect in progress, entry_id=%s)",
                entry_id,
            )

        except asyncio.CancelledError:
            raise

        except Exception as err:
            _LOGGER.error(
                "Unexpected OpenIOTAI MQTT export error (entry_id=%s): %s",
                entry_id,
                err,
                exc_info=True,
            )

    coordinator.async_add_listener(
        lambda: hass.async_create_task(_export_after_update())
    )

    _LOGGER.info(
        "OpenIOTAI MQTT delta export pipeline activated "
        "(interval=%ss, entry_id=%s)",
        int(interval.total_seconds()),
        entry_id,
    )
