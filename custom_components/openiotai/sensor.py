"""Sensor platform for OpenIOTAI integration.

Initializes polling coordinator and exports snapshots via MQTT.
No Home Assistant sensor entities are created.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Optional

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
        # Absolute safety fallback – should never happen due to validation
        interval = timedelta(seconds=DEFAULT_PUBLISH_INTERVAL)

    _LOGGER.info(
        "OpenIOTAI publish interval set to %s seconds (entry_id=%s)",
        interval.total_seconds(),
        entry_id,
    )

    # ------------------------------------------------------------------
    # 2. Polling coordinator (data source)
    # ------------------------------------------------------------------
    coordinator = OpenIOTAIDataCoordinator(
        hass,
        update_interval=interval,
    )

    await coordinator.async_config_entry_first_refresh()

    # ------------------------------------------------------------------
    # 3. Get MQTT exporter created during async_setup_entry
    # ------------------------------------------------------------------
    domain_data = hass.data.get(DOMAIN, {})
    exporter: Optional[OpenIOTAIMQTTExporter] = domain_data.get(entry_id)

    if exporter is None:
        # This should never happen; log once and disable export
        _LOGGER.error(
            "OpenIOTAI MQTT exporter not found (entry_id=%s) – "
            "snapshot export disabled",
            entry_id,
        )
        return

    _LOGGER.debug(
        "OpenIOTAI MQTT exporter resolved (entry_id=%s)",
        entry_id,
    )

    # ------------------------------------------------------------------
    # 4. Export snapshot after each polling update
    # ------------------------------------------------------------------
    async def _export_after_update() -> None:
        snapshot = coordinator.data or {}

        _LOGGER.debug(
            "Exporting OpenIOTAI snapshot to MQTT "
            "(entities=%d, entry_id=%s)",
            len(snapshot),
            entry_id,
        )

        try:
            await exporter.publish_snapshot(snapshot)

        except CannotConnect:
            # Expected and self-healing condition:
            # - broker temporarily unavailable
            # - reconnect in progress
            # Must NEVER be logged as ERROR
            _LOGGER.debug(
                "OpenIOTAI MQTT export skipped "
                "(connect in progress, entry_id=%s)",
                entry_id,
            )

        except asyncio.CancelledError:
            # Normal during reload / shutdown
            raise

        except Exception as err:
            # This indicates a real bug
            _LOGGER.error(
                "Unexpected OpenIOTAI MQTT export error "
                "(entry_id=%s): %s",
                entry_id,
                err,
                exc_info=True,
            )

    coordinator.async_add_listener(
        lambda: hass.async_create_task(_export_after_update())
    )

    _LOGGER.info(
        "OpenIOTAI MQTT export pipeline activated "
        "(interval=%ss, entry_id=%s)",
        int(interval.total_seconds()),
        entry_id,
    )
