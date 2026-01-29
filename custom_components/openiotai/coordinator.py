"""DataUpdateCoordinator for OpenIOTAI integration."""

from __future__ import annotations

from datetime import timedelta
import logging
import time
from typing import Any, Dict

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = timedelta(seconds=30)


class OpenIOTAIDataCoordinator(DataUpdateCoordinator[Dict[str, Any]]):
    """Coordinator that polls current Home Assistant state snapshots."""

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the coordinator."""
        _LOGGER.info(
            "Initializing OpenIOTAI DataUpdateCoordinator (interval=%s)",
            DEFAULT_POLL_INTERVAL,
        )

        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name=DOMAIN,
            update_interval=DEFAULT_POLL_INTERVAL,
        )

    async def _async_update_data(self) -> Dict[str, Any]:
        """
        Fetch the latest data from Home Assistant.

        This method intentionally avoids the event bus and returns
        a deterministic snapshot of current entity states.
        """
        start_ts = time.monotonic()

        _LOGGER.debug("Starting OpenIOTAI polling cycle")

        try:
            data: Dict[str, Any] = {}

            for state in self.hass.states.async_all():
                data[state.entity_id] = {
                    "state": state.state,
                    "attributes": state.attributes,
                }

            duration = time.monotonic() - start_ts

            _LOGGER.debug(
                "OpenIOTAI polling completed: entities=%d, duration=%.3fs",
                len(data),
                duration,
            )

            # Optional, but VERY useful during development
            if _LOGGER.isEnabledFor(logging.DEBUG) and data:
                sample_keys = list(data.keys())[:3]
                _LOGGER.debug(
                    "OpenIOTAI snapshot sample entity_ids: %s",
                    sample_keys,
                )

            return data

        except Exception as err:
            _LOGGER.exception("OpenIOTAI polling failed")
            raise UpdateFailed(f"Polling failed: {err}") from err
