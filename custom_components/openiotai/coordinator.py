"""DataUpdateCoordinator for OpenIOTAI integration."""

from __future__ import annotations

from datetime import timedelta
import logging
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
        try:
            data: Dict[str, Any] = {}

            # Iterate over all current states in Home Assistant
            for state in self.hass.states.async_all():
                data[state.entity_id] = {
                    "state": state.state,
                    "attributes": state.attributes,
                }

            _LOGGER.debug(
                "OpenIOTAI polling snapshot collected (%d entities)",
                len(data),
            )

            return data

        except Exception as err:
            # Any exception here marks the update as failed
            raise UpdateFailed(f"Polling failed: {err}") from err
