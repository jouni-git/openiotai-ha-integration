"""DataUpdateCoordinator for OpenIOTAI integration."""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from typing import Any, Dict

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)
from homeassistant.util.json import JSONEncoder

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

        Returns a JSON-serializable snapshot of current entity states.
        """
        try:
            _LOGGER.debug("Starting OpenIOTAI polling cycle")

            data: Dict[str, Any] = {}

            for state in self.hass.states.async_all():
                # Normalize attributes to JSON-compatible types
                attributes = json.loads(
                    json.dumps(state.attributes, cls=JSONEncoder)
                )

                data[state.entity_id] = {
                    "state": state.state,
                    "attributes": attributes,
                }

            _LOGGER.debug(
                "OpenIOTAI polling completed: entities=%d",
                len(data),
            )

            if _LOGGER.isEnabledFor(logging.DEBUG):
                _LOGGER.debug(
                    "OpenIOTAI snapshot sample entity_ids: %s",
                    list(data.keys())[:3],
                )

            return data

        except Exception as err:
            _LOGGER.exception("OpenIOTAI polling failed")
            raise UpdateFailed(f"Polling failed: {err}") from err
