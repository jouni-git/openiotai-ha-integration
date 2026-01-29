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
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name=DOMAIN,
            update_interval=DEFAULT_POLL_INTERVAL,
        )

        _LOGGER.info(
            "Initializing OpenIOTAI DataUpdateCoordinator (interval=%s)",
            DEFAULT_POLL_INTERVAL,
        )

    async def _async_update_data(self) -> Dict[str, Any]:
        try:
            data: Dict[str, Any] = {}

            for state in self.hass.states.async_all():
                data[state.entity_id] = {
                    "state": state.state,
                    "attributes": dict(state.attributes),
                    "last_changed": state.last_changed.isoformat()
                    if state.last_changed
                    else None,
                    "last_updated": state.last_updated.isoformat()
                    if state.last_updated
                    else None,
                }

            _LOGGER.debug(
                "OpenIOTAI polling completed: entities=%d",
                len(data),
            )

            return data

        except Exception as err:
            _LOGGER.exception("OpenIOTAI polling failed")
            raise UpdateFailed(str(err)) from err
