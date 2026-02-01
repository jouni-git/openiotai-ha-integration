"""DataUpdateCoordinator for OpenIOTAI integration.

Responsible only for polling Home Assistant state snapshots.
Polling interval is controlled at runtime via __init__.py options listener.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)

from .const import DOMAIN, DEFAULT_PUBLISH_INTERVAL

_LOGGER = logging.getLogger(__name__)


class OpenIOTAIDataCoordinator(DataUpdateCoordinator[Dict[str, Any]]):
    """Coordinator that polls Home Assistant state snapshots."""

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize coordinator with default polling interval."""
        update_interval = timedelta(seconds=DEFAULT_PUBLISH_INTERVAL)

        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name=f"{DOMAIN}_coordinator",
            update_interval=update_interval,
        )


        self._first_poll_logged = False  # 👈 uusi lippu
        
        _LOGGER.info(
            "OpenIOTAI DataUpdateCoordinator initialized (interval=%ss)",
            update_interval.total_seconds(),
        )

    # -----------------------------------------------------------------
    # Runtime control (called from __init__.py)
    # -----------------------------------------------------------------
    def set_interval(self, seconds: int) -> None:
        """Update polling interval at runtime (no reload required)."""
        try:
            seconds = int(seconds)
            if seconds <= 0:
                raise ValueError
        except Exception:
            _LOGGER.warning(
                "Invalid OpenIOTAI polling interval requested (%r) – ignored",
                seconds,
            )
            return

        new_interval = timedelta(seconds=seconds)

        if self.update_interval == new_interval:
            return  # no change

        self.update_interval = new_interval

        _LOGGER.info(
            "OpenIOTAI polling interval updated → %ss",
            seconds,
        )

    # -----------------------------------------------------------------
    # Data collection
    # -----------------------------------------------------------------
    async def _async_update_data(self) -> Dict[str, Any]:
        try:
            data: Dict[str, Any] = {}

            for state in self.hass.states.async_all():
                data[state.entity_id] = {
                    "state": state.state,
                    "attributes": dict(state.attributes),
                    "last_changed": (
                        state.last_changed.isoformat()
                        if state.last_changed
                        else None
                    ),
                    "last_updated": (
                        state.last_updated.isoformat()
                        if state.last_updated
                        else None
                    ),
                }

            if not self._first_poll_logged:
                _LOGGER.info(
                    "OpenIOTAI initial polling successful (entities=%d)",
                    len(data),
                )
                self._first_poll_logged = True

            _LOGGER.debug(
                "OpenIOTAI polling completed: entities=%d (interval=%ss)",
                len(data),
                self.update_interval.total_seconds(),
            )

            return data

        except Exception as err:
            _LOGGER.exception("OpenIOTAI polling failed")
            raise UpdateFailed(str(err)) from err
