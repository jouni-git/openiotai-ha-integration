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

from .const import DOMAIN, DEFAULT_PUBLISH_INTERVAL

_LOGGER = logging.getLogger(__name__)


class OpenIOTAIDataCoordinator(DataUpdateCoordinator[Dict[str, Any]]):
    """Coordinator that polls current Home Assistant state snapshots."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        interval_seconds: int | None = None,
    ) -> None:
        if interval_seconds is None:
            interval_seconds = DEFAULT_PUBLISH_INTERVAL

        update_interval = timedelta(seconds=int(interval_seconds))

        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name=DOMAIN,
            update_interval=update_interval,
        )

        _LOGGER.info(
            "Initializing OpenIOTAI DataUpdateCoordinator (interval=%ss)",
            update_interval.total_seconds(),
        )

    # -----------------------------------------------------------------
    # Runtime control
    # -----------------------------------------------------------------
    def set_interval(self, seconds: int) -> None:
        """Update polling interval at runtime (no reload required)."""
        try:
            seconds = int(seconds)
            if seconds <= 0:
                raise ValueError
        except Exception:
            _LOGGER.warning(
                "Invalid poll interval requested (%r) – ignored",
                seconds,
            )
            return

        new_interval = timedelta(seconds=seconds)

        if self.update_interval == new_interval:
            return  # no-op

        self.update_interval = new_interval

        _LOGGER.info(
            "OpenIOTAI polling interval updated to %ss",
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

            _LOGGER.debug(
                "OpenIOTAI polling completed: entities=%d (interval=%ss)",
                len(data),
                self.update_interval.total_seconds()
                if self.update_interval
                else None,
            )

            return data

        except Exception as err:
            _LOGGER.exception("OpenIOTAI polling failed")
            raise UpdateFailed(str(err)) from err
