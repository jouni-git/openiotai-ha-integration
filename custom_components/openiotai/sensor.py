"""Sensor platform for OpenIOTAI integration."""

from __future__ import annotations

import logging

from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import OpenIOTAIDataCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """
    Set up the OpenIOTAI integration.

    This function initializes the DataUpdateCoordinator and starts polling.
    No Home Assistant sensor entities are created at this stage.
    """
    _LOGGER.info("Setting up OpenIOTAI integration")

    coordinator = OpenIOTAIDataCoordinator(hass)

    # Perform the first refresh immediately
    await coordinator.async_config_entry_first_refresh()

    # Store coordinator for later use (e.g. MQTT publishing)
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN]["coordinator"] = coordinator

    _LOGGER.info("OpenIOTAI polling coordinator started")
