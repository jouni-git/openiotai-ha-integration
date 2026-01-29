"""Sensor platform for OpenIOTAI integration."""

from __future__ import annotations

import logging

from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry

from .const import DOMAIN
from .coordinator import OpenIOTAIDataCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """
    Set up the OpenIOTAI sensor platform.

    This initializes the polling coordinator but does not create
    Home Assistant sensor entities.
    """
    _LOGGER.info(
        "Setting up OpenIOTAI sensor platform (entry_id=%s)",
        entry.entry_id,
    )

    coordinator = OpenIOTAIDataCoordinator(hass)

    await coordinator.async_config_entry_first_refresh()

    # Store coordinator per config entry
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    _LOGGER.info(
        "OpenIOTAI polling coordinator started (entry_id=%s)",
        entry.entry_id,
    )
