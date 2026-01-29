"""OpenIOTAI integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the OpenIOTAI integration."""
    _LOGGER.info("Initializing OpenIOTAI integration (async_setup)")
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OpenIOTAI from a config entry."""
    _LOGGER.info(
        "Setting up OpenIOTAI config entry (entry_id=%s)",
        entry.entry_id,
    )

    try:
        # Forward setup to supported platforms
        await hass.config_entries.async_forward_entry_setups(
            entry,
            ["sensor"],
        )
    except Exception:
        _LOGGER.exception(
            "Failed to set up OpenIOTAI platforms for entry_id=%s",
            entry.entry_id,
        )
        return False

    _LOGGER.info(
        "OpenIOTAI config entry setup completed (entry_id=%s)",
        entry.entry_id,
    )
    return True


async def async_setup_entry_OLD(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OpenIOTAI from a config entry."""
    _LOGGER.info("Setting up OpenIOTAI config entry")

    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])
    return True



async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload an OpenIOTAI config entry."""
    _LOGGER.info(
        "Unloading OpenIOTAI config entry (entry_id=%s)",
        entry.entry_id,
    )

    try:
        unloaded = await hass.config_entries.async_forward_entry_unload(
            entry,
            "sensor",
        )
    except Exception:
        _LOGGER.exception(
            "Error while unloading OpenIOTAI config entry (entry_id=%s)",
            entry.entry_id,
        )
        return False

    if unloaded:
        _LOGGER.info(
            "OpenIOTAI config entry unloaded successfully (entry_id=%s)",
            entry.entry_id,
        )
    else:
        _LOGGER.warning(
            "OpenIOTAI config entry unload reported failure (entry_id=%s)",
            entry.entry_id,
        )

    return unloaded


async def async_unload_entry_OLD(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info("Unloading OpenIOTAI config entry")

    return await hass.config_entries.async_forward_entry_unload(entry, "sensor")
