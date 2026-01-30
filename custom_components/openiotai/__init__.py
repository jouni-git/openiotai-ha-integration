"""OpenIOTAI integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady

from .const import (
    DOMAIN,
    CONF_MQTT_BROKER,
    CONF_MQTT_PORT,
    CONF_MQTT_TOPIC,
    CONF_MQTT_TLS,
    CONF_MQTT_CA_CERT,
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
)
from .mqtt_export import OpenIOTAIMQTTExporter

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the OpenIOTAI integration."""
    _LOGGER.info("Initializing OpenIOTAI integration (async_setup)")
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate old config entries to the current version.

    No schema changes required; only bump the version number.
    """
    _LOGGER.info(
        "Migrating OpenIOTAI config entry (entry_id=%s, version=%s → 2)",
        entry.entry_id,
        entry.version,
    )

    if entry.version < 2:
        entry.version = 2

    return True


@callback
def _options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    _LOGGER.info(
        "OpenIOTAI options updated → reloading entry (entry_id=%s)",
        entry.entry_id,
    )
    hass.async_create_task(hass.config_entries.async_reload(entry.entry_id))


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OpenIOTAI from a config entry."""
    _LOGGER.info("Setting up OpenIOTAI config entry (entry_id=%s)", entry.entry_id)

    cfg = entry.options or entry.data

    # --------------------------------------------------------------
    # MQTT SETUP-TIME VALIDATION
    # --------------------------------------------------------------
    try:
        exporter = OpenIOTAIMQTTExporter(
            broker=cfg[CONF_MQTT_BROKER],
            port=cfg[CONF_MQTT_PORT],
            topic=cfg[CONF_MQTT_TOPIC],
            use_tls=cfg.get(CONF_MQTT_TLS, False),
            ca_cert=cfg.get(CONF_MQTT_CA_CERT),
            username=cfg.get(CONF_MQTT_USERNAME),
            password=cfg.get(CONF_MQTT_PASSWORD),
            client_id=f"openiotai-{entry.entry_id}",
        )
    except KeyError as exc:
        _LOGGER.error(
            "Missing required MQTT configuration key %s (entry_id=%s)",
            exc,
            entry.entry_id,
        )
        raise ConfigEntryNotReady("Incomplete MQTT configuration") from exc

    try:
        await exporter.async_test_connection()
    except ConfigEntryNotReady:
        _LOGGER.error("MQTT setup failed during OpenIOTAI setup (entry_id=%s)", entry.entry_id)
        raise
    except Exception as exc:
        _LOGGER.exception("Unexpected error during MQTT setup test (entry_id=%s)", entry.entry_id)
        raise ConfigEntryNotReady(str(exc)) from exc

    _LOGGER.info("MQTT connection verified successfully (entry_id=%s)", entry.entry_id)

    # Reload integration when options change
    entry.async_on_unload(entry.add_update_listener(_options_updated))

    try:
        await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])
    except Exception:
        _LOGGER.exception("Failed to set up OpenIOTAI platforms (entry_id=%s)", entry.entry_id)
        return False

    _LOGGER.info("OpenIOTAI config entry setup completed (entry_id=%s)", entry.entry_id)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload an OpenIOTAI config entry."""
    _LOGGER.info("Unloading OpenIOTAI config entry (entry_id=%s)", entry.entry_id)

    try:
        unloaded = await hass.config_entries.async_forward_entry_unload(entry, "sensor")
    except Exception:
        _LOGGER.exception("Error while unloading OpenIOTAI config entry (entry_id=%s)", entry.entry_id)
        return False

    if unloaded:
        _LOGGER.info("OpenIOTAI config entry unloaded successfully (entry_id=%s)", entry.entry_id)

    return unloaded
