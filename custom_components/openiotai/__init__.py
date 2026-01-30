"""OpenIOTAI integration."""

from __future__ import annotations

import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
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
    CONF_PUBLISH_INTERVAL,
    DEFAULT_PUBLISH_INTERVAL,
)
from .mqtt_export import OpenIOTAIMQTTExporter

_LOGGER = logging.getLogger(__name__)

PLATFORMS: tuple[str, ...] = ("sensor", "binary_sensor")


# ---------------------------------------------------------------------
# Integration setup (YAML not used)
# ---------------------------------------------------------------------
async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up OpenIOTAI base structures."""
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("coordinators", {})
    return True


# ---------------------------------------------------------------------
# Options update listener (NO reload)
# ---------------------------------------------------------------------
async def _options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Apply updated options to runtime without reloading integration."""
    entry_id = entry.entry_id
    cfg = entry.options or entry.data

    # ------------------------------------------------------------
    # 1. Update MQTT exporter options
    # ------------------------------------------------------------
    exporter: OpenIOTAIMQTTExporter | None = hass.data[DOMAIN].get(entry_id)
    if exporter:
        try:
            exporter.update_options(cfg)
            _LOGGER.debug(
                "OpenIOTAI MQTT options applied to runtime (entry_id=%s)",
                entry_id,
            )
        except Exception as err:
            _LOGGER.error(
                "Failed to apply MQTT options (entry_id=%s): %s",
                entry_id,
                err,
            )

    # ------------------------------------------------------------
    # 2. Update polling interval at runtime
    # ------------------------------------------------------------
    try:
        interval_sec = int(
            cfg.get(CONF_PUBLISH_INTERVAL, DEFAULT_PUBLISH_INTERVAL)
        )
    except Exception:
        interval_sec = DEFAULT_PUBLISH_INTERVAL

    coordinator = hass.data[DOMAIN]["coordinators"].get(entry_id)
    if not coordinator:
        return

    coordinator.update_interval = timedelta(seconds=interval_sec)

    # Force reschedule using new interval
    await coordinator.async_request_refresh()

    _LOGGER.debug(
        "OpenIOTAI polling interval updated at runtime → %ss (entry_id=%s)",
        interval_sec,
        entry_id,
    )


# ---------------------------------------------------------------------
# Entry setup
# ---------------------------------------------------------------------
async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OpenIOTAI from a config entry."""
    _LOGGER.info("Setting up OpenIOTAI (entry_id=%s)", entry.entry_id)

    cfg = entry.options or entry.data

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
        raise ConfigEntryNotReady(
            "Incomplete MQTT configuration"
        ) from exc

    # Store exporter
    hass.data[DOMAIN][entry.entry_id] = exporter

    # Start MQTT runtime (non-blocking)
    hass.async_create_task(exporter.async_start())

    # Register options update listener (NO reload)
    entry.async_on_unload(
        entry.add_update_listener(_options_updated)
    )

    # Forward platforms
    await hass.config_entries.async_forward_entry_setups(
        entry,
        PLATFORMS,
    )

    return True


# ---------------------------------------------------------------------
# Entry unload
# ---------------------------------------------------------------------
async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload OpenIOTAI integration."""
    entry_id = entry.entry_id
    unload_ok = True

    # Unload platforms
    for platform in PLATFORMS:
        unload_ok &= await hass.config_entries.async_forward_entry_unload(
            entry, platform
        )

    # Stop MQTT runtime
    exporter: OpenIOTAIMQTTExporter | None = hass.data[DOMAIN].pop(entry_id, None)
    if exporter:
        try:
            await exporter.async_stop()
        except Exception as err:
            _LOGGER.debug(
                "OpenIOTAI MQTT runtime stop failed "
                "(ignored, entry_id=%s): %s",
                entry_id,
                err,
            )

    # Remove coordinator reference
    hass.data[DOMAIN]["coordinators"].pop(entry_id, None)

    return unload_ok
