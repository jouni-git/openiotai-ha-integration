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
    hass.data.setdefault(DOMAIN, {})
    return True


@callback
def _options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload integration when options change."""
    hass.async_create_task(
        hass.config_entries.async_reload(entry.entry_id)
    )


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
        raise ConfigEntryNotReady("Incomplete MQTT configuration") from exc

    # ------------------------------------------------------------------
    # Store exporter for sensor / diagnostics / binary_sensor
    # ------------------------------------------------------------------
    hass.data[DOMAIN][entry.entry_id] = exporter

    # ------------------------------------------------------------------
    # 🔥 START MQTT RUNTIME (CRITICAL)
    # ------------------------------------------------------------------
    # This starts background connection management and allows
    # exporter.connected to ever become True.
    hass.async_create_task(exporter.async_start())

    # ------------------------------------------------------------------
    # Reload integration when options change
    # ------------------------------------------------------------------
    entry.async_on_unload(
        entry.add_update_listener(_options_updated)
    )

    # ------------------------------------------------------------------
    # Forward platforms
    # ------------------------------------------------------------------
    await hass.config_entries.async_forward_entry_setups(
        entry, ["sensor", "binary_sensor"]
    )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload OpenIOTAI."""
    unload_ok = await hass.config_entries.async_forward_entry_unload(
        entry, ["sensor", "binary_sensor"]
    )

    if unload_ok:
        exporter = hass.data[DOMAIN].pop(entry.entry_id, None)
        if exporter:
            await exporter.async_stop()

    return unload_ok
