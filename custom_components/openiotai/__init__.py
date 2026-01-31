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
# Base setup
# ---------------------------------------------------------------------
async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up OpenIOTAI integration base."""
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("coordinators", {})
    return True


# ---------------------------------------------------------------------
# Options update listener (NO reload)
# ---------------------------------------------------------------------
async def _options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Apply updated options at runtime without reloading the integration."""
    entry_id = entry.entry_id

    # Options override data (HA best practice for merged runtime config)
    cfg = {**entry.data, **entry.options}

    _LOGGER.warning(
        "OpenIOTAI OPTIONS LISTENER FIRED (entry_id=%s) data=%s options=%s merged=%s",
        entry_id,
        dict(entry.data),
        dict(entry.options),
        cfg,
    )

    # ------------------------------------------------------------
    # 1) Update MQTT exporter options
    # ------------------------------------------------------------
    exporter: OpenIOTAIMQTTExporter | None = hass.data[DOMAIN].get(entry_id)
    if exporter:
        try:
            exporter.update_options(cfg)
            _LOGGER.warning("OpenIOTAI MQTT options applied (entry_id=%s)", entry_id)
        except Exception as err:
            _LOGGER.exception("Failed to apply MQTT options (entry_id=%s): %s", entry_id, err)
    else:
        _LOGGER.warning("OpenIOTAI MQTT exporter NOT found (entry_id=%s)", entry_id)

    # ------------------------------------------------------------
    # 2) Resolve polling interval from cfg
    # ------------------------------------------------------------
    # Primary key
    interval_raw = cfg.get(CONF_PUBLISH_INTERVAL)

    # Safety fallbacks in case OptionsFlow uses a different key name
    if interval_raw is None:
        interval_raw = (
            cfg.get("publish_interval")
            or cfg.get("poll_interval")
            or cfg.get("polling_interval")
        )

    try:
        interval_sec = int(interval_raw) if interval_raw is not None else int(DEFAULT_PUBLISH_INTERVAL)
        if interval_sec <= 0:
            raise ValueError
    except Exception:
        _LOGGER.warning(
            "Invalid interval in options (entry_id=%s): raw=%r -> fallback=%s",
            entry_id,
            interval_raw,
            DEFAULT_PUBLISH_INTERVAL,
        )
        interval_sec = int(DEFAULT_PUBLISH_INTERVAL)

    # ------------------------------------------------------------
    # 3) Update coordinator interval + reschedule
    # ------------------------------------------------------------
    coordinators = hass.data.get(DOMAIN, {}).get("coordinators", {})
    coordinator = coordinators.get(entry_id)

    if not coordinator:
        _LOGGER.warning(
            "OpenIOTAI coordinator NOT found (entry_id=%s). Known keys=%s",
            entry_id,
            list(coordinators.keys()),
        )
        return

    old = coordinator.update_interval.total_seconds() if coordinator.update_interval else None

    # Use coordinator API
    try:
        coordinator.set_interval(interval_sec)
    except Exception as err:
        _LOGGER.exception("Failed to set coordinator interval (entry_id=%s): %s", entry_id, err)
        return

    new = coordinator.update_interval.total_seconds() if coordinator.update_interval else None

    # Reschedule next refresh using the new interval
    coordinator._schedule_refresh()  # pylint: disable=protected-access

    _LOGGER.warning(
        "OpenIOTAI polling interval updated (entry_id=%s): %s -> %s seconds",
        entry_id,
        old,
        new,
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

    # Store exporter for runtime access
    hass.data[DOMAIN][entry.entry_id] = exporter

    # Start MQTT runtime (non-blocking)
    hass.async_create_task(exporter.async_start())

    # Register options update listener (NO reload)
    entry.async_on_unload(
        entry.add_update_listener(_options_updated)
    )

    # Forward platforms (sensor.py creates & registers coordinator)
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
                "OpenIOTAI MQTT stop failed (ignored, entry_id=%s): %s",
                entry_id,
                err,
            )

    # Remove coordinator reference
    hass.data[DOMAIN]["coordinators"].pop(entry_id, None)

    return unload_ok
