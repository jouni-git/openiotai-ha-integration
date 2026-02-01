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
# Options update listener (NO reload, secure logging)
# ---------------------------------------------------------------------
SENSITIVE_KEYS = {
    "mqtt_password",
    "password",
    "token",
    "api_key",
    "mqtt_ca_cert",
}

def _sanitize_cfg(cfg: dict) -> dict:
    """Mask sensitive values for safe logging."""
    return {
        k: ("***" if k in SENSITIVE_KEYS and v else v)
        for k, v in cfg.items()
    }


async def _options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Apply updated options at runtime without reloading the integration."""
    entry_id = entry.entry_id

    # Merge config entry data + options (options override data)
    cfg = {**entry.data, **entry.options}

    # ------------------------------------------------------------
    # Log options update (safe)
    # ------------------------------------------------------------
    _LOGGER.info(
        "OpenIOTAI options updated (entry_id=%s): publish_interval=%ss",
        entry_id,
        cfg.get(CONF_PUBLISH_INTERVAL),
    )

    _LOGGER.debug(
        "OpenIOTAI options full config (masked, entry_id=%s): %s",
        entry_id,
        _sanitize_cfg(cfg),
    )

    # ------------------------------------------------------------
    # 1. Ensure MQTT exporter exists (create if needed)
    # ------------------------------------------------------------
    exporter: OpenIOTAIMQTTExporter | None = hass.data[DOMAIN].get(entry_id)

    mqtt_required = (
        CONF_MQTT_BROKER,
        CONF_MQTT_PORT,
        CONF_MQTT_TOPIC,
    )

    if not exporter:
        if all(k in cfg for k in mqtt_required):
            _LOGGER.info(
                "OpenIOTAI MQTT configuration complete – creating exporter (entry_id=%s)",
                entry_id,
            )

            exporter = OpenIOTAIMQTTExporter(
                broker=cfg[CONF_MQTT_BROKER],
                port=cfg[CONF_MQTT_PORT],
                topic=cfg[CONF_MQTT_TOPIC],
                use_tls=cfg.get(CONF_MQTT_TLS, False),
                ca_cert=cfg.get(CONF_MQTT_CA_CERT),
                username=cfg.get(CONF_MQTT_USERNAME),
                password=cfg.get(CONF_MQTT_PASSWORD),
                client_id=f"openiotai-{entry_id}",
            )

            hass.data[DOMAIN][entry_id] = exporter
            hass.async_create_task(exporter.async_start())
        else:
            _LOGGER.info(
                "OpenIOTAI MQTT exporter not started – configuration incomplete (entry_id=%s)",
                entry_id,
            )
    else:
        try:
            exporter.update_options(cfg)
            _LOGGER.info(
                "OpenIOTAI MQTT runtime options applied (entry_id=%s)",
                entry_id,
            )
        except Exception as err:
            _LOGGER.error(
                "Failed to apply MQTT options (entry_id=%s): %s",
                entry_id,
                err,
            )

    # ------------------------------------------------------------
    # 2. Update polling interval (DataUpdateCoordinator)
    # ------------------------------------------------------------
    try:
        interval_sec = int(
            cfg.get(CONF_PUBLISH_INTERVAL, DEFAULT_PUBLISH_INTERVAL)
        )
        if interval_sec <= 0:
            raise ValueError
    except Exception:
        _LOGGER.warning(
            "Invalid publish_interval in options (entry_id=%s), "
            "falling back to default=%s",
            entry_id,
            DEFAULT_PUBLISH_INTERVAL,
        )
        interval_sec = DEFAULT_PUBLISH_INTERVAL

    coordinators = hass.data.get(DOMAIN, {}).get("coordinators", {})
    coordinator = coordinators.get(entry_id)

    if not coordinator:
        _LOGGER.warning(
            "OpenIOTAI coordinator not found – polling interval not updated "
            "(entry_id=%s)",
            entry_id,
        )
        return

    old_interval = (
        coordinator.update_interval.total_seconds()
        if coordinator.update_interval
        else None
    )

    coordinator.set_interval(interval_sec)
    coordinator._schedule_refresh()  # pylint: disable=protected-access

    _LOGGER.info(
        "OpenIOTAI polling interval updated (entry_id=%s): %s → %ss",
        entry_id,
        old_interval,
        interval_sec,
    )


# ---------------------------------------------------------------------
# Entry setup
# ---------------------------------------------------------------------
async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OpenIOTAI from a config entry."""
    _LOGGER.info("Setting up OpenIOTAI (entry_id=%s)", entry.entry_id)

    # Merge data + options (options override)
    cfg = {**entry.data, **entry.options}

    required = (
        CONF_MQTT_BROKER,
        CONF_MQTT_PORT,
        CONF_MQTT_TOPIC,
    )

    if not all(k in cfg for k in required):
        _LOGGER.info(
            "OpenIOTAI MQTT not started yet – configuration incomplete "
            "(configure options to enable MQTT)"
        )
        # Still forward platforms (coordinator works without MQTT)
        await hass.config_entries.async_forward_entry_setups(
            entry,
            PLATFORMS,
        )
        return True

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

    hass.data[DOMAIN][entry.entry_id] = exporter
    hass.async_create_task(exporter.async_start())

    entry.async_on_unload(
        entry.add_update_listener(_options_updated)
    )

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
