"""OpenIOTAI integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

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

SENSITIVE_KEYS = {
    CONF_MQTT_PASSWORD,
    CONF_MQTT_CA_CERT,
    "password",
    "token",
    "api_key",
}


# ---------------------------------------------------------------------
# Base setup
# ---------------------------------------------------------------------
async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up OpenIOTAI integration base."""
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("coordinators", {})
    return True


def _sanitize_cfg(cfg: dict) -> dict:
    """Mask sensitive values for safe logging."""
    return {k: ("***" if k in SENSITIVE_KEYS and v else v) for k, v in cfg.items()}


async def _maybe_create_exporter(hass: HomeAssistant, entry: ConfigEntry, cfg: dict) -> bool:
    """Create exporter if missing and config is complete.

    Returns True if a NEW exporter was created (so caller can reload entry once).
    """
    entry_id = entry.entry_id
    domain_data = hass.data.setdefault(DOMAIN, {})
    exporter: OpenIOTAIMQTTExporter | None = domain_data.get(entry_id)

    mqtt_required = (CONF_MQTT_BROKER, CONF_MQTT_PORT, CONF_MQTT_TOPIC)
    if exporter is not None:
        return False

    if not all(k in cfg for k in mqtt_required):
        _LOGGER.info(
            "OpenIOTAI MQTT exporter not started – configuration incomplete (entry_id=%s)",
            entry_id,
        )
        return False

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

    domain_data[entry_id] = exporter
    await exporter.async_start()
    return True


async def _options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Apply updated options at runtime without reloading the integration."""
    entry_id = entry.entry_id
    cfg = {**entry.data, **entry.options}

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

    domain_data = hass.data.setdefault(DOMAIN, {})
    exporter: OpenIOTAIMQTTExporter | None = domain_data.get(entry_id)

    # 1) Update existing exporter OR create if missing
    created = False
    if exporter:
        try:
            exporter.update_options(cfg)
            _LOGGER.info("OpenIOTAI MQTT runtime options applied (entry_id=%s)", entry_id)
        except Exception as err:
            _LOGGER.error("Failed to apply MQTT options (entry_id=%s): %s", entry_id, err)
    else:
        created = await _maybe_create_exporter(hass, entry, cfg)

    # If exporter was created after initial setup, reload once so binary_sensor.py gets added
    # (binary_sensor platform bails out if exporter was missing during first setup)
    if created:
        _LOGGER.info(
            "OpenIOTAI exporter created after initial setup -> reloading entry once "
            "to expose MQTT status entities (entry_id=%s)",
            entry_id,
        )
        hass.async_create_task(hass.config_entries.async_reload(entry_id))

    # 2) Update polling interval (coordinator)
    coordinator = hass.data.get(DOMAIN, {}).get("coordinators", {}).get(entry_id)
    if not coordinator:
        return

    try:
        interval_sec = int(cfg.get(CONF_PUBLISH_INTERVAL, DEFAULT_PUBLISH_INTERVAL))
        if interval_sec <= 0:
            raise ValueError
    except Exception:
        interval_sec = DEFAULT_PUBLISH_INTERVAL

    coordinator.set_interval(interval_sec)
    coordinator._schedule_refresh()  # pylint: disable=protected-access


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OpenIOTAI from a config entry."""
    entry_id = entry.entry_id
    _LOGGER.info("Setting up OpenIOTAI (entry_id=%s)", entry_id)

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("coordinators", {})

    cfg = {**entry.data, **entry.options}

    # Create exporter if config is already complete at startup
    await _maybe_create_exporter(hass, entry, cfg)

    # Options listener
    entry.async_on_unload(entry.add_update_listener(_options_updated))

    # Forward platforms (sensor creates coordinator; binary_sensor shows MQTT status if exporter exists)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload OpenIOTAI integration."""
    entry_id = entry.entry_id
    unload_ok = True

    for platform in PLATFORMS:
        unload_ok &= await hass.config_entries.async_forward_entry_unload(entry, platform)

    exporter: OpenIOTAIMQTTExporter | None = hass.data.get(DOMAIN, {}).pop(entry_id, None)
    if exporter:
        try:
            await exporter.async_stop()
        except Exception:
            pass

    hass.data.get(DOMAIN, {}).get("coordinators", {}).pop(entry_id, None)
    return unload_ok
