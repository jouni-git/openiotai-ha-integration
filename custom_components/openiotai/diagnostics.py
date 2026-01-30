# custom_components/openiotai/diagnostics.py
from __future__ import annotations

from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry

from .const import DOMAIN


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict:
    """Return diagnostics for an OpenIOTAI config entry.

    This is intended for advanced troubleshooting only.
    No secrets (passwords, tokens) are exposed.
    """
    domain_data = hass.data.get(DOMAIN, {})
    exporter = domain_data.get(entry.entry_id)

    if exporter is None:
        # Integration not fully set up or already unloaded
        return {
            "mqtt": {
                "connected": False,
                "status": "exporter_not_initialized",
            }
        }

    return {
        "mqtt": {
            "connected": exporter.connected,
            "last_error": exporter.last_error,
            "last_connect_attempt": exporter.last_connect_attempt,
            "broker": exporter.broker,
            "port": exporter.port,
            "tls": exporter.tls,
            "topic": exporter.topic,
        }
    }
