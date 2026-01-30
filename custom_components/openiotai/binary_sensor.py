# custom_components/openiotai/binary_sensor.py
from __future__ import annotations

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo

from .const import DOMAIN


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities,
) -> None:
    exporter = hass.data[DOMAIN].get(entry.entry_id)

    if exporter is None:
        # Should never happen, but be defensive
        return

    async_add_entities(
        [OpenIOTAIConnectionSensor(hass, entry, exporter)],
        update_before_add=True,
    )


class OpenIOTAIConnectionSensor(BinarySensorEntity):
    """Binary sensor reflecting MQTT connection state."""

    _attr_name = "OpenIOTAI MQTT Connected"
    _attr_icon = "mdi:lan-connect"
    _attr_device_class = "connectivity"
    _attr_has_entity_name = True

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, exporter) -> None:
        self.hass = hass
        self.entry = entry
        self.exporter = exporter

        # Stable entity_id (no migrations, no randomness)
        self._attr_unique_id = f"{entry.entry_id}_mqtt_connected"

    @property
    def device_info(self) -> DeviceInfo:
        """Attach entity to the OpenIOTAI integration device."""
        return DeviceInfo(
            identifiers={(DOMAIN, self.entry.entry_id)},
            name="OpenIOTAI",
            manufacturer="OpenIOTAI",
            configuration_url="https://github.com/openiotai",
        )

    @property
    def is_on(self) -> bool:
        """Return True if MQTT is currently connected."""
        return bool(self.exporter.connected)

    @callback
    def _handle_state_change(self) -> None:
        """Write updated state to Home Assistant."""
        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        """Register callbacks when entity is added to HA."""
        # Exporter updates connected/last_error internally;
        # we just poll state lightly via dispatcher-style callback.
        self.async_on_remove(
            self.hass.helpers.event.async_track_time_interval(
                lambda _: self._handle_state_change(),
                5,
            )
        )
