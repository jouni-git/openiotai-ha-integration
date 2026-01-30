# custom_components/openiotai/binary_sensor.py
from __future__ import annotations

from datetime import timedelta

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.event import async_track_time_interval

from .const import DOMAIN


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities,
) -> None:
    exporter = hass.data[DOMAIN].get(entry.entry_id)

    if exporter is None:
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

        self._attr_unique_id = f"{entry.entry_id}_mqtt_connected"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self.entry.entry_id)},
            name="OpenIOTAI",
            manufacturer="OpenIOTAI",
            configuration_url="https://github.com/jouni-git/openiotai-ha-integration",
        )

    @property
    def is_on(self) -> bool:
        return bool(self.exporter.connected)

    @callback
    def _handle_state_change(self, _now=None) -> None:
        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_track_time_interval(
                self.hass,
                self._handle_state_change,
                timedelta(seconds=5),
            )
        )
