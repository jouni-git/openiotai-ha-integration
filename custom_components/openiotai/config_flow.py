"""Config flow for OpenIOTAI integration."""

from __future__ import annotations

import logging
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .const import (
    DOMAIN,
    CONF_MQTT_BROKER,
    CONF_MQTT_PORT,
    CONF_MQTT_TOPIC,
    CONF_MQTT_TLS,
    CONF_MQTT_CA_CERT,
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
    DEFAULT_MQTT_BROKER,
    DEFAULT_MQTT_PORT,
    DEFAULT_MQTT_TOPIC,
    DEFAULT_MQTT_TLS,
    DEFAULT_MQTT_USERNAME,
    DEFAULT_MQTT_PASSWORD,
    CONF_PUBLISH_INTERVAL,
    DEFAULT_PUBLISH_INTERVAL,
)

_LOGGER = logging.getLogger(__name__)

MIN_PUBLISH_INTERVAL_SEC = 1
MAX_PUBLISH_INTERVAL_SEC = 3600


# ---------------------------------------------------------------------
# Config flow (dummy entry, all real config is in options)
# ---------------------------------------------------------------------
class OpenIOTAIConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle initial setup."""

    VERSION = 1  # no migrations

    async def async_step_user(self, user_input=None) -> FlowResult:
        if user_input is not None:
            return self.async_create_entry(title="OpenIOTAI", data={})

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({}),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry):
        return OpenIOTAIOptionsFlow(config_entry)


# ---------------------------------------------------------------------
# Options flow (Save only, no network, no test)
# ---------------------------------------------------------------------
class OpenIOTAIOptionsFlow(config_entries.OptionsFlow):
    """Options flow for OpenIOTAI."""

    def __init__(self, entry: config_entries.ConfigEntry) -> None:
        self._entry = entry

    async def async_step_init(self, user_input=None) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            publish_interval = user_input.get(CONF_PUBLISH_INTERVAL)

            if not isinstance(publish_interval, int):
                errors[CONF_PUBLISH_INTERVAL] = "invalid_publish_interval"
            elif not (
                MIN_PUBLISH_INTERVAL_SEC
                <= publish_interval
                <= MAX_PUBLISH_INTERVAL_SEC
            ):
                errors[CONF_PUBLISH_INTERVAL] = "invalid_publish_interval"

            if not errors:
                return self.async_create_entry(
                    title="",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="init",
            data_schema=self._schema(),
            errors=errors,
        )

    def _schema(self) -> vol.Schema:
        opts = self._entry.options

        return vol.Schema(
            {
                vol.Required(
                    CONF_MQTT_BROKER,
                    default=opts.get(CONF_MQTT_BROKER, DEFAULT_MQTT_BROKER),
                ): str,
                vol.Required(
                    CONF_MQTT_PORT,
                    default=opts.get(CONF_MQTT_PORT, DEFAULT_MQTT_PORT),
                ): int,
                vol.Required(
                    CONF_MQTT_TOPIC,
                    default=opts.get(CONF_MQTT_TOPIC, DEFAULT_MQTT_TOPIC),
                ): str,
                vol.Required(
                    CONF_MQTT_TLS,
                    default=opts.get(CONF_MQTT_TLS, DEFAULT_MQTT_TLS),
                ): bool,
                vol.Required(
                    CONF_MQTT_USERNAME,
                    default=opts.get(CONF_MQTT_USERNAME, DEFAULT_MQTT_USERNAME),
                ): str,
                vol.Required(
                    CONF_MQTT_PASSWORD,
                    default=opts.get(CONF_MQTT_PASSWORD, DEFAULT_MQTT_PASSWORD),
                ): str,
                vol.Optional(
                    CONF_MQTT_CA_CERT,
                    default=opts.get(CONF_MQTT_CA_CERT, ""),
                ): str,
                vol.Required(
                    CONF_PUBLISH_INTERVAL,
                    default=opts.get(
                        CONF_PUBLISH_INTERVAL, DEFAULT_PUBLISH_INTERVAL
                    ),
                ): vol.All(
                    int,
                    vol.Range(
                        min=MIN_PUBLISH_INTERVAL_SEC,
                        max=MAX_PUBLISH_INTERVAL_SEC,
                    ),
                ),
            }
        )
