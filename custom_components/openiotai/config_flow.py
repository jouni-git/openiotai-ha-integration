"""Config flow for OpenIOTAI integration."""

from __future__ import annotations

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
)


class OpenIOTAIConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for OpenIOTAI."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict | None = None
    ) -> FlowResult:
        """
        Initial setup step.

        No mandatory configuration is required at creation time.
        MQTT configuration is handled via Options Flow.
        """
        if user_input is not None:
            return self.async_create_entry(
                title="OpenIOTAI",
                data={},
            )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({}),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry):
        """Return the options flow handler."""
        return OpenIOTAIOptionsFlow(config_entry)


class OpenIOTAIOptionsFlow(config_entries.OptionsFlow):
    """Handle OpenIOTAI options flow."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._entry = config_entry

    async def async_step_init(
        self, user_input: dict | None = None
    ) -> FlowResult:
        """Manage the OpenIOTAI options."""
        if user_input is not None:
            return self.async_create_entry(
                title="",
                data=user_input,
            )

        options = self._entry.options

        schema = vol.Schema(
            {
                vol.Required(
                    CONF_MQTT_BROKER,
                    default=options.get(
                        CONF_MQTT_BROKER,
                        DEFAULT_MQTT_BROKER,
                    ),
                ): str,
                vol.Required(
                    CONF_MQTT_PORT,
                    default=options.get(
                        CONF_MQTT_PORT,
                        DEFAULT_MQTT_PORT,
                    ),
                ): int,
                vol.Required(
                    CONF_MQTT_TOPIC,
                    default=options.get(
                        CONF_MQTT_TOPIC,
                        DEFAULT_MQTT_TOPIC,
                    ),
                ): str,
                vol.Required(
                    CONF_MQTT_TLS,
                    default=options.get(
                        CONF_MQTT_TLS,
                        DEFAULT_MQTT_TLS,
                    ),
                ): bool,
                vol.Required(
                    CONF_MQTT_USERNAME,
                    default=options.get(
                        CONF_MQTT_USERNAME,
                        DEFAULT_MQTT_USERNAME,
                    ),
                ): str,
                vol.Required(
                    CONF_MQTT_PASSWORD,
                    default=options.get(
                        CONF_MQTT_PASSWORD,
                        DEFAULT_MQTT_PASSWORD,
                    ),
                ): str,
                vol.Optional(
                    CONF_MQTT_CA_CERT,
                    default=options.get(CONF_MQTT_CA_CERT, ""),
                ): str,
            }
        )

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
        )
