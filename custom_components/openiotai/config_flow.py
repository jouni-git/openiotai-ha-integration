"""Config flow for OpenIOTAI integration."""

from __future__ import annotations

import asyncio
import socket
import ssl
import voluptuous as vol
import paho.mqtt.client as mqtt

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
        """Initial setup step."""
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
        errors: dict[str, str] = {}

        if user_input is not None:
            # --------------------------------------------------------------
            # 1. Soft validation
            # --------------------------------------------------------------
            broker = user_input.get(CONF_MQTT_BROKER)
            port = user_input.get(CONF_MQTT_PORT)

            if not broker:
                errors["base"] = "missing_broker"
            elif not isinstance(port, int) or port <= 0:
                errors["base"] = "invalid_port"

            # --------------------------------------------------------------
            # 2. MQTT connection test (blocking but short & safe)
            # --------------------------------------------------------------
            if not errors:
                try:
                    await self._async_test_mqtt_connection(user_input)
                except CannotConnect:
                    errors["base"] = "cannot_connect"
                except InvalidAuth:
                    errors["base"] = "invalid_auth"
                except TlsError:
                    errors["base"] = "tls_error"
                except Exception:
                    errors["base"] = "unknown"

            if not errors:
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
            errors=errors,
        )

    # ------------------------------------------------------------------
    # MQTT connection test
    # ------------------------------------------------------------------
    async def _async_test_mqtt_connection(self, cfg: dict) -> None:
        """Test MQTT connection with TLS and auth."""
        broker = cfg[CONF_MQTT_BROKER]
        port = cfg[CONF_MQTT_PORT]
        use_tls = cfg[CONF_MQTT_TLS]
        ca_cert = cfg.get(CONF_MQTT_CA_CERT)
        username = cfg.get(CONF_MQTT_USERNAME)
        password = cfg.get(CONF_MQTT_PASSWORD)

        def _blocking_test() -> None:
            client = mqtt.Client(client_id="openiotai-config-test")

            if username:
                client.username_pw_set(username, password)

            if use_tls:
                try:
                    context = ssl.create_default_context(
                        cafile=ca_cert if ca_cert else None
                    )
                    client.tls_set_context(context)
                except Exception as exc:
                    raise TlsError from exc

            try:
                result = client.connect(broker, port, keepalive=5)
                if result != mqtt.MQTT_ERR_SUCCESS:
                    raise CannotConnect
                client.disconnect()
            except ssl.SSLError as exc:
                raise TlsError from exc
            except socket.gaierror as exc:
                raise CannotConnect from exc
            except ConnectionRefusedError as exc:
                raise CannotConnect from exc
            except mqtt.MQTTException as exc:
                raise InvalidAuth from exc

        await asyncio.get_running_loop().run_in_executor(
            None, _blocking_test
        )


# ----------------------------------------------------------------------
# Custom exceptions for HA-style error mapping
# ----------------------------------------------------------------------
class CannotConnect(Exception):
    """MQTT broker not reachable."""


class InvalidAuth(Exception):
    """Invalid MQTT authentication."""


class TlsError(Exception):
    """TLS configuration or handshake failed."""
