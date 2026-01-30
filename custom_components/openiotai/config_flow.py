"""Config flow for OpenIOTAI integration."""

from __future__ import annotations

import asyncio
import logging
import socket
import ssl
from typing import Any

import paho.mqtt.client as mqtt
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

        return self.async_show_form(step_id="user", data_schema=vol.Schema({}))

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry):
        return OpenIOTAIOptionsFlow(config_entry)


# ---------------------------------------------------------------------
# Options flow
# ---------------------------------------------------------------------
class OpenIOTAIOptionsFlow(config_entries.OptionsFlow):
    """Options flow with Test connection + Save."""

    def __init__(self, entry: config_entries.ConfigEntry) -> None:
        self._entry = entry
        self._cached_input: dict[str, Any] | None = None

    # ------------------------------------------------------------
    # STEP: main options (Save only, no network!)
    # ------------------------------------------------------------
    async def async_step_init(self, user_input=None) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            publish_interval = user_input[CONF_PUBLISH_INTERVAL]
            if not (
                MIN_PUBLISH_INTERVAL_SEC
                <= publish_interval
                <= MAX_PUBLISH_INTERVAL_SEC
            ):
                errors[CONF_PUBLISH_INTERVAL] = "invalid_publish_interval"

            if not errors:
                return self.async_create_entry(title="", data=user_input)

        return self.async_show_form(
            step_id="init",
            data_schema=self._schema(),
            errors=errors,
        )

    # ------------------------------------------------------------
    # STEP: Test connection (network allowed)
    # ------------------------------------------------------------
    async def async_step_test(self, user_input=None) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            self._cached_input = user_input
            try:
                await self._async_test_mqtt_connection(user_input)
                return self.async_show_form(
                    step_id="init",
                    data_schema=self._schema(user_input),
                    description_placeholders={
                        "test_result": "✅ Connection successful"
                    },
                )
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except TlsError:
                errors["base"] = "tls_error"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="init",
            data_schema=self._schema(user_input),
            errors=errors,
        )

    # ------------------------------------------------------------
    # Schema with Test button
    # ------------------------------------------------------------
    def _schema(self, defaults: dict | None = None) -> vol.Schema:
        opts = defaults or self._entry.options

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
                # 👇 This creates the "Test connection" button
                vol.Optional("test_connection", default=False): bool,
            }
        )

    # ------------------------------------------------------------
    # MQTT connection test (blocking, authoritative)
    # ------------------------------------------------------------
    async def _async_test_mqtt_connection(self, cfg: dict) -> None:
        broker = cfg[CONF_MQTT_BROKER]
        port = cfg[CONF_MQTT_PORT]
        use_tls = cfg[CONF_MQTT_TLS]
        ca_cert = cfg.get(CONF_MQTT_CA_CERT)
        username = cfg.get(CONF_MQTT_USERNAME)
        password = cfg.get(CONF_MQTT_PASSWORD)

        loop = asyncio.get_running_loop()
        connected = asyncio.Event()
        error: dict[str, str | None] = {"reason": None}

        def on_connect(_client, _userdata, _flags, rc, _props=None):
            if rc == 0:
                loop.call_soon_threadsafe(connected.set)
            elif rc in (4, 5):
                error["reason"] = "auth"
                loop.call_soon_threadsafe(connected.set)
            else:
                error["reason"] = "connect"
                loop.call_soon_threadsafe(connected.set)

        def blocking():
            client = mqtt.Client(client_id="openiotai-test")
            client.on_connect = on_connect

            if username:
                client.username_pw_set(username, password)

            if use_tls:
                try:
                    ctx = ssl.create_default_context(
                        cafile=ca_cert if ca_cert else None
                    )
                    client.tls_set_context(ctx)
                except Exception as exc:
                    raise TlsError from exc

            try:
                client.connect(broker, port, keepalive=5)
                client.loop_start()
                if not connected.wait(timeout=5):
                    raise CannotConnect
            finally:
                client.loop_stop()
                client.disconnect()

            if error["reason"] == "auth":
                raise InvalidAuth
            if error["reason"] == "connect":
                raise CannotConnect

        await loop.run_in_executor(None, blocking)


# ---------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------
class CannotConnect(Exception):
    """MQTT broker not reachable."""


class InvalidAuth(Exception):
    """Invalid MQTT authentication."""


class TlsError(Exception):
    """TLS configuration or handshake failed."""
