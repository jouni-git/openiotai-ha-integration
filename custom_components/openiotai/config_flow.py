"""Config flow for OpenIOTAI integration."""

from __future__ import annotations

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult

from .const import DOMAIN


class OpenIOTAIConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for OpenIOTAI."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict | None = None
    ) -> FlowResult:
        """
        Handle the initial step.

        This integration currently has no configuration options.
        """
        if user_input is not None:
            return self.async_create_entry(
                title="OpenIOTAI",
                data={},
            )

        return self.async_show_form(
            step_id="user",
            data_schema=None,
        )
