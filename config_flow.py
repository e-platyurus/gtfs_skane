"""Config flow for GTFS Skåne integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import selector

from .const import (
    CONF_API_KEY,
    CONF_DATA_URL,
    CONF_OPERATING_AREA,
    DEFAULT_DATA_URL_TEMPLATE,
    DEFAULT_OPERATING_AREA,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)


class GTFSSkaneConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for GTFS Skåne."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        # Check if already configured - only allow one entry
        if self._async_current_entries():
            return self.async_abort(reason="already_configured")

        errors: dict[str, str] = {}

        if user_input is not None:
            # Validate that API key is provided
            if not user_input.get(CONF_API_KEY):
                errors[CONF_API_KEY] = "api_key_required"
            else:
                # Create the entry
                api_key = user_input.get(CONF_API_KEY)
                operating_area = user_input.get(CONF_OPERATING_AREA, DEFAULT_OPERATING_AREA)
                data_url = user_input.get(CONF_DATA_URL, DEFAULT_DATA_URL_TEMPLATE)

                return self.async_create_entry(
                    title=f"GTFS Skåne ({operating_area})",
                    data={
                        CONF_OPERATING_AREA: operating_area,
                        CONF_DATA_URL: data_url,
                        CONF_API_KEY: api_key,
                    },
                )

        # Show form
        data_schema = vol.Schema(
            {
                vol.Required(CONF_API_KEY): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.PASSWORD,
                        autocomplete="off",
                    ),
                ),
                vol.Optional(
                    CONF_OPERATING_AREA, default=DEFAULT_OPERATING_AREA
                ): selector.TextSelector(),
                vol.Optional(
                    CONF_DATA_URL, 
                    default=DEFAULT_DATA_URL_TEMPLATE,
                ): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.URL,
                    ),
                ),
            }
        )

        return self.async_show_form(
            step_id="user", 
            data_schema=data_schema, 
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle reconfiguration of the integration."""
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}

        if user_input is not None:
            # Validate that API key is provided
            if not user_input.get(CONF_API_KEY):
                errors[CONF_API_KEY] = "api_key_required"
            else:
                # Update the entry
                operating_area = user_input.get(CONF_OPERATING_AREA, DEFAULT_OPERATING_AREA)
                data_url = user_input.get(CONF_DATA_URL, DEFAULT_DATA_URL_TEMPLATE)

                return self.async_update_reload_and_abort(
                    entry,
                    title=f"GTFS Skåne ({operating_area})",
                    data={
                        CONF_OPERATING_AREA: operating_area,
                        CONF_DATA_URL: data_url,
                        CONF_API_KEY: user_input[CONF_API_KEY],
                    },
                )

        # Pre-fill form with current values
        data_schema = vol.Schema(
            {
                vol.Required(
                    CONF_API_KEY, 
                    default=entry.data.get(CONF_API_KEY)
                ): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.PASSWORD,
                        autocomplete="off",
                    ),
                ),
                vol.Optional(
                    CONF_OPERATING_AREA,
                    default=entry.data.get(CONF_OPERATING_AREA, DEFAULT_OPERATING_AREA)
                ): selector.TextSelector(),
                vol.Optional(
                    CONF_DATA_URL,
                    default=entry.data.get(CONF_DATA_URL, DEFAULT_DATA_URL_TEMPLATE)
                ): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.URL,
                    ),
                ),
            }
        )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=data_schema,
            errors=errors,
        )
