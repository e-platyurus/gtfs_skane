"""The GTFS Skåne integration."""
from __future__ import annotations

import logging
import os
from pathlib import Path

import aiohttp
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    CONF_API_KEY,
    CONF_DATA_URL,
    CONF_OPERATING_AREA,
    DATA_DIR_NAME,
    DEFAULT_DATA_URL_TEMPLATE,
    DOMAIN,
)
from .gtfs_data import GTFSDataManager

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.UPDATE]


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the GTFS Skåne component."""
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up GTFS Skåne from a config entry."""
    _LOGGER.info("Setting up GTFS Skåne integration")

    # Get configuration
    operating_area = entry.data.get(CONF_OPERATING_AREA)
    api_key = entry.data.get(CONF_API_KEY)
    data_url_template = entry.data.get(CONF_DATA_URL)

    # Build the actual data URL
    data_url = data_url_template.format(operating_area=operating_area)
    
    # Add API key as query parameter
    if "?" in data_url:
        data_url = f"{data_url}&key={api_key}"
    else:
        data_url = f"{data_url}?key={api_key}"

    # Ensure data directory exists
    config_dir = hass.config.path()
    data_dir = Path(config_dir) / DATA_DIR_NAME
    
    try:
        data_dir.mkdir(exist_ok=True)
        _LOGGER.info(f"Data directory ensured at: {data_dir}")
    except Exception as err:
        _LOGGER.error(f"Failed to create data directory: {err}")
        raise ConfigEntryNotReady from err

    # Initialize data manager
    data_manager = GTFSDataManager(
        hass=hass,
        data_dir=data_dir,
        data_url=data_url,
        operating_area=operating_area,
    )
    
    # Check if database exists (initial setup)
    if not data_manager.database_exists():
        _LOGGER.warning(
            "GTFS database not found. Please trigger an update via the Update entity "
            "to download and convert GTFS data."
        )
        # Don't raise ConfigEntryNotReady - let the integration load
        # User will see update entity showing update available
    else:
        _LOGGER.info("GTFS database found, integration ready")

    # Store data in hass.data
    hass.data[DOMAIN][entry.entry_id] = {
        "operating_area": operating_area,
        "data_url": data_url,
        "data_dir": data_dir,
        "data_manager": data_manager,
    }

    # Set up platforms
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok
