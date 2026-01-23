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

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = []


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

    # Check if GTFS zip file exists
    zip_filename = f"{operating_area}.zip"
    zip_path = data_dir / zip_filename

    if not zip_path.exists():
        _LOGGER.info(f"GTFS data file not found at {zip_path}, downloading...")
        try:
            await download_gtfs_data(hass, data_url, zip_path)
            _LOGGER.info(f"Successfully downloaded GTFS data to {zip_path}")
        except Exception as err:
            _LOGGER.error(f"Failed to download GTFS data: {err}")
            raise ConfigEntryNotReady from err
    else:
        _LOGGER.info(f"GTFS data file already exists at {zip_path}")

    # Store data in hass.data
    hass.data[DOMAIN][entry.entry_id] = {
        "operating_area": operating_area,
        "data_url": data_url,
        "data_dir": data_dir,
        "zip_path": zip_path,
    }

    # Set up platforms (none for now, but ready for future sensor platform)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def download_gtfs_data(
    hass: HomeAssistant, url: str, destination: Path
) -> None:
    """Download GTFS data from the specified URL."""
    _LOGGER.debug(f"Starting download from {url}")
    
    # Use Home Assistant's shared session for better connection pooling
    session = async_get_clientsession(hass)
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=600)) as response:
            if response.status != 200:
                raise Exception(
                    f"Failed to download GTFS data: HTTP {response.status}"
                )

            # Download in chunks to handle large files
            with open(destination, "wb") as f:
                async for chunk in response.content.iter_chunked(8192):
                    f.write(chunk)

            _LOGGER.debug(f"Download completed: {destination}")
            
    except Exception as err:
        # Clean up partial download
        if destination.exists():
            destination.unlink()
        raise


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok
