"""Update entity for GTFS Skåne integration."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from homeassistant.components.update import UpdateEntity, UpdateEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, CONF_OPERATING_AREA
from .gtfs_data import GTFSDataManager

_LOGGER = logging.getLogger(__name__)

# Update is considered needed after this many days
UPDATE_INTERVAL_DAYS = 60


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up GTFS Skåne update entity."""
    update_entity = GTFSUpdateEntity(hass, entry)
    async_add_entities([update_entity])


class GTFSUpdateEntity(UpdateEntity):
    """Representation of GTFS data update status."""

    _attr_has_entity_name = True
    _attr_name = "GTFS Data"
    _attr_supported_features = UpdateEntityFeature.INSTALL

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Initialize the update entity."""
        self.hass = hass
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_update"
        
        operating_area = entry.data.get(CONF_OPERATING_AREA, "skane")
        self._attr_title = f"GTFS {operating_area.capitalize()}"
        
        # Get data manager
        self._data_manager: GTFSDataManager = hass.data[DOMAIN][entry.entry_id]["data_manager"]

    @property
    def device_info(self):
        """Return device info."""
        operating_area = self._entry.data.get(CONF_OPERATING_AREA, "skane")
        return {
            "identifiers": {(DOMAIN, self._entry.entry_id)},
            "name": f"GTFS {operating_area.capitalize()}",
            "manufacturer": "Skånetrafiken",
            "model": "GTFS Static Data",
            "configuration_url": "https://opendata.samtrafiken.se/",
        }

    @property
    def installed_version(self) -> str | None:
        """Version of currently installed GTFS data."""
        metadata = self._data_manager.get_metadata()
        if metadata and metadata.get("last_download"):
            # Return date as version string
            last_download = metadata["last_download"]
            if isinstance(last_download, datetime):
                return last_download.strftime("%Y-%m-%d")
            return str(last_download)
        # Return a very old date to indicate no data installed
        return "1970-01-01"

    @property
    def latest_version(self) -> str | None:
        """Latest available version."""
        # For GTFS, "latest" is always current date since data updates daily
        # But we only recommend updates every 60 days
        return datetime.now().strftime("%Y-%m-%d")

    @property
    def release_url(self) -> str | None:
        """URL for release notes."""
        return "https://opendata.samtrafiken.se/"

    @property
    def update_percentage(self) -> int | None:
        """Update progress percentage."""
        state = self._data_manager.get_state()
        return state.get("progress")

    @property
    def in_progress(self) -> bool:
        """Update installation in progress."""
        state = self._data_manager.get_state()
        return state.get("state") in ["downloading", "converting", "validating"]

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        metadata = self._data_manager.get_metadata()
        state = self._data_manager.get_state()
        
        attrs = {
            "last_download": metadata.get("last_download"),
            "database_size_mb": metadata.get("db_size_mb"),
            "update_state": state.get("state", "idle"),
        }
        
        # Add error info if present
        if state.get("error"):
            attrs["error"] = state["error"]
            
        # Add next recommended update
        if metadata.get("last_download"):
            last_download = metadata["last_download"]
            if isinstance(last_download, datetime):
                next_update = last_download + timedelta(days=UPDATE_INTERVAL_DAYS)
                attrs["next_recommended_update"] = next_update.strftime("%Y-%m-%d")
                
                # Check if update is recommended
                attrs["update_recommended"] = datetime.now() > next_update
        
        return attrs

    async def async_install(
        self, version: str | None, backup: bool, **kwargs: Any
    ) -> None:
        """Install an update (download and convert GTFS data)."""
        _LOGGER.info("Starting GTFS data update")
        
        try:
            # Perform the update
            await self._data_manager.update_data()
            
            # Trigger entity update
            self.async_write_ha_state()
            
            _LOGGER.info("GTFS data update completed successfully")
            
        except Exception as err:
            _LOGGER.error(f"Failed to update GTFS data: {err}")
            # Error is stored in data manager state, will show in attributes
            self.async_write_ha_state()
            raise
