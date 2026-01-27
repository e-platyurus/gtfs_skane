"""GTFS data management for downloads and conversions."""
from __future__ import annotations

import asyncio
import logging
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
import pygtfs
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession

_LOGGER = logging.getLogger(__name__)

# Retry configuration
MAX_DOWNLOAD_RETRIES = 3
RETRY_DELAYS = [5, 15, 30]  # seconds between retries


class GTFSDataManager:
    """Manages GTFS data download, conversion, and validation."""

    def __init__(
        self,
        hass: HomeAssistant,
        data_dir: Path,
        data_url: str,
        operating_area: str,
    ) -> None:
        """Initialize the data manager."""
        self.hass = hass
        self.data_dir = data_dir
        self.data_url = data_url
        self.operating_area = operating_area
        
        self.zip_path = data_dir / f"{operating_area}.zip"
        self.db_path = data_dir / f"{operating_area}.sqlite"
        self.metadata_path = data_dir / "metadata.json"
        
        self._state = {
            "state": "idle",  # idle|downloading|converting|validating|error
            "progress": None,
            "error": None,
        }
        
        self._metadata = self._load_metadata()

    def get_state(self) -> dict[str, Any]:
        """Get current update state."""
        return self._state.copy()

    def get_metadata(self) -> dict[str, Any]:
        """Get data metadata."""
        return self._metadata.copy()

    def _load_metadata(self) -> dict[str, Any]:
        """Load metadata from file."""
        if not self.metadata_path.exists():
            return {}
        
        try:
            import json
            with open(self.metadata_path, "r") as f:
                data = json.load(f)
                # Convert ISO strings back to datetime
                if "last_download" in data:
                    data["last_download"] = datetime.fromisoformat(data["last_download"])
                return data
        except Exception as err:
            _LOGGER.warning(f"Failed to load metadata: {err}")
            return {}

    def _save_metadata(self) -> None:
        """Save metadata to file."""
        try:
            import json
            # Convert datetime to ISO string for JSON serialization
            data = self._metadata.copy()
            if "last_download" in data and isinstance(data["last_download"], datetime):
                data["last_download"] = data["last_download"].isoformat()
            
            with open(self.metadata_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as err:
            _LOGGER.error(f"Failed to save metadata: {err}")

    async def update_data(self) -> None:
        """Download and convert GTFS data."""
        try:
            # Step 1: Cleanup old data
            _LOGGER.info("Step 1/4: Cleaning up old data")
            self._state["state"] = "downloading"
            self._state["progress"] = 0
            self._state["error"] = None
            await self._cleanup_old_data()
            
            # Step 2: Download new zip
            _LOGGER.info("Step 2/4: Downloading GTFS data")
            self._state["progress"] = 25
            await self._download_with_retry()
            
            # Step 3: Convert to SQLite
            _LOGGER.info("Step 3/4: Converting to database (this may take 1-2 hours)")
            self._state["state"] = "converting"
            self._state["progress"] = 50
            await self._convert_to_sqlite()
            
            # Step 4: Validate
            _LOGGER.info("Step 4/4: Validating data")
            self._state["state"] = "validating"
            self._state["progress"] = 90
            await self._validate_data()
            
            # Update metadata
            self._metadata["last_download"] = datetime.now()
            self._metadata["db_size_mb"] = round(self.db_path.stat().st_size / (1024 * 1024), 1)
            self._save_metadata()
            
            # Cleanup zip file
            if self.zip_path.exists():
                self.zip_path.unlink()
                _LOGGER.debug(f"Cleaned up zip file: {self.zip_path}")
            
            # Done
            self._state["state"] = "idle"
            self._state["progress"] = 100
            _LOGGER.info("GTFS data update completed successfully")
            
        except Exception as err:
            self._state["state"] = "error"
            self._state["error"] = str(err)
            self._state["progress"] = None
            _LOGGER.error(f"GTFS data update failed: {err}")
            raise

    async def _cleanup_old_data(self) -> None:
        """Delete old database and zip files."""
        if self.db_path.exists():
            _LOGGER.info(f"Deleting old database: {self.db_path}")
            await self.hass.async_add_executor_job(self.db_path.unlink)
        
        if self.zip_path.exists():
            _LOGGER.info(f"Deleting old zip: {self.zip_path}")
            await self.hass.async_add_executor_job(self.zip_path.unlink)

    async def _download_with_retry(self) -> None:
        """Download GTFS zip with retry logic."""
        for attempt in range(MAX_DOWNLOAD_RETRIES):
            try:
                _LOGGER.debug(f"Download attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES}")
                await self._download_gtfs_zip()
                return  # Success
            except Exception as err:
                _LOGGER.warning(f"Download attempt {attempt + 1} failed: {err}")
                
                if attempt < MAX_DOWNLOAD_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt]
                    _LOGGER.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    # Final attempt failed
                    raise HomeAssistantError(
                        f"Failed to download GTFS data after {MAX_DOWNLOAD_RETRIES} attempts"
                    ) from err

    async def _download_gtfs_zip(self) -> None:
        """Download GTFS zip file."""
        session = async_get_clientsession(self.hass)
        
        _LOGGER.debug(f"Downloading from {self.data_url}")
        
        async with session.get(
            self.data_url,
            timeout=aiohttp.ClientTimeout(total=600)
        ) as response:
            if response.status != 200:
                raise HomeAssistantError(
                    f"Failed to download GTFS data: HTTP {response.status}"
                )
            
            # Download in chunks
            with open(self.zip_path, "wb") as f:
                async for chunk in response.content.iter_chunked(8192):
                    f.write(chunk)
        
        _LOGGER.info(f"Downloaded GTFS data to {self.zip_path}")

    async def _convert_to_sqlite(self) -> None:
        """Convert GTFS zip to SQLite database using pygtfs."""
        def _convert():
            """Blocking conversion operation."""
            _LOGGER.info("Starting pygtfs conversion (this will take a while)...")
            
            # Create schedule from zip
            # pygtfs.append_feed creates/updates a SQLite database
            schedule = pygtfs.Schedule(str(self.db_path))
            pygtfs.append_feed(schedule, str(self.zip_path))
            
            _LOGGER.info("pygtfs conversion completed")
            
            # Create performance indexes
            _LOGGER.info("Creating performance indexes...")
            self._create_indexes(schedule)
            _LOGGER.info("Indexes created")
        
        # Run in executor since it's CPU-intensive and blocking
        await self.hass.async_add_executor_job(_convert)

    def _create_indexes(self, schedule: pygtfs.Schedule) -> None:
        """Create performance indexes on the database."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id ON stop_times(stop_id)",
            "CREATE INDEX IF NOT EXISTS idx_stop_times_trip_id ON stop_times(trip_id)",
            "CREATE INDEX IF NOT EXISTS idx_trips_route_id ON trips(route_id)",
            "CREATE INDEX IF NOT EXISTS idx_trips_service_id ON trips(service_id)",
            "CREATE INDEX IF NOT EXISTS idx_trips_direction_id ON trips(direction_id)",
            "CREATE INDEX IF NOT EXISTS idx_calendar_dates_service_id ON calendar_dates(service_id)",
            "CREATE INDEX IF NOT EXISTS idx_calendar_dates_date ON calendar_dates(date)",
            "CREATE INDEX IF NOT EXISTS idx_routes_agency_id ON routes(agency_id)",
            "CREATE INDEX IF NOT EXISTS idx_stop_times_composite ON stop_times(stop_id, trip_id, departure_time)",
        ]
        
        conn = schedule.engine.raw_connection()
        cursor = conn.cursor()
        
        for idx_sql in indexes:
            try:
                cursor.execute(idx_sql)
                _LOGGER.debug(f"Created index: {idx_sql.split('idx_')[1].split(' ')[0]}")
            except Exception as err:
                _LOGGER.warning(f"Failed to create index: {err}")
        
        conn.commit()
        cursor.close()

    async def _validate_data(self) -> None:
        """Validate the converted database."""
        def _validate():
            """Blocking validation operation."""
            import sqlite3
            
            # Check database exists and is not corrupt
            if not self.db_path.exists():
                raise HomeAssistantError("Database file not created")
            
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            try:
                # Check required tables exist
                required_tables = ["agency", "stops", "routes", "trips", "stop_times", "calendar_dates"]
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                existing_tables = {row[0] for row in cursor.fetchall()}
                
                missing_tables = set(required_tables) - existing_tables
                if missing_tables:
                    raise HomeAssistantError(
                        f"Database missing required tables: {missing_tables}"
                    )
                
                # Check that tables have data
                for table in required_tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    if count == 0:
                        raise HomeAssistantError(f"Table {table} is empty")
                    _LOGGER.debug(f"Table {table}: {count} rows")
                
                # Validate configured stops exist (if any devices configured)
                # TODO: Check stops from hass.data when we implement device/sensor platform
                
                _LOGGER.info("Database validation passed")
                
            finally:
                cursor.close()
                conn.close()
        
        await self.hass.async_add_executor_job(_validate)

    def database_exists(self) -> bool:
        """Check if database exists."""
        return self.db_path.exists()
