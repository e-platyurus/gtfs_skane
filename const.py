"""Constants for the GTFS Skåne integration."""

DOMAIN = "gtfs_skane"

# Configuration keys
CONF_DATA_URL = "data_url"
CONF_OPERATING_AREA = "operating_area"
CONF_API_KEY = "api_key"

# Default values
DEFAULT_OPERATING_AREA = "skane"
DEFAULT_DATA_URL_TEMPLATE = "https://opendata.samtrafiken.se/gtfs/{operating_area}/{operating_area}.zip"

# Data directory
DATA_DIR_NAME = "gtfs_skane"
