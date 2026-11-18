"""
===============================================================================
GeoPulse Retail Mobility Analytics - Application Settings & Configuration
File: backend/app/config.py
Description: Centralized Pydantic v2 settings loading environment variables,
             Snowflake credentials, CORS policies, and spatial catchment defaults.
===============================================================================
"""

from pathlib import Path
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Base directory for resolving relative data paths
BASE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables and .env configuration.
    """

    # Service Metadata
    PROJECT_NAME: str = "GeoPulse Retail Mobility Analytics API"
    VERSION: str = "1.0.0"
    API_V1_PREFIX: str = "/api/v1"
    DEBUG: bool = False

    # CORS Configuration
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://localhost:8000",
    ]

    # Snowflake Data Warehouse Credentials
    SNOWFLAKE_ACCOUNT: str = Field(default="xy12345.us-east-1", alias="SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_USER: str = Field(default="DEV_ANALYTICS_USER", alias="SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD: str = Field(default="DevPassSecure123!", alias="SNOWFLAKE_PASSWORD")
    SNOWFLAKE_WAREHOUSE: str = Field(default="COMPUTE_WH", alias="SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE: str = Field(default="GEOPULSE_DB", alias="SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA: str = Field(default="GOLD", alias="SNOWFLAKE_SCHEMA")
    SNOWFLAKE_ROLE: str = Field(default="SYSADMIN", alias="SNOWFLAKE_ROLE")

    # Spatial Catchment & Indexing Parameters
    DEFAULT_CATCHMENT_RADIUS_METERS: float = Field(default=250.0, alias="DEFAULT_CATCHMENT_RADIUS")
    H3_RESOLUTION: int = Field(default=9, alias="H3_RESOLUTION")
    MANHATTAN_MIN_LAT: float = 40.700000
    MANHATTAN_MAX_LAT: float = 40.850000
    MANHATTAN_MIN_LON: float = -74.050000
    MANHATTAN_MAX_LON: float = -73.900000

    # Local Dev Data Fallback Paths
    LOCAL_FALLBACK_MODE: bool = Field(default=True, alias="LOCAL_FALLBACK_MODE")
    PARQUET_METRICS_PATH: Path = BASE_DIR / "pyspark" / "output" / "partitioned_metrics"
    GEOFENCES_CONFIG_PATH: Path = BASE_DIR / "pyspark" / "store_geofences.json"
    MOCK_GPS_PINGS_PATH: Path = BASE_DIR / "pyspark" / "mock_gps_pings.csv"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )


settings = Settings()
