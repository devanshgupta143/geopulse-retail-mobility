"""
GeoPulse Retail Mobility Analytics - Centralized Configuration Module
====================================================================
Provides centralized configurations, spatial constants, store geofence
definitions, and standardized input/output filesystem paths across
the PySpark processing pipeline.
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("GeoPulse.Config")

# ==========================================
# PATH CONFIGURATION
# ==========================================
PYSPARK_DIR: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = PYSPARK_DIR.parent

# Input paths
STORE_GEOFENCES_PATH: Path = PYSPARK_DIR / "store_geofences.json"
RAW_GPS_PINGS_PATH: Path = PYSPARK_DIR / "mock_gps_pings.csv"

# Output directory & subpaths
OUTPUT_DIR: Path = PYSPARK_DIR / "output"
PROCESSED_PARTITIONS_DIR: Path = OUTPUT_DIR / "partitioned_pings"
H3_INDEXED_PINGS_DIR: Path = OUTPUT_DIR / "h3_indexed_pings"
CANNIBALIZATION_METRICS_DIR: Path = OUTPUT_DIR / "cannibalization_metrics"

# Ensure output directory structure exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================
# SPATIAL & H3 INDEXING CONSTANTS
# ==========================================
# Catchment radius around retail anchors
CATCHMENT_RADIUS_METERS: float = 250.0

# H3 Hexagonal Grid Resolutions
# Res 9: ~100m edge length, ~0.1 km2 area - ideal for 250m walking catchment matching
# Res 8: ~460m edge length, ~0.7 km2 area - neighborhood/district aggregation
# Res 10: ~40m edge length, ~0.015 km2 area - micro-corridor / street frontage
H3_RESOLUTION: int = 9
H3_PARENT_RESOLUTION: int = 8
H3_FINE_RESOLUTION: int = 10

# Earth radius in meters for haversine / spatial distance calculations
EARTH_RADIUS_METERS: float = 6371000.0

# Approximate degree conversions for Manhattan (~40.75 deg N)
METERS_PER_DEG_LAT: float = 111132.95
METERS_PER_DEG_LON: float = 84400.0  # cos(40.75°) * 111320m

# ==========================================
# TEMPORAL & COMMUTER CANNIBALIZATION SPECS
# ==========================================
SIMULATION_DATE: str = "2026-11-01"
PEAK_HOURS_START: str = "07:00:00"
PEAK_HOURS_END: str = "09:59:59"
DAY_HOURS_START: str = "06:00:00"
DAY_HOURS_END: str = "22:00:00"

# Target stores for cannibalization analysis
STORE_A_ID: str = "STORE_A_EXISTING"
STORE_B_ID: str = "STORE_B_PROPOSED_5TH"
STORE_C_ID: str = "STORE_C_ALT_WEST"


# ==========================================
# DATA MODELS & GEOFENCE LOADING
# ==========================================
@dataclass(frozen=True)
class StoreGeofence:
    """Store geofence and metadata model."""

    store_id: str
    name: str
    status: str
    lat: float
    lon: float
    catchment_radius_meters: float
    address: str
    borough: str
    neighborhood: str
    coordinates: List[float] = field(default_factory=list)

    @property
    def point(self) -> tuple[float, float]:
        """Returns (latitude, longitude) tuple."""
        return (self.lat, self.lon)


def load_store_geofences(
    config_path: Path = STORE_GEOFENCES_PATH,
) -> Dict[str, StoreGeofence]:
    """
    Loads and parses the store geofences JSON configuration.

    Returns:
        Dict[str, StoreGeofence]: Mapping of store_id to StoreGeofence dataclass instance.
    """
    if not config_path.exists():
        logger.warning("Geofence file not found at %s. Returning empty mapping.", config_path)
        return {}

    with open(config_path, "r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)

    stores_map: Dict[str, StoreGeofence] = {}
    for item in data.get("stores", []):
        store = StoreGeofence(
            store_id=item["store_id"],
            name=item["name"],
            status=item["status"],
            lat=float(item["lat"]),
            lon=float(item["lon"]),
            catchment_radius_meters=float(
                item.get("catchment_radius_meters", CATCHMENT_RADIUS_METERS)
            ),
            address=item.get("address", ""),
            borough=item.get("borough", "Manhattan"),
            neighborhood=item.get("neighborhood", ""),
            coordinates=[float(item["lon"]), float(item["lat"])],
        )
        stores_map[store.store_id] = store

    logger.debug("Loaded %d store geofences from %s", len(stores_map), config_path)
    return stores_map


# Module-level loaded store geofences
STORE_GEOFENCES: Dict[str, StoreGeofence] = load_store_geofences()


def get_store(store_id: str) -> Optional[StoreGeofence]:
    """Retrieves store geofence by ID."""
    return STORE_GEOFENCES.get(store_id)
