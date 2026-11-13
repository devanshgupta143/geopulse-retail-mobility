#!/usr/bin/env python3
"""
GeoPulse Retail Mobility Analytics - Spatial Indexing & Geofence Enrichment
===========================================================================
Performs H3 hexagonal spatial indexing and store catchment distance evaluation
on raw GPS pings.

Pipeline functions:
1. Loads raw mobile GPS pings (`pyspark/mock_gps_pings.csv`) and store geofence
   reference configurations (`pyspark/store_geofences.json`).
2. Converts coordinates (Lat, Long) into H3 hexagonal indices at resolution 9
   (~100m edge length / 0.1 km2 area, optimal for retail catchment analysis).
3. Computes exact geodesic distance (Haversine formula in meters) to:
   - STORE_A_EXISTING (Herald Square Flagship)
   - STORE_B_PROPOSED_5TH (Fifth Avenue Luxury Corridor)
   - STORE_C_ALT_WEST (West 34th District Alternative)
4. Evaluates whether each ping falls within the target 250-meter catchment radius.
5. Exports the enriched dataset to `pyspark/output/enriched_gps_pings.parquet`.
"""

import json
import logging
import math
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

import h3
import numpy as np
import pandas as pd

# Add parent directory to sys.path to import config if needed
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

try:
    import config as cfg
except ImportError:
    cfg = None

# Structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GeoPulse.SpatialIndexing")

# Default paths and constants (fallback to config if present)
DEFAULT_INPUT_CSV = current_dir / "mock_gps_pings.csv"
DEFAULT_GEOFENCES_JSON = current_dir / "store_geofences.json"
DEFAULT_OUTPUT_PARQUET = current_dir / "output" / "enriched_gps_pings.parquet"
H3_RESOLUTION = getattr(cfg, "H3_RESOLUTION", 9)
CATCHMENT_RADIUS_METERS = getattr(cfg, "CATCHMENT_RADIUS_METERS", 250.0)
EARTH_RADIUS_METERS = 6371000.0  # WGS84 volumetric mean radius


def latlng_to_h3(lat: float, lon: float, resolution: int = H3_RESOLUTION) -> str:
    """Converts (lat, lon) to H3 index string supporting both h3 v3 and v4 APIs."""
    if hasattr(h3, "geo_to_h3"):
        return h3.geo_to_h3(lat, lon, resolution)
    elif hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, resolution)
    raise AttributeError("H3 library does not expose geo_to_h3 or latlng_to_cell.")


def haversine_distance_vectorized(
    lats1: np.ndarray,
    lons1: np.ndarray,
    lat2: float,
    lon2: float,
) -> np.ndarray:
    """
    Computes great-circle distances between arrays of coordinates (lats1, lons1)
    and a single target coordinate (lat2, lon2) using the Haversine formula in meters.
    """
    phi1 = np.radians(lats1)
    phi2 = math.radians(lat2)
    delta_phi = np.radians(lat2 - lats1)
    delta_lambda = np.radians(lon2 - lons1)

    a = (
        np.sin(delta_phi / 2.0) ** 2
        + np.cos(phi1) * math.cos(phi2) * np.sin(delta_lambda / 2.0) ** 2
    )
    # Clamp values to avoid numerical precision issues with sqrt
    a = np.clip(a, 0.0, 1.0)
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    return EARTH_RADIUS_METERS * c


def load_store_geofences(geofences_path: Path) -> Dict[str, Dict]:
    """Loads store geofence definitions from JSON."""
    if not geofences_path.exists():
        raise FileNotFoundError(f"Store geofences file not found at: {geofences_path}")
    
    with open(geofences_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stores = {store["store_id"]: store for store in data.get("stores", [])}
    logger.info("Loaded %d store geofence definitions from %s", len(stores), geofences_path)
    return stores


def enrich_spatial_mobility_data(
    input_csv_path: Path = DEFAULT_INPUT_CSV,
    geofences_json_path: Path = DEFAULT_GEOFENCES_JSON,
    output_parquet_path: Path = DEFAULT_OUTPUT_PARQUET,
    h3_res: int = H3_RESOLUTION,
    catchment_radius_m: float = CATCHMENT_RADIUS_METERS,
) -> pd.DataFrame:
    """
    Main spatial enrichment pipeline:
    - Loads raw GPS pings
    - Calculates H3 resolution 9 hex index
    - Computes geodesic distance to stores
    - Determines 250m catchment membership
    - Saves enriched parquet dataset
    """
    logger.info("Loading GPS pings from %s...", input_csv_path)
    if not input_csv_path.exists():
        raise FileNotFoundError(f"Input GPS ping CSV does not exist at: {input_csv_path}")

    df = pd.read_csv(input_csv_path)
    logger.info("Loaded %d rows across %d unique devices.", len(df), df["DeviceID"].nunique())

    # Load store anchors
    stores = load_store_geofences(geofences_json_path)
    store_a = stores.get("STORE_A_EXISTING", {"lat": 40.7549, "lon": -73.9840})
    store_b = stores.get("STORE_B_PROPOSED_5TH", {"lat": 40.7565, "lon": -73.9815})
    store_c = stores.get("STORE_C_ALT_WEST", {"lat": 40.7510, "lon": -73.9930})

    # 1. H3 Hexagonal Spatial Indexing (Resolution 9)
    logger.info("Computing H3 spatial indices at resolution %d...", h3_res)
    # Vectorized / list comprehension conversion
    lats = df["Lat"].values
    lons = df["Long"].values

    if hasattr(h3, "geo_to_h3"):
        df["h3_index_res9"] = [h3.geo_to_h3(lat, lon, h3_res) for lat, lon in zip(lats, lons)]
    else:
        df["h3_index_res9"] = [h3.latlng_to_cell(lat, lon, h3_res) for lat, lon in zip(lats, lons)]

    # 2. Geodesic Distance Calculations (meters)
    logger.info("Calculating Haversine distances to store anchors...")
    dist_a = haversine_distance_vectorized(lats, lons, store_a["lat"], store_a["lon"])
    dist_b = haversine_distance_vectorized(lats, lons, store_b["lat"], store_b["lon"])
    dist_c = haversine_distance_vectorized(lats, lons, store_c["lat"], store_c["lon"])

    df["dist_to_store_a_m"] = np.round(dist_a, 2)
    df["dist_to_store_b_m"] = np.round(dist_b, 2)
    df["dist_to_store_c_m"] = np.round(dist_c, 2)

    # 3. Catchment Membership Evaluation (250m threshold)
    logger.info("Evaluating 250m store catchment inclusion...")
    radius_a = float(store_a.get("catchment_radius_meters", catchment_radius_m))
    radius_b = float(store_b.get("catchment_radius_meters", catchment_radius_m))
    radius_c = float(store_c.get("catchment_radius_meters", catchment_radius_m))

    df["in_catchment_store_a"] = df["dist_to_store_a_m"] <= radius_a
    df["in_catchment_store_b"] = df["dist_to_store_b_m"] <= radius_b
    df["in_catchment_store_c"] = df["dist_to_store_c_m"] <= radius_c

    # Combined nearest store and catchment indicator
    dist_matrix = np.column_stack([dist_a, dist_b, dist_c])
    nearest_idx = np.argmin(dist_matrix, axis=1)
    store_names = ["STORE_A_EXISTING", "STORE_B_PROPOSED_5TH", "STORE_C_ALT_WEST"]
    df["nearest_store_id"] = [store_names[i] for i in nearest_idx]
    df["min_dist_to_store_m"] = np.round(np.min(dist_matrix, axis=1), 2)
    df["in_any_catchment"] = (
        df["in_catchment_store_a"]
        | df["in_catchment_store_b"]
        | df["in_catchment_store_c"]
    )

    # 4. Save to Parquet
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Exporting spatially enriched dataset to %s...", output_parquet_path)
    df.to_parquet(output_parquet_path, engine="pyarrow", index=False)
    
    file_size_mb = output_parquet_path.stat().st_size / (1024 * 1024)
    logger.info("Parquet export successful. Size: %.2f MB", file_size_mb)

    # 5. Summary Statistics
    unique_h3_count = df["h3_index_res9"].nunique()
    pings_in_a = df["in_catchment_store_a"].sum()
    pings_in_b = df["in_catchment_store_b"].sum()
    pings_in_c = df["in_catchment_store_c"].sum()
    pings_in_any = df["in_any_catchment"].sum()

    print("\n" + "=" * 65)
    print("GEOPULSE SPATIAL INDEXING & GEOFENCE ENRICHMENT SUMMARY")
    print("=" * 65)
    print(f"Total Rows Enriched           : {len(df):,}")
    print(f"Unique Mobile Devices         : {df['DeviceID'].nunique():,}")
    print(f"Unique H3 Hexagons (Res {h3_res})   : {unique_h3_count:,}")
    print(f"Catchment Radius Threshold    : {catchment_radius_m} meters")
    print(f"Pings in STORE_A Catchment    : {pings_in_a:,} ({pings_in_a / len(df) * 100:.1f}%)")
    print(f"Pings in STORE_B Catchment    : {pings_in_b:,} ({pings_in_b / len(df) * 100:.1f}%)")
    print(f"Pings in STORE_C Catchment    : {pings_in_c:,} ({pings_in_c / len(df) * 100:.1f}%)")
    print(f"Pings in ANY Catchment        : {pings_in_any:,} ({pings_in_any / len(df) * 100:.1f}%)")
    print(f"Parquet Output Destination    : {output_parquet_path}")
    print("=" * 65 + "\n")

    return df


def main() -> None:
    """CLI execution entry point."""
    enrich_spatial_mobility_data()


if __name__ == "__main__":
    main()
