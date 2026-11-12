#!/usr/bin/env python3
"""
GeoPulse Retail Mobility Analytics - GPS Ping Synthesis Engine
==============================================================
Simulates high-throughput mobile device GPS pings across Manhattan retail store zones
to quantify foot-traffic dynamics, catchment coverage, and cannibalization risk.

Specifications:
- Total unique mobile devices: 5,000
- Pings per device: 40 (200,000 total records)
- Temporal range: 16-hour retail day (06:00:00 to 22:00:00 on 2026-11-01)
- Spatial anchors (Manhattan, NY):
    * STORE_A_EXISTING:    Lat 40.7549, Lon -73.9840 (Herald Square Flagship)
    * STORE_B_PROPOSED_5TH: Lat 40.7565, Lon -73.9815 (5th Ave Luxury Corridor)
    * STORE_C_ALT_WEST:     Lat 40.7510, Lon -73.9930 (West 34th District Alternative)
- Commuter overlap & cannibalization injection:
    * 30% of devices commute between STORE_A_EXISTING and STORE_B_PROPOSED_5TH
      during morning peak transit hours (07:00:00–09:59:59).
    * Pings cluster around store anchors with Gaussian noise (std=0.0025).
- Output datasets:
    * pyspark/mock_gps_pings.csv [DeviceID, Lat, Long, Timestamp]
    * pyspark/store_geofences.json (Store metadata, coordinates & 250m catchment radius)
"""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GeoPulse.GPSGenerator")

# ==========================================
# CONSTANTS & CONFIGURATION
# ==========================================
RANDOM_SEED = 42
TOTAL_DEVICES = 5000
PINGS_PER_DEVICE = 40
TOTAL_RECORDS = TOTAL_DEVICES * PINGS_PER_DEVICE  # 200,000

SIMULATION_DATE = "2026-11-01"
DAY_START_TIME = datetime.strptime(f"{SIMULATION_DATE} 06:00:00", "%Y-%m-%d %H:%M:%S")
DAY_END_TIME = datetime.strptime(f"{SIMULATION_DATE} 22:00:00", "%Y-%m-%d %H:%M:%S")

PEAK_START_TIME = datetime.strptime(f"{SIMULATION_DATE} 07:00:00", "%Y-%m-%d %H:%M:%S")
PEAK_END_TIME = datetime.strptime(f"{SIMULATION_DATE} 09:59:59", "%Y-%m-%d %H:%M:%S")

COMMUTER_OVERLAP_RATIO = 0.30  # 30% of devices commute between Store A and Store B
GAUSSIAN_NOISE_STD = 0.0025    # ~250m-270m spatial dispersion standard deviation

# Store anchors & metadata
STORE_CONFIGS = {
    "STORE_A_EXISTING": {
        "store_id": "STORE_A_EXISTING",
        "name": "Herald Square Flagship",
        "status": "EXISTING",
        "lat": 40.7549,
        "lon": -73.9840,
        "catchment_radius_meters": 250,
        "address": "151 W 34th St, New York, NY 10001",
        "borough": "Manhattan",
        "neighborhood": "Herald Square / Midtown South",
    },
    "STORE_B_PROPOSED_5TH": {
        "store_id": "STORE_B_PROPOSED_5TH",
        "name": "Fifth Avenue Luxury Corridor",
        "status": "PROPOSED",
        "lat": 40.7565,
        "lon": -73.9815,
        "catchment_radius_meters": 250,
        "address": "500 5th Ave, New York, NY 10110",
        "borough": "Manhattan",
        "neighborhood": "Midtown East / 5th Ave",
    },
    "STORE_C_ALT_WEST": {
        "store_id": "STORE_C_ALT_WEST",
        "name": "West 34th District Alternative",
        "status": "ALTERNATIVE",
        "lat": 40.7510,
        "lon": -73.9930,
        "catchment_radius_meters": 250,
        "address": "400 W 34th St, New York, NY 10001",
        "borough": "Manhattan",
        "neighborhood": "Hudson Yards / Penn Station West",
    },
}


def export_store_geofences(output_path: Path) -> None:
    """Exports store geofences metadata including target 250m catchment radius to JSON."""
    logger.info("Exporting store geofences to %s", output_path)
    
    geofence_payload = {
        "metadata": {
            "project": "GeoPulse Retail Mobility Analytics",
            "study_area": "Midtown Manhattan, NY",
            "default_catchment_radius_meters": 250,
            "crs": "EPSG:4326 (WGS84)",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "stores": list(STORE_CONFIGS.values()),
        "geojson": {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [store["lon"], store["lat"]],
                    },
                    "properties": {
                        "store_id": store["store_id"],
                        "name": store["name"],
                        "status": store["status"],
                        "catchment_radius_meters": store["catchment_radius_meters"],
                        "address": store["address"],
                        "borough": store["borough"],
                        "neighborhood": store["neighborhood"],
                    },
                }
                for store in STORE_CONFIGS.values()
            ],
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geofence_payload, f, indent=2)
    logger.info("Successfully exported %d store geofences.", len(STORE_CONFIGS))


def generate_mobility_dataset(
    total_devices: int = TOTAL_DEVICES,
    pings_per_device: int = PINGS_PER_DEVICE,
    seed: int = RANDOM_SEED,
) -> pd.DataFrame:
    """
    Synthesizes GPS mobility pings for mobile devices across a 16-hour retail day.
    
    Models:
    - 30% commuter devices exhibiting foot-traffic cannibalization between
      STORE_A_EXISTING and STORE_B_PROPOSED_5TH during morning peak hours (07:00-09:59).
    - 70% anchor visitors clustered around Store A, B, or C with Gaussian noise (std=0.0025).
    """
    logger.info(
        "Initializing GPS mobility synthesis: %d devices x %d pings = %d records",
        total_devices,
        pings_per_device,
        total_devices * pings_per_device,
    )
    rng = np.random.default_rng(seed)

    # 1. Deterministic UUID generation for 5,000 devices
    device_uuids = [
        str(uuid.UUID(bytes=rng.bytes(16), version=4))
        for _ in range(total_devices)
    ]

    # Device cohort split: 30% commuters, 70% regular anchor visitors
    num_commuters = int(total_devices * COMMUTER_OVERLAP_RATIO)  # 1,500
    num_regular = total_devices - num_commuters                  # 3,500

    logger.info(
        "Device cohorts: %d commuter devices (30%% cannibalization study), %d regular anchor devices",
        num_commuters,
        num_regular,
    )

    store_a = STORE_CONFIGS["STORE_A_EXISTING"]
    store_b = STORE_CONFIGS["STORE_B_PROPOSED_5TH"]
    store_c = STORE_CONFIGS["STORE_C_ALT_WEST"]
    regular_stores = [store_a, store_b, store_c]

    # Pre-calculate time boundary seconds relative to DAY_START_TIME
    day_total_seconds = int((DAY_END_TIME - DAY_START_TIME).total_seconds())  # 16 * 3600 = 57600
    peak_start_sec = int((PEAK_START_TIME - DAY_START_TIME).total_seconds())   # 1 * 3600 = 3600
    peak_end_sec = int((PEAK_END_TIME - DAY_START_TIME).total_seconds())       # 3600*4 - 1 = 14399
    
    # Morning peak window length = 10,800 seconds (07:00 to 10:00)
    peak_duration_sec = peak_end_sec - peak_start_sec

    records: List[Tuple[str, float, float, str]] = []

    # ==========================================
    # COHORT 1: COMMUTER CANNIBALIZATION DEVICES
    # ==========================================
    # Devices commute between Store A and Store B during 07:00 - 09:59
    # Pings during peak hours transition along corridor between Store A & B
    # Remaining pings cluster around Store A or B
    pings_in_peak = 12
    pings_off_peak = pings_per_device - pings_in_peak  # 28

    off_peak_seconds_1 = list(range(0, peak_start_sec))
    off_peak_seconds_2 = list(range(peak_end_sec + 1, day_total_seconds))
    off_peak_all = np.array(off_peak_seconds_1 + off_peak_seconds_2)

    for i in range(num_commuters):
        dev_id = device_uuids[i]

        # Morning peak timestamps (07:00 to 09:59:59)
        peak_secs = rng.choice(range(peak_start_sec, peak_end_sec + 1), size=pings_in_peak, replace=False)
        peak_secs.sort()

        # Commute transition factor t from Store A (0.0) to Store B (1.0), or vice versa
        reverse_direction = rng.random() > 0.5
        t_steps = np.linspace(0.0, 1.0, pings_in_peak)
        if reverse_direction:
            t_steps = 1.0 - t_steps

        # Corridor coordinates + Gaussian noise
        corridor_lat = store_a["lat"] + t_steps * (store_b["lat"] - store_a["lat"])
        corridor_lon = store_a["lon"] + t_steps * (store_b["lon"] - store_a["lon"])

        noise_lat_peak = rng.normal(0, GAUSSIAN_NOISE_STD, size=pings_in_peak)
        noise_lon_peak = rng.normal(0, GAUSSIAN_NOISE_STD, size=pings_in_peak)

        pings_peak_lat = corridor_lat + noise_lat_peak
        pings_peak_lon = corridor_lon + noise_lon_peak

        # Off-peak timestamps (06:00-06:59 and 10:00-22:00)
        off_peak_secs = rng.choice(off_peak_all, size=pings_off_peak, replace=False)
        off_peak_secs.sort()

        # Anchor for off-peak: primarily Store A or Store B
        anchor_store = store_b if rng.random() > 0.5 else store_a
        noise_lat_off = rng.normal(0, GAUSSIAN_NOISE_STD, size=pings_off_peak)
        noise_lon_off = rng.normal(0, GAUSSIAN_NOISE_STD, size=pings_off_peak)

        pings_off_lat = anchor_store["lat"] + noise_lat_off
        pings_off_lon = anchor_store["lon"] + noise_lon_off

        # Combine and sort all 40 pings chronologically
        all_secs = np.concatenate([peak_secs, off_peak_secs])
        all_lats = np.concatenate([pings_peak_lat, pings_off_lat])
        all_lons = np.concatenate([pings_peak_lon, pings_off_lon])

        sort_idx = np.argsort(all_secs)
        for s_idx in sort_idx:
            ts = (DAY_START_TIME + timedelta(seconds=int(all_secs[s_idx]))).strftime("%Y-%m-%d %H:%M:%S")
            records.append((
                dev_id,
                round(float(all_lats[s_idx]), 6),
                round(float(all_lons[s_idx]), 6),
                ts,
            ))

    # ==========================================
    # COHORT 2: REGULAR ANCHOR VISITORS
    # ==========================================
    # Devices visit their assigned retail anchor across the 16-hour day
    # Probabilities: Store A (38%), Store B (32%), Store C (30%)
    store_weights = [0.38, 0.32, 0.30]
    assigned_store_indices = rng.choice(len(regular_stores), size=num_regular, p=store_weights)

    for idx, store_idx in enumerate(assigned_store_indices):
        dev_id = device_uuids[num_commuters + idx]
        store = regular_stores[store_idx]

        # 40 timestamps sampled across the day
        sampled_secs = rng.choice(day_total_seconds, size=pings_per_device, replace=False)
        sampled_secs.sort()

        noise_lat = rng.normal(0, GAUSSIAN_NOISE_STD, size=pings_per_device)
        noise_lon = rng.normal(0, GAUSSIAN_NOISE_STD, size=pings_per_device)

        dev_lats = store["lat"] + noise_lat
        dev_lons = store["lon"] + noise_lon

        for k in range(pings_per_device):
            ts = (DAY_START_TIME + timedelta(seconds=int(sampled_secs[k]))).strftime("%Y-%m-%d %H:%M:%S")
            records.append((
                dev_id,
                round(float(dev_lats[k]), 6),
                round(float(dev_lons[k]), 6),
                ts,
            ))

    # Construct DataFrame
    df = pd.DataFrame(records, columns=["DeviceID", "Lat", "Long", "Timestamp"])
    logger.info("Successfully synthesized %d records across %d devices.", len(df), df["DeviceID"].nunique())
    return df


def main() -> None:
    """CLI execution entrypoint for GeoPulse mobility data synthesis."""
    script_dir = Path(__file__).resolve().parent
    csv_output_path = script_dir / "mock_gps_pings.csv"
    json_output_path = script_dir / "store_geofences.json"

    logger.info("Starting GeoPulse Retail Mobility data synthesis...")

    # 1. Export store geofence configuration
    export_store_geofences(json_output_path)

    # 2. Synthesize GPS mobility pings
    df = generate_mobility_dataset()

    # 3. Save to CSV
    logger.info("Writing synthesized GPS pings to %s...", csv_output_path)
    df.to_csv(csv_output_path, index=False)
    logger.info("Output saved. File size: %.2f MB", csv_output_path.stat().st_size / (1024 * 1024))

    # 4. Verification summary
    print("\n" + "=" * 60)
    print("GEOPULSE MOBILITY DATA SYNTHESIS SUMMARY")
    print("=" * 60)
    print(f"Total Rows Generated   : {len(df):,}")
    print(f"Unique Devices         : {df['DeviceID'].nunique():,}")
    print(f"Pings per Device       : {len(df) // df['DeviceID'].nunique()}")
    print(f"Columns                : {list(df.columns)}")
    print(f"Min Timestamp          : {df['Timestamp'].min()}")
    print(f"Max Timestamp          : {df['Timestamp'].max()}")
    print(f"CSV Path               : {csv_output_path}")
    print(f"Geofences JSON Path    : {json_output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
