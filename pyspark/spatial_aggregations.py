#!/usr/bin/env python3
"""
GeoPulse Retail Mobility Analytics - Spatial Aggregations Pipeline
==================================================================
Aggregates spatially enriched GPS pings by H3 hexagonal index (resolution 9)
and store catchment zones to compute mobility foot-traffic metrics:
- Total visit counts (pings)
- Unique mobile device counts
- Catchment dwell and proximity metrics
- Peak commuter transit window attribution (07:00-09:59)

Outputs:
- `pyspark/output/spatial_aggregations.parquet`
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# Setup path and config
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

try:
    import config as cfg
except ImportError:
    cfg = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GeoPulse.SpatialAggregations")

INPUT_PARQUET_PATH = current_dir / "output" / "enriched_gps_pings.parquet"
OUTPUT_PARQUET_PATH = current_dir / "output" / "spatial_aggregations.parquet"

STORE_COLUMNS = [
    ("STORE_A_EXISTING", "in_catchment_store_a", "dist_to_store_a_m"),
    ("STORE_B_PROPOSED_5TH", "in_catchment_store_b", "dist_to_store_b_m"),
    ("STORE_C_ALT_WEST", "in_catchment_store_c", "dist_to_store_c_m"),
]


def load_enriched_pings(input_path: Path = INPUT_PARQUET_PATH) -> pd.DataFrame:
    """Loads enriched GPS pings dataset."""
    if not input_path.exists():
        logger.warning("Enriched dataset not found at %s. Attempting to generate...", input_path)
        from spatial_indexing import enrich_spatial_mobility_data
        enrich_spatial_mobility_data()

    logger.info("Reading enriched GPS pings from %s...", input_path)
    df = pd.read_parquet(input_path)
    logger.info("Loaded %d rows across %d columns.", len(df), len(df.columns))
    return df


def aggregate_by_h3_and_store(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expands store catchment memberships and aggregates by H3 index and store zone.
    
    Computes:
    - total_visits (ping count)
    - unique_devices (unique mobile device count)
    - avg_dist_to_store_m
    - peak_hour_visits (07:00 to 09:59)
    """
    logger.info("Parsing timestamps and temporal features...")
    # Extract date and time features
    df["Timestamp_dt"] = pd.to_datetime(df["Timestamp"])
    df["date"] = df["Timestamp_dt"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["Timestamp_dt"].dt.hour
    df["is_morning_peak"] = df["hour"].between(7, 9)

    # Transform wide catchment flags into store-attributed visit records
    store_records: List[pd.DataFrame] = []

    for store_id, catchment_col, dist_col in STORE_COLUMNS:
        store_pings = df[df[catchment_col]].copy()
        if len(store_pings) > 0:
            store_pings["store_id"] = store_id
            store_pings["dist_to_store_m"] = store_pings[dist_col]
            store_records.append(
                store_pings[
                    [
                        "DeviceID",
                        "h3_index_res9",
                        "store_id",
                        "date",
                        "hour",
                        "is_morning_peak",
                        "dist_to_store_m",
                    ]
                ]
            )

    # Also capture pings outside any catchment for complete spatial coverage
    outside_pings = df[~df["in_any_catchment"]].copy()
    if len(outside_pings) > 0:
        outside_pings["store_id"] = "OUTSIDE_CATCHMENT"
        outside_pings["dist_to_store_m"] = outside_pings["min_dist_to_store_m"]
        store_records.append(
            outside_pings[
                [
                    "DeviceID",
                    "h3_index_res9",
                    "store_id",
                    "date",
                    "hour",
                    "is_morning_peak",
                    "dist_to_store_m",
                ]
            ]
        )

    all_store_pings = pd.concat(store_records, ignore_index=True)
    logger.info(
        "Attributed %d total zone visits across store catchments and unassigned zones.",
        len(all_store_pings),
    )

    # Aggregate by (store_id, h3_index_res9, date)
    logger.info("Computing spatial aggregations by (store_id, h3_index_res9, date)...")
    agg_df = (
        all_store_pings.groupby(["store_id", "h3_index_res9", "date"], as_index=False)
        .agg(
            total_visits=("DeviceID", "count"),
            unique_devices=("DeviceID", "nunique"),
            avg_dist_to_store_m=("dist_to_store_m", "mean"),
            min_dist_to_store_m=("dist_to_store_m", "min"),
            max_dist_to_store_m=("dist_to_store_m", "max"),
            morning_peak_visits=("is_morning_peak", "sum"),
            morning_peak_unique_devices=("DeviceID", lambda s: s[all_store_pings.loc[s.index, "is_morning_peak"]].nunique()),
        )
    )

    agg_df["avg_dist_to_store_m"] = agg_df["avg_dist_to_store_m"].round(2)
    agg_df["min_dist_to_store_m"] = agg_df["min_dist_to_store_m"].round(2)
    agg_df["max_dist_to_store_m"] = agg_df["max_dist_to_store_m"].round(2)

    # Sort deterministically
    agg_df.sort_values(
        by=["store_id", "total_visits", "unique_devices"],
        ascending=[True, False, False],
        inplace=True,
    )
    agg_df.reset_index(drop=True, inplace=True)
    return agg_df


def run_spatial_aggregations(
    input_path: Path = INPUT_PARQUET_PATH,
    output_path: Path = OUTPUT_PARQUET_PATH,
) -> pd.DataFrame:
    """Executes the spatial aggregation pipeline and saves Parquet output."""
    df_raw = load_enriched_pings(input_path)
    agg_df = aggregate_by_h3_and_store(df_raw)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing aggregated spatial metrics to %s...", output_path)
    agg_df.to_parquet(output_path, engine="pyarrow", index=False)
    logger.info("Output saved. File size: %.2f KB", output_path.stat().st_size / 1024)

    # Print summary
    print("\n" + "=" * 70)
    print("GEOPULSE SPATIAL AGGREGATIONS SUMMARY")
    print("=" * 70)
    print(f"Total Aggregated Rows (Store x H3 x Date) : {len(agg_df):,}")
    print(f"Unique H3 Hexagons (Res 9)               : {agg_df['h3_index_res9'].nunique():,}")
    print(f"Total Aggregated Visits                  : {agg_df['total_visits'].sum():,}")
    print("\nMetrics by Store Zone:")
    store_summary = (
        agg_df.groupby("store_id")
        .agg(
            h3_cells=("h3_index_res9", "nunique"),
            total_visits=("total_visits", "sum"),
            unique_devices=("unique_devices", "max"),
            morning_peak_visits=("morning_peak_visits", "sum"),
        )
    )
    print(store_summary.to_string())
    print("\nTop 5 H3 Cells by Total Visits:")
    top_5 = agg_df.head(5)[["store_id", "h3_index_res9", "total_visits", "unique_devices", "morning_peak_visits"]]
    print(top_5.to_string(index=False))
    print(f"\nSaved to: {output_path}")
    print("=" * 70 + "\n")

    return agg_df


def main() -> None:
    """CLI execution entrypoint."""
    run_spatial_aggregations()


if __name__ == "__main__":
    main()
