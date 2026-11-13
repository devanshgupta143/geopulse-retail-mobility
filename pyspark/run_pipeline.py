#!/usr/bin/env python3
"""
GeoPulse Retail Mobility Analytics - End-to-End Pipeline Orchestrator
=====================================================================
Sequentially orchestrates the complete retail mobility spatial engineering workflow:
1. Environment & Configuration Verification
2. Raw Data Ingestion & Generation (Mock GPS pings + Geofences)
3. H3 Spatial Indexing & Catchment Distance Enrichment (Parquet output)
4. Spatial Aggregations by H3 Hexagon and Retail Catchment Zones
5. Hive-Partitioned Parquet Export for High-Throughput Analytics

Usage:
    python pyspark/run_pipeline.py
"""

import logging
import sys
import time
from pathlib import Path
from typing import Dict, Optional

# Path configuration
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GeoPulse.Orchestrator")


def print_banner(stage_num: int, title: str) -> None:
    """Prints a styled stage banner."""
    print("\n" + "=" * 75)
    print(f"STAGE {stage_num}: {title.upper()}")
    print("=" * 75)


def run_pipeline() -> bool:
    """Executes all pipeline stages sequentially."""
    total_start = time.perf_counter()
    logger.info("Starting GeoPulse Retail Mobility Processing Pipeline...")

    # -------------------------------------------------------------
    # STAGE 1: Environment & Raw Data Verification
    # -------------------------------------------------------------
    print_banner(1, "Data Ingestion & Store Geofence Verification")
    csv_path = current_dir / "mock_gps_pings.csv"
    json_path = current_dir / "store_geofences.json"

    if not csv_path.exists() or not json_path.exists():
        logger.info("Raw GPS pings or geofences missing. Running synthesis engine...")
        from generate_gps_pings import main as generate_data
        generate_data()
    else:
        logger.info("Found input GPS pings at: %s (%.2f MB)", csv_path, csv_path.stat().st_size / (1024 * 1024))
        logger.info("Found store geofences at: %s", json_path)

    # -------------------------------------------------------------
    # STAGE 2: Spatial Indexing & Catchment Enrichment
    # -------------------------------------------------------------
    print_banner(2, "H3 Hexagonal Indexing & Geodesic Distance Enrichment")
    t0 = time.perf_counter()
    from spatial_indexing import enrich_spatial_mobility_data
    df_enriched = enrich_spatial_mobility_data()
    t_stage2 = time.perf_counter() - t0
    logger.info("Stage 2 completed in %.2f seconds.", t_stage2)

    # -------------------------------------------------------------
    # STAGE 3: Spatial Aggregations by H3 & Store Zones
    # -------------------------------------------------------------
    print_banner(3, "Spatial Aggregations by H3 Index & Store Zones")
    t0 = time.perf_counter()
    from spatial_aggregations import run_spatial_aggregations
    df_agg = run_spatial_aggregations()
    t_stage3 = time.perf_counter() - t0
    logger.info("Stage 3 completed in %.2f seconds.", t_stage3)

    # -------------------------------------------------------------
    # STAGE 4: Optimized Partitioned Parquet Export
    # -------------------------------------------------------------
    print_banner(4, "Hive-Partitioned Parquet Export (Analytical Storage)")
    t0 = time.perf_counter()
    from optimize_parquet import (
        OUTPUT_PARTITIONED_DIR,
        verify_partitioned_dataset,
        write_partitioned_parquet,
    )
    write_partitioned_parquet(df_agg)
    partition_stats = verify_partitioned_dataset(OUTPUT_PARTITIONED_DIR)
    t_stage4 = time.perf_counter() - t0
    logger.info("Stage 4 completed in %.2f seconds.", t_stage4)

    # -------------------------------------------------------------
    # PIPELINE EXECUTION SUMMARY
    # -------------------------------------------------------------
    total_elapsed = time.perf_counter() - total_start
    print("\n" + "=" * 75)
    print("GEOPULSE END-TO-END PIPELINE EXECUTION SUMMARY")
    print("=" * 75)
    print(f"Total Pipeline Runtime         : {total_elapsed:.2f} seconds")
    print(f"Stage 2 (Spatial Indexing)     : {t_stage2:.2f}s (200,000 pings enriched)")
    print(f"Stage 3 (Spatial Aggregations) : {t_stage3:.2f}s ({len(df_agg)} aggregated groups)")
    print(f"Stage 4 (Partitioned Parquet)  : {t_stage4:.2f}s ({len(partition_stats)} partitions created)")
    print("-" * 75)
    print("Key Artifacts Generated:")
    print(f" - Spatially Enriched Parquet  : {current_dir / 'output' / 'enriched_gps_pings.parquet'}")
    print(f" - Aggregated Metrics Parquet  : {current_dir / 'output' / 'spatial_aggregations.parquet'}")
    print(f" - Hive-Partitioned Dataset    : {OUTPUT_PARTITIONED_DIR}")
    print("=" * 75 + "\n")

    logger.info("All pipeline stages executed successfully.")
    return True


def main() -> None:
    """CLI execution entrypoint."""
    success = run_pipeline()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
