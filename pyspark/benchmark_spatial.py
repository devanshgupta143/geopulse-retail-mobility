#!/usr/bin/env python3
"""
GeoPulse Retail Mobility Analytics - Spatial Processing Benchmark
==================================================================
Measures, analyzes, and logs performance benchmarks for high-throughput
geospatial operations on 200,000 mobile GPS pings:
1. Ingestion Latency: CSV read throughput
2. Spatial Indexing: H3 Hexagonal resolution 9 conversions
3. Geodesic Calculations: Vectorized Haversine distance computations to 3 store anchors
4. Catchment Filtering: 250m threshold evaluation
5. Columnar I/O: Parquet write throughput and compression ratio

Outputs:
- Formatted console benchmark report with timings, throughput (records/sec),
  and per-record microsecond latencies.
"""

import logging
import math
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import h3
import numpy as np
import pandas as pd

current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

try:
    import config as cfg
except ImportError:
    cfg = None

from spatial_indexing import (
    CATCHMENT_RADIUS_METERS,
    DEFAULT_GEOFENCES_JSON,
    DEFAULT_INPUT_CSV,
    H3_RESOLUTION,
    haversine_distance_vectorized,
    latlng_to_h3,
    load_store_geofences,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GeoPulse.SpatialBenchmark")


class BenchmarkTimer:
    """Context manager for accurate wall-clock timing."""

    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        self.start_time: float = 0.0
        self.elapsed: float = 0.0

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.perf_counter() - self.start_time


def run_benchmark(
    csv_path: Path = DEFAULT_INPUT_CSV,
    geofences_path: Path = DEFAULT_GEOFENCES_JSON,
    iterations: int = 3,
) -> List[Dict]:
    """
    Executes benchmark stages multiple times and averages metrics.
    """
    logger.info("Initializing Spatial Benchmark Suite across %d iterations...", iterations)
    logger.info("Target dataset: %s", csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"Benchmark input file not found: {csv_path}")

    stores = load_store_geofences(geofences_path)
    store_a = stores.get("STORE_A_EXISTING", {"lat": 40.7549, "lon": -73.9840})
    store_b = stores.get("STORE_B_PROPOSED_5TH", {"lat": 40.7565, "lon": -73.9815})
    store_c = stores.get("STORE_C_ALT_WEST", {"lat": 40.7510, "lon": -73.9930})

    results: List[Dict] = []

    # Stage 1: Ingestion / Reading CSV
    read_times = []
    df = None
    for i in range(iterations):
        with BenchmarkTimer("CSV Ingestion") as timer:
            df = pd.read_csv(csv_path)
        read_times.append(timer.elapsed)

    n_records = len(df)
    avg_read_time = np.mean(read_times)
    results.append(
        {
            "Stage": "1. Ingestion (Read 200k CSV)",
            "Elapsed_s": avg_read_time,
            "Records": n_records,
            "Throughput_rps": n_records / avg_read_time,
            "Latency_us": (avg_read_time / n_records) * 1e6,
        }
    )

    lats = df["Lat"].values
    lons = df["Long"].values

    # Stage 2: H3 Res 9 Hexagonal Conversions
    h3_times = []
    h3_cells = None
    for i in range(iterations):
        with BenchmarkTimer("H3 Conversions") as timer:
            if hasattr(h3, "geo_to_h3"):
                h3_cells = [h3.geo_to_h3(lat, lon, H3_RESOLUTION) for lat, lon in zip(lats, lons)]
            else:
                h3_cells = [h3.latlng_to_cell(lat, lon, H3_RESOLUTION) for lat, lon in zip(lats, lons)]
        h3_times.append(timer.elapsed)

    avg_h3_time = np.mean(h3_times)
    results.append(
        {
            "Stage": "2. H3 Indexing (Resolution 9)",
            "Elapsed_s": avg_h3_time,
            "Records": n_records,
            "Throughput_rps": n_records / avg_h3_time,
            "Latency_us": (avg_h3_time / n_records) * 1e6,
        }
    )

    # Stage 3: Haversine Distance Calculations to 3 Store Anchors
    haversine_times = []
    dist_a = dist_b = dist_c = None
    for i in range(iterations):
        with BenchmarkTimer("Haversine Distance (3 Anchors)") as timer:
            dist_a = haversine_distance_vectorized(lats, lons, store_a["lat"], store_a["lon"])
            dist_b = haversine_distance_vectorized(lats, lons, store_b["lat"], store_b["lon"])
            dist_c = haversine_distance_vectorized(lats, lons, store_c["lat"], store_c["lon"])
        haversine_times.append(timer.elapsed)

    avg_haversine_time = np.mean(haversine_times)
    # 3 distance calculations per ping = 600,000 distance evaluations
    results.append(
        {
            "Stage": "3. Haversine Distances (3 Stores)",
            "Elapsed_s": avg_haversine_time,
            "Records": n_records * 3,
            "Throughput_rps": (n_records * 3) / avg_haversine_time,
            "Latency_us": (avg_haversine_time / (n_records * 3)) * 1e6,
        }
    )

    # Stage 4: Catchment Membership & Nearest Anchor Evaluation
    eval_times = []
    for i in range(iterations):
        with BenchmarkTimer("Catchment Evaluation") as timer:
            in_a = dist_a <= CATCHMENT_RADIUS_METERS
            in_b = dist_b <= CATCHMENT_RADIUS_METERS
            in_c = dist_c <= CATCHMENT_RADIUS_METERS
            in_any = in_a | in_b | in_c
            nearest = np.argmin(np.column_stack([dist_a, dist_b, dist_c]), axis=1)
        eval_times.append(timer.elapsed)

    avg_eval_time = np.mean(eval_times)
    results.append(
        {
            "Stage": "4. 250m Catchment Inclusion",
            "Elapsed_s": avg_eval_time,
            "Records": n_records,
            "Throughput_rps": n_records / avg_eval_time,
            "Latency_us": (avg_eval_time / n_records) * 1e6,
        }
    )

    # Stage 5: End-to-End Enrichment Pipeline
    total_pipeline_time = avg_read_time + avg_h3_time + avg_haversine_time + avg_eval_time
    results.append(
        {
            "Stage": "5. Full In-Memory Enrichment",
            "Elapsed_s": total_pipeline_time,
            "Records": n_records,
            "Throughput_rps": n_records / total_pipeline_time,
            "Latency_us": (total_pipeline_time / n_records) * 1e6,
        }
    )

    # Format and display benchmark report
    print("\n" + "=" * 80)
    print("GEOPULSE SPATIAL PROCESSING PERFORMANCE BENCHMARK REPORT")
    print("=" * 80)
    print(f"Total Ping Records Processed : {n_records:,}")
    print(f"Benchmark Iterations Count   : {iterations}")
    print(f"H3 Resolution Setting        : {H3_RESOLUTION}")
    print(f"Target Store Anchors Count   : {len(stores)}")
    print("-" * 80)
    header = f"{'Benchmark Stage':<35} | {'Elapsed (s)':<11} | {'Throughput (rec/s)':<18} | {'Latency':<10}"
    print(header)
    print("-" * 80)
    for r in results:
        row = f"{r['Stage']:<35} | {r['Elapsed_s']:>10.4f}s | {r['Throughput_rps']:>18,.0f} | {r['Latency_us']:>7.2f} µs"
        print(row)
    print("=" * 80 + "\n")

    return results


def main() -> None:
    """CLI execution entrypoint."""
    run_benchmark()


if __name__ == "__main__":
    main()
