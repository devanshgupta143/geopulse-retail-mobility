#!/usr/bin/env python3
"""
GeoPulse Retail Mobility Analytics - Partitioned Parquet Optimization
====================================================================
Reads aggregated mobility metrics and writes them out as partitioned
Parquet files organized by `store_id` and `date` using Hive-style partitioning.

Hive Partition Scheme:
`pyspark/output/partitioned_metrics/store_id=<store_id>/date=<YYYY-MM-DD>/`

Optimizations applied:
- Hive-style directory partitioning for efficient predicate pushdown (Pruning)
- Snappy columnar compression for high throughput analytical queries
- Dictionary encoding for low-cardinality categorical columns
- Schema validation and partition round-trip verification
"""

import logging
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Path setup
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GeoPulse.OptimizeParquet")

INPUT_AGGREGATIONS_PATH = current_dir / "output" / "spatial_aggregations.parquet"
OUTPUT_PARTITIONED_DIR = current_dir / "output" / "partitioned_metrics"


def load_aggregated_metrics(input_path: Path = INPUT_AGGREGATIONS_PATH) -> pd.DataFrame:
    """Loads aggregated mobility metrics dataset."""
    if not input_path.exists():
        logger.warning(
            "Aggregated metrics not found at %s. Running spatial aggregations pipeline...",
            input_path,
        )
        from spatial_aggregations import run_spatial_aggregations
        run_spatial_aggregations()

    logger.info("Reading aggregated mobility metrics from %s...", input_path)
    df = pd.read_parquet(input_path)
    logger.info("Loaded %d rows across %d columns.", len(df), len(df.columns))
    return df


def write_partitioned_parquet(
    df: pd.DataFrame,
    output_dir: Path = OUTPUT_PARTITIONED_DIR,
    partition_cols: Optional[List[str]] = None,
) -> Path:
    """
    Writes the DataFrame to Hive-style partitioned Parquet files organized by store ID and date.
    
    Args:
        df: Input DataFrame with aggregated spatial mobility metrics.
        output_dir: Root directory for partitioned Parquet datasets.
        partition_cols: Columns used to partition files (default: ['store_id', 'date']).
        
    Returns:
        Path: Path to the generated partitioned dataset directory.
    """
    if partition_cols is None:
        partition_cols = ["store_id", "date"]

    logger.info(
        "Preparing partitioned parquet export with partition columns: %s",
        partition_cols,
    )

    # Ensure required partition columns exist
    for col in partition_cols:
        if col not in df.columns:
            raise KeyError(f"Required partition column '{col}' not found in DataFrame.")

    # Clean previous partition directory if present
    if output_dir.exists():
        logger.info("Cleaning existing partition directory at %s...", output_dir)
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert to PyArrow Table with explicit types
    logger.info("Converting DataFrame to PyArrow Table and configuring compression...")
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write partitioned dataset
    logger.info("Writing partitioned Parquet files to %s...", output_dir)
    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=partition_cols,
        compression="SNAPPY",
        use_dictionary=True,
    )
    logger.info("Partitioned Parquet export completed successfully.")
    return output_dir


def verify_partitioned_dataset(output_dir: Path = OUTPUT_PARTITIONED_DIR) -> Dict[str, int]:
    """
    Validates partitioned directory layout, reads partitions back,
    and returns record counts per partition.
    """
    logger.info("Verifying partitioned Parquet dataset at %s...", output_dir)
    partition_files = list(output_dir.glob("**/*.parquet"))
    logger.info("Found %d parquet part files across partitions.", len(partition_files))

    # Read dataset back using PyArrow / Pandas dataset reader
    df_readback = pd.read_parquet(output_dir)
    logger.info("Read back %d rows successfully from partitioned dataset.", len(df_readback))

    partition_stats = {}
    for part_file in partition_files:
        rel_path = part_file.relative_to(output_dir)
        part_name = str(rel_path.parent).replace("\\", "/")
        partition_stats[part_name] = len(pd.read_parquet(part_file))

    print("\n" + "=" * 70)
    print("GEOPULSE PARTITIONED PARQUET EXPORT SUMMARY")
    print("=" * 70)
    print(f"Partition Root Directory     : {output_dir}")
    print(f"Total Part Files Generated    : {len(partition_files)}")
    print(f"Total Records Readback        : {len(df_readback):,}")
    print("\nPartitions Discovered:")
    for part_name, count in sorted(partition_stats.items()):
        print(f" - {part_name:<45} : {count:>4} rows")
    print("=" * 70 + "\n")

    return partition_stats


def main() -> None:
    """CLI execution entry point."""
    df = load_aggregated_metrics()
    write_partitioned_parquet(df)
    verify_partitioned_dataset()


if __name__ == "__main__":
    main()
