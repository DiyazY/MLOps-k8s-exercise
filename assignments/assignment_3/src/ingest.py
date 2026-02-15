"""
Bronze Layer: Incremental data ingestion with simulated quality issues.

Ingests batches one at a time into the bronze layer, introducing realistic
data quality problems (missing values, duplicates, dropped rows) and adding
ingestion metadata for lineage tracking.
"""

import os
import argparse
from datetime import datetime

import yaml
import numpy as np
import pandas as pd


def load_params():
    """Load pipeline parameters from params.yaml."""
    with open("params.yaml", "r") as f:
        return yaml.safe_load(f)


def simulate_quality_issues(df, params, rng):
    """
    Introduce realistic data quality problems into a batch.

    - Randomly drop a fraction of rows
    - Inject missing values (NaN) into random cells
    - Duplicate a fraction of rows (simulating duplicate events)
    """
    n_original = len(df)
    numeric_cols = ["meantemp", "humidity", "wind_speed", "meanpressure"]

    # 1. Drop random rows
    drop_frac = params["drop_fraction"]
    n_drop = max(1, int(n_original * drop_frac))
    drop_idx = rng.choice(df.index, size=n_drop, replace=False)
    df = df.drop(drop_idx).reset_index(drop=True)
    print(f"  Dropped {n_drop} rows ({drop_frac*100:.0f}%)")

    # 2. Inject missing values into random cells
    missing_frac = params["missing_fraction"]
    n_missing = max(1, int(len(df) * len(numeric_cols) * missing_frac))
    for _ in range(n_missing):
        row_idx = rng.integers(0, len(df))
        col = rng.choice(numeric_cols)
        df.at[row_idx, col] = np.nan
    print(f"  Injected {n_missing} missing values ({missing_frac*100:.0f}%)")

    # 3. Duplicate random rows (simulating duplicate events)
    dup_frac = params["duplicate_fraction"]
    n_dup = max(1, int(len(df) * dup_frac))
    dup_idx = rng.choice(df.index, size=n_dup, replace=False)
    duplicates = df.loc[dup_idx].copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    df = df.sort_values("date").reset_index(drop=True)
    print(f"  Added {n_dup} duplicate rows ({dup_frac*100:.0f}%)")

    return df


def add_metadata(df, batch_id, source_file):
    """Add ingestion metadata columns for lineage tracking."""
    df = df.copy()
    df["batch_id"] = batch_id
    df["ingestion_timestamp"] = datetime.now().isoformat()
    df["source_file"] = source_file
    return df


def ingest_batch(batch_num, params):
    """Ingest a single batch into the bronze layer."""
    ingest_params = params["ingest"]
    batch_dir = ingest_params["batch_dir"]
    output_file = ingest_params["output_file"]
    seed = ingest_params["random_seed"]

    batch_file = os.path.join(batch_dir, f"batch_{batch_num}.csv")
    if not os.path.exists(batch_file):
        print(f"Batch file not found: {batch_file}")
        return

    # Use a seed derived from base seed + batch number for reproducibility
    rng = np.random.default_rng(seed + batch_num)

    print(f"\nIngesting batch {batch_num} from {batch_file}")
    batch_df = pd.read_csv(batch_file, parse_dates=["date"])
    print(f"  Original size: {len(batch_df)} rows")

    # Simulate quality issues
    batch_df = simulate_quality_issues(batch_df, ingest_params, rng)

    # Add metadata
    batch_df = add_metadata(batch_df, batch_num, f"batch_{batch_num}.csv")

    # Append to existing bronze data or create new
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if os.path.exists(output_file):
        existing = pd.read_csv(output_file, parse_dates=["date"])
        # Check if this batch was already ingested
        if batch_num in existing["batch_id"].values:
            print(f"  Batch {batch_num} already ingested, replacing...")
            existing = existing[existing["batch_id"] != batch_num]
        combined = pd.concat([existing, batch_df], ignore_index=True)
    else:
        combined = batch_df

    combined = combined.sort_values("date").reset_index(drop=True)
    combined.to_csv(output_file, index=False)

    print(f"  Bronze data now has {len(combined)} total rows -> {output_file}")
    return combined


def ingest_all(params):
    """Ingest all batches sequentially into the bronze layer."""
    n_batches = params["split"]["n_batches"]

    for batch_num in range(1, n_batches + 1):
        ingest_batch(batch_num, params)


def main():
    parser = argparse.ArgumentParser(description="Bronze layer ingestion")
    parser.add_argument(
        "--batch", type=int, default=None,
        help="Ingest a specific batch number (1-5). If not set, ingest all."
    )
    args = parser.parse_args()

    params = load_params()

    if args.batch is not None:
        ingest_batch(args.batch, params)
    else:
        ingest_all(params)


if __name__ == "__main__":
    main()
