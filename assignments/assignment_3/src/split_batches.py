"""
Split the raw Delhi Climate dataset into temporal batches.

Divides train.csv into N time-ordered batches of roughly equal size,
simulating incremental data arrivals over time.
"""

import os
import yaml
import numpy as np
import pandas as pd


def load_params():
    """Load pipeline parameters from params.yaml."""
    with open("params.yaml", "r") as f:
        return yaml.safe_load(f)


def split_into_batches(df, n_batches):
    """Split a time-sorted DataFrame into n roughly equal temporal batches."""
    df = df.sort_values("date").reset_index(drop=True)
    indices = np.array_split(range(len(df)), n_batches)
    batches = [df.iloc[idx].copy() for idx in indices]
    return batches


def main():
    params = load_params()
    split_params = params["split"]

    input_file = split_params["input_file"]
    output_dir = split_params["output_dir"]
    n_batches = split_params["n_batches"]

    # Load raw data
    df = pd.read_csv(input_file, parse_dates=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    print(f"Loaded {len(df)} records from {input_file}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    # Split into batches
    batches = split_into_batches(df, n_batches)

    os.makedirs(output_dir, exist_ok=True)

    for i, batch in enumerate(batches, start=1):
        output_path = os.path.join(output_dir, f"batch_{i}.csv")
        batch.to_csv(output_path, index=False)
        print(
            f"Batch {i}: {len(batch)} records "
            f"({batch['date'].min().date()} to {batch['date'].max().date()}) "
            f"-> {output_path}"
        )

    print(f"\nSplit complete: {n_batches} batches created in {output_dir}/")


if __name__ == "__main__":
    main()
