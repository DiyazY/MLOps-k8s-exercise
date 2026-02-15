"""
Gold Layer: Prepare ML-ready dataset for next-day temperature forecasting.

Takes the cleaned silver data and structures it as a supervised learning
dataset — selecting relevant features, creating the forecast target,
and removing metadata columns not needed for modelling.
"""

import os

import yaml
import pandas as pd


def load_params():
    """Load pipeline parameters from params.yaml."""
    with open("params.yaml", "r") as f:
        return yaml.safe_load(f)


def select_features(df, target, threshold):
    """
    Select numeric features with absolute correlation to the target
    above the given threshold. Drops metadata and non-numeric columns.
    """
    numeric_df = df.select_dtypes(include="number")

    if target not in numeric_df.columns:
        raise ValueError(f"Target column '{target}' not found in numeric columns")

    correlations = numeric_df.corr()[target].drop(target).abs()
    selected = correlations[correlations >= threshold].sort_values(ascending=False)

    print(f"  Feature correlations with '{target}' (threshold={threshold}):")
    for feat, corr in selected.items():
        print(f"    {feat}: {corr:.3f}")

    dropped = correlations[correlations < threshold]
    if len(dropped) > 0:
        print(f"  Dropped {len(dropped)} low-correlation features: {list(dropped.index)}")

    return list(selected.index)


def main():
    params = load_params()
    gold_params = params["gold"]

    input_file = gold_params["input_file"]
    output_file = gold_params["output_file"]
    target = gold_params["target"]
    horizon = gold_params["forecast_horizon"]
    corr_threshold = gold_params["correlation_threshold"]

    # Load silver data
    df = pd.read_csv(input_file, parse_dates=["date"])
    print(f"Loaded silver data: {len(df)} rows")

    # Create forecast target: meantemp at t+horizon
    df["target"] = df[target].shift(-horizon)
    df = df.dropna(subset=["target"]).reset_index(drop=True)
    print(f"Created target: {target} at t+{horizon} ({len(df)} rows after dropping last {horizon})")

    # Drop metadata columns not relevant for modelling
    metadata_cols = ["batch_id", "ingestion_timestamp", "source_file"]
    df = df.drop(columns=[c for c in metadata_cols if c in df.columns])
    print(f"Dropped metadata columns: {metadata_cols}")

    # One-hot encode season
    df = pd.get_dummies(df, columns=["season"], dtype=int)

    # Select features based on correlation with target
    print("\nFeature selection:")
    feature_cols = select_features(df, "target", corr_threshold)

    # Build final dataset: selected features + target + date (for reference)
    gold_cols = ["date"] + feature_cols + ["target"]
    gold_df = df[gold_cols].copy()

    # Save
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    gold_df.to_csv(output_file, index=False)

    print(f"\nGold dataset: {len(gold_df)} rows, {len(gold_df.columns)} columns -> {output_file}")
    print(f"Features ({len(feature_cols)}): {feature_cols}")
    print(f"Target: next-day {target}")


if __name__ == "__main__":
    main()
