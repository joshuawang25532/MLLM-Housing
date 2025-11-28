"""
Convert all house JSON files to a single CSV file.

Takes all individual JSON files from houses_imputed/ (or houses_onehot_encoded/)
and converts them into a single CSV file for easy loading into pandas/sklearn.

Usage:
    python3 convert_to_single_file.py [--source houses_imputed] [--output houses_dataset.csv]
"""

import json
import pandas as pd
from pathlib import Path
import argparse


def load_houses_to_dataframe(houses_dir: str) -> pd.DataFrame:
    """
    Load all house JSON files into a pandas DataFrame.
    
    Args:
        houses_dir: Directory containing house JSON files
        
    Returns:
        DataFrame with all houses
    """
    houses_path = Path(houses_dir)
    json_files = list(houses_path.glob("*.json"))
    
    # Exclude metadata files (not actual house data)
    json_files = [f for f in json_files if f.name not in ["knn_metadata.json", "issues.json", "visited_houses.json"]]
    
    if not json_files:
        raise ValueError(f"No JSON files found in {houses_dir}")
    
    houses_data = []
    
    print(f"📂 Loading houses from {houses_dir}...")
    print(f"   Found {len(json_files)} house files (excluding metadata)")
    
    for i, filepath in enumerate(sorted(json_files), 1):
        if i % 500 == 0:
            print(f"   Progress: {i}/{len(json_files)} ({i*100//len(json_files)}%)")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            houses_data.append(data)
        except Exception as e:
            print(f"⚠️  Error loading {filepath.name}: {e}")
            continue
    
    df = pd.DataFrame(houses_data)
    
    print(f"✅ Loaded {len(df)} houses")
    print(f"   Total features: {len(df.columns)}")
    
    return df


def convert_to_csv(source_dir: str, output_file: str):
    """
    Convert all house JSON files to a single CSV file.
    
    Args:
        source_dir: Directory containing house JSON files
        output_file: Output CSV file path
    """
    print("=" * 70)
    print("CONVERTING HOUSES TO SINGLE CSV FILE")
    print("=" * 70)
    
    # Load all houses
    df = load_houses_to_dataframe(source_dir)
    
    # Remove rows with missing critical values (shouldn't exist, but safety check)
    print("\n🔍 Cleaning data...")
    initial_rows = len(df)
    
    # Remove rows with missing lastSoldPrice (target variable)
    target_col = 'financial_lastSoldPrice'
    if target_col in df.columns:
        rows_before = len(df)
        df = df[df[target_col].notna()]
        removed_price = rows_before - len(df)
        if removed_price > 0:
            print(f"   Removed {removed_price} rows with missing {target_col}")
    
    # Remove rows with missing latitude or longitude
    lat_col = 'location_latitude'
    long_col = 'location_longitude'
    if lat_col in df.columns and long_col in df.columns:
        rows_before = len(df)
        df = df[df[lat_col].notna() & df[long_col].notna()]
        removed_location = rows_before - len(df)
        if removed_location > 0:
            print(f"   Removed {removed_location} rows with missing location data")
    
    # Remove NaN columns (features that don't exist in any house)
    initial_cols = len(df.columns)
    df = df.dropna(axis=1, how='all')  # Remove columns that are all NaN
    removed_cols = initial_cols - len(df.columns)
    
    if removed_cols > 0:
        print(f"   Removed {removed_cols} columns with all NaN values")
    
    if initial_rows != len(df):
        print(f"   Total rows removed: {initial_rows - len(df)}")
    
    # Sort columns for consistency
    df = df.reindex(sorted(df.columns), axis=1)
    
    # Save to CSV
    print(f"\n💾 Saving to {output_file}...")
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    
    # Print summary
    file_size_mb = output_path.stat().st_size / 1024 / 1024
    
    print("\n" + "=" * 70)
    print("CONVERSION COMPLETE")
    print("=" * 70)
    print(f"Output file: {output_file}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"File size: {file_size_mb:.2f} MB")
    print("\nColumn names (first 20):")
    for i, col in enumerate(df.columns[:20], 1):
        print(f"  {i:2d}. {col}")
    if len(df.columns) > 20:
        print(f"  ... and {len(df.columns) - 20} more columns")
    print("=" * 70)
    
    # Print data types summary
    print("\n📊 Data Types Summary:")
    print("-" * 70)
    dtype_counts = df.dtypes.value_counts()
    for dtype, count in dtype_counts.items():
        print(f"  {dtype}: {count} columns")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert all house JSON files to a single CSV file"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="houses_imputed",
        help="Source directory containing house JSON files (default: houses_imputed)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="houses_dataset.csv",
        help="Output CSV file path (default: houses_dataset.csv)"
    )
    
    args = parser.parse_args()
    
    convert_to_csv(args.source, args.output)

