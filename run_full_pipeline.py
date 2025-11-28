#!/usr/bin/env python3
"""
Complete ML Pipeline Runner

Runs the entire preprocessing pipeline from raw scraped data to model-ready dataset:
1. Validation (optional)
2. Preprocessing
3. One-hot encoding
4. KNN imputation
5. Single file conversion

Usage:
    python3 run_full_pipeline.py [--skip-validation] [--skip-imputation]
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle errors."""
    print("\n" + "=" * 70)
    print(f"RUNNING: {description}")
    print("=" * 70)
    print(f"Command: {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode != 0:
        print(f"\n❌ Error running: {description}")
        print(f"Exit code: {result.returncode}")
        sys.exit(1)
    
    print(f"\n✅ Completed: {description}")
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Run the complete ML preprocessing pipeline"
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip the validation step (faster, but less safe)"
    )
    parser.add_argument(
        "--skip-imputation",
        action="store_true",
        help="Skip KNN imputation step (for testing)"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="nodriver_houses",
        help="Source directory with raw scraped houses (default: nodriver_houses)"
    )
    parser.add_argument(
        "--k",
        type=int,
        default=5,
        help="K value for KNN imputation (default: 5)"
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("ML PREPROCESSING PIPELINE")
    print("=" * 70)
    print(f"Source directory: {args.source}")
    print(f"Skip validation: {args.skip_validation}")
    print(f"Skip imputation: {args.skip_imputation}")
    print(f"KNN K value: {args.k}")
    print("=" * 70)
    
    # Check source directory exists
    source_path = Path(args.source)
    if not source_path.exists():
        print(f"\n❌ Error: Source directory '{args.source}' does not exist!")
        sys.exit(1)
    
    # Stage 1: Validation (optional)
    if not args.skip_validation:
        print("\n📋 Stage 1: Validating houses...")
        run_command(
            ["python3", "nodriver_validator.py", "--output-issues", "issues.json"],
            "House Validation"
        )
    else:
        print("\n⏭️  Skipping validation step...")
    
    # Stage 2: Preprocessing
    print("\n🔧 Stage 2: Preprocessing houses...")
    run_command(
        ["python3", "house_preprocessing.py"],
        "House Preprocessing (filtering, cleaning, transforming)"
    )
    
    # Check filtered_houses exists
    filtered_path = Path("filtered_houses")
    if not filtered_path.exists() or len(list(filtered_path.glob("*.json"))) == 0:
        print("\n❌ Error: No houses in filtered_houses/ directory!")
        print("   Preprocessing may have failed or filtered out all houses.")
        sys.exit(1)
    
    # Stage 3: One-hot encoding
    print("\n🔢 Stage 3: One-hot encoding houses...")
    run_command(
        ["python3", "one_hot_encode_houses.py", "--source", "filtered_houses", "--dest", "houses_onehot_encoded"],
        "One-Hot Encoding (categorical features, flattening)"
    )
    
    # Check houses_onehot_encoded exists
    encoded_path = Path("houses_onehot_encoded")
    if not encoded_path.exists() or len(list(encoded_path.glob("*.json"))) == 0:
        print("\n❌ Error: No houses in houses_onehot_encoded/ directory!")
        print("   One-hot encoding may have failed.")
        sys.exit(1)
    
    # Stage 4: KNN Imputation
    if not args.skip_imputation:
        print("\n🤖 Stage 4: KNN imputation...")
        run_command(
            ["python3", "house_imputation.py", "--source", "houses_onehot_encoded", "--dest", "houses_imputed", "--k", str(args.k)],
            "KNN Imputation (Phase 4 features)"
        )
        
        # Check houses_imputed exists
        imputed_path = Path("houses_imputed")
        if not imputed_path.exists() or len(list(imputed_path.glob("*.json"))) == 0:
            print("\n❌ Error: No houses in houses_imputed/ directory!")
            print("   Imputation may have failed.")
            sys.exit(1)
        
        # Stage 5: Convert to single file
        print("\n📄 Stage 5: Converting to single file...")
        run_command(
            ["python3", "convert_to_single_file.py", "--source", "houses_imputed", "--output", "houses_dataset.csv"],
            "Single File Conversion"
        )
    else:
        print("\n⏭️  Skipping imputation step...")
        print("   Converting one-hot encoded houses to single file instead...")
        run_command(
            ["python3", "convert_to_single_file.py", "--source", "houses_onehot_encoded", "--output", "houses_dataset.csv"],
            "Single File Conversion"
        )
    
    # Final summary
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE!")
    print("=" * 70)
    
    if not args.skip_imputation:
        dataset_file = Path("houses_dataset.csv")
        if dataset_file.exists():
            import pandas as pd
            df = pd.read_csv(dataset_file)
            print(f"\n✅ Final dataset: {dataset_file}")
            print(f"   Rows: {len(df):,}")
            print(f"   Columns: {len(df.columns)}")
            print(f"   Size: {dataset_file.stat().st_size / 1024 / 1024:.2f} MB")
    
    print("\n📁 Output directories:")
    print(f"   - filtered_houses/        (preprocessed)")
    print(f"   - houses_onehot_encoded/  (one-hot encoded)")
    if not args.skip_imputation:
        print(f"   - houses_imputed/         (imputed)")
        print(f"   - houses_dataset.csv      (single file)")
    print("=" * 70)


if __name__ == "__main__":
    main()

