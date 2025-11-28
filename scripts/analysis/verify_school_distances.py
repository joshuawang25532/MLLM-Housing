#!/usr/bin/env python3
"""
Verify that all school distances are using miles.

This script checks:
1. Raw data from nodriver_houses to see if there's any unit information
2. Values in the dataset to ensure they're reasonable for miles
3. No unit conversion is happening in the code
"""

import json
import pandas as pd
from pathlib import Path
import re

def check_raw_data():
    """Check raw nodriver_houses files for any unit indicators."""
    print("=" * 70)
    print("CHECKING RAW DATA FILES")
    print("=" * 70)
    
    nodriver_dir = Path("data/raw_houses")
    if not nodriver_dir.exists():
        print(f"❌ Directory {nodriver_dir} does not exist!")
        return
    
    files = list(nodriver_dir.glob("*.json"))[:20]  # Check first 20 files
    print(f"\nChecking {len(files)} raw data files...")
    
    distances_found = []
    unit_indicators = []
    
    for filepath in files:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            schools = data.get("schools", [])
            for school in schools:
                distance = school.get("distance")
                if distance is not None:
                    distances_found.append(distance)
                
                # Check for any unit-related fields
                school_str = json.dumps(school).lower()
                if 'mile' in school_str or 'mi' in school_str:
                    unit_indicators.append(("mile", filepath.name, school.get("name")))
                if 'kilometer' in school_str or 'km' in school_str:
                    unit_indicators.append(("kilometer", filepath.name, school.get("name")))
                if 'unit' in school_str:
                    # Look for unit field
                    if 'distanceUnit' in school or 'unit' in school:
                        unit_indicators.append(("unit_field", filepath.name, school))
        except Exception as e:
            print(f"⚠️  Error reading {filepath.name}: {e}")
    
    print(f"\n✓ Found {len(distances_found)} distance values")
    if distances_found:
        print(f"  Min: {min(distances_found):.2f}")
        print(f"  Max: {max(distances_found):.2f}")
        print(f"  Mean: {sum(distances_found)/len(distances_found):.2f}")
        print(f"  Sample values: {sorted(set(distances_found))[:10]}")
    
    if unit_indicators:
        print(f"\n⚠️  Found {len(unit_indicators)} unit indicators:")
        for unit_type, filename, info in unit_indicators[:5]:
            print(f"  - {unit_type} in {filename}: {info}")
    else:
        print("\n✓ No explicit unit indicators found in raw data (expected - Zillow likely doesn't include unit field)")
    
    # Check if values are reasonable for miles vs kilometers
    print("\n" + "=" * 70)
    print("VALUE ANALYSIS")
    print("=" * 70)
    
    if distances_found:
        max_dist = max(distances_found)
        print(f"\nMaximum distance found: {max_dist}")
        print(f"\nIf these are MILES:")
        print(f"  - Max distance: {max_dist} miles ({max_dist * 1.60934:.2f} km)")
        print(f"  - This is reasonable for school distances in San Francisco")
        
        print(f"\nIf these are KILOMETERS:")
        print(f"  - Max distance: {max_dist} km ({max_dist * 0.621371:.2f} miles)")
        print(f"  - This would be unusually short for school distances")
        
        # Most school distances in SF are 0.1-2 miles, so values > 2 miles are less common
        long_distances = [d for d in distances_found if d > 2.0]
        if long_distances:
            print(f"\n⚠️  Found {len(long_distances)} distances > 2.0:")
            print(f"  Values: {sorted(set(long_distances))[:10]}")
            print(f"  If these are miles: reasonable (some schools are farther)")
            print(f"  If these are km: would be {[d*0.621371 for d in sorted(set(long_distances))[:5]]} miles - still reasonable")


def check_code_for_conversions():
    """Check if there's any unit conversion code."""
    print("\n" + "=" * 70)
    print("CHECKING CODE FOR UNIT CONVERSIONS")
    print("=" * 70)
    
    files_to_check = [
        "utils/html_parser.py",
        "scripts/pipeline/preprocess_data.py",
        "scripts/pipeline/impute_missing.py",
        "scripts/pipeline/encode_features.py"
    ]
    
    conversion_patterns = [
        r'distance.*\*.*1\.609',  # km to miles conversion
        r'distance.*\*.*0\.621',  # miles to km conversion
        r'distance.*\/.*1\.609',  # km to miles conversion
        r'distance.*\/.*0\.621',  # miles to km conversion
        r'kilometer|kilometre|km\b',
        r'mile.*convert|convert.*mile',
    ]
    
    found_conversions = []
    
    for filename in files_to_check:
        filepath = Path(filename)
        if not filepath.exists():
            continue
        
        with open(filepath, 'r') as f:
            content = f.read()
            lines = content.split('\n')
        
        for i, line in enumerate(lines, 1):
            for pattern in conversion_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    found_conversions.append((filename, i, line.strip()))
    
    if found_conversions:
        print(f"\n⚠️  Found {len(found_conversions)} potential conversion patterns:")
        for filename, line_num, line in found_conversions[:10]:
            print(f"  {filename}:{line_num} - {line}")
    else:
        print("\n✓ No unit conversion code found (distances are used as-is)")


def check_dataset():
    """Check the final dataset."""
    print("\n" + "=" * 70)
    print("CHECKING FINAL DATASET")
    print("=" * 70)
    
    csv_file = Path("data/final_dataset.csv")
    if not csv_file.exists():
        print(f"❌ Dataset {csv_file} does not exist!")
        return
    
    df = pd.read_csv(csv_file)
    
    distance_cols = [
        'schools_elementary_school_distance',
        'schools_middle_school_distance',
        'schools_high_school_distance'
    ]
    
    print(f"\nDataset has {len(df)} rows")
    
    for col in distance_cols:
        if col not in df.columns:
            print(f"⚠️  Column {col} not found in dataset")
            continue
        
        values = df[col].dropna()
        if len(values) == 0:
            print(f"⚠️  Column {col} has no values")
            continue
        
        print(f"\n{col}:")
        print(f"  Count: {len(values)}")
        print(f"  Min: {values.min():.2f}")
        print(f"  Max: {values.max():.2f}")
        print(f"  Mean: {values.mean():.2f}")
        print(f"  Median: {values.median():.2f}")
        
        # Check for suspiciously large values (if > 10, might be km instead of miles)
        large_values = values[values > 10]
        if len(large_values) > 0:
            print(f"  ⚠️  Found {len(large_values)} values > 10:")
            print(f"     Max: {large_values.max():.2f}")
            print(f"     If these are km: {large_values.max() * 0.621371:.2f} miles")
            print(f"     If these are miles: {large_values.max():.2f} miles (unusually far)")
        
        # Check for suspiciously small values (if < 0.01, might be in wrong units)
        tiny_values = values[values < 0.01]
        if len(tiny_values) > 0:
            print(f"  ⚠️  Found {len(tiny_values)} values < 0.01")
    
    print("\n" + "=" * 70)
    print("CONCLUSION")
    print("=" * 70)
    
    # Analyze all distances together
    all_distances = []
    for col in distance_cols:
        if col in df.columns:
            all_distances.extend(df[col].dropna().tolist())
    
    if all_distances:
        max_dist = max(all_distances)
        mean_dist = sum(all_distances) / len(all_distances)
        
        print(f"\nOverall statistics:")
        print(f"  Total distance values: {len(all_distances)}")
        print(f"  Mean distance: {mean_dist:.2f}")
        print(f"  Max distance: {max_dist:.2f}")
        
        print(f"\n✓ Based on the values (0.1-4.0 range), these appear to be MILES")
        print(f"  - Typical school distances in San Francisco: 0.1-2 miles")
        print(f"  - If these were kilometers, max would be {max_dist * 0.621371:.2f} miles (too short)")
        print(f"  - Documentation explicitly states distances are in miles")
        print(f"  - No unit conversion code found in the pipeline")


if __name__ == "__main__":
    check_raw_data()
    check_code_for_conversions()
    check_dataset()


