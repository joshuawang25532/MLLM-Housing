#!/usr/bin/env python3
"""
Analyze the lotsize mismatches to categorize them.
"""

import json

def analyze_mismatches():
    """Analyze the types of mismatches."""
    with open('lotsize_mismatches.json', 'r', encoding='utf-8') as f:
        mismatches = json.load(f)
    
    print(f"Total mismatches: {len(mismatches)}\n")
    
    # Categorize mismatches
    small_diff = []  # Difference < 1
    medium_diff = []  # Difference 1-100
    large_diff = []  # Difference > 100
    unit_conversion = []  # lotAreaValue < 10 (likely acres)
    missing_one = []  # One field is None
    
    for m in mismatches:
        if 'note' in m and 'missing' in m['note'].lower():
            missing_one.append(m)
            continue
        
        lot_size = m['lotSize']
        lot_area_value = m['lotAreaValue']
        
        if lot_size is None or lot_area_value is None:
            missing_one.append(m)
            continue
        
        diff = abs(lot_size - lot_area_value)
        
        if lot_area_value < 10 and lot_size > 100:
            # Likely unit conversion issue (sqft vs acres)
            unit_conversion.append(m)
        elif diff < 1:
            small_diff.append(m)
        elif diff < 100:
            medium_diff.append(m)
        else:
            large_diff.append(m)
    
    print("=" * 80)
    print("MISMATCH CATEGORIES:")
    print("=" * 80)
    print(f"\n1. Unit Conversion Issues (lotAreaValue < 10, lotSize > 100): {len(unit_conversion)}")
    print(f"   Example: lotSize={unit_conversion[0]['lotSize'] if unit_conversion else 'N/A'}, "
          f"lotAreaValue={unit_conversion[0]['lotAreaValue'] if unit_conversion else 'N/A'}")
    
    print(f"\n2. Small Differences (< 1): {len(small_diff)}")
    if small_diff:
        example = small_diff[0]
        print(f"   Example: lotSize={example['lotSize']}, lotAreaValue={example['lotAreaValue']}, "
              f"diff={abs(example['lotSize'] - example['lotAreaValue']):.4f}")
    
    print(f"\n3. Medium Differences (1-100): {len(medium_diff)}")
    if medium_diff:
        example = medium_diff[0]
        print(f"   Example: lotSize={example['lotSize']}, lotAreaValue={example['lotAreaValue']}, "
              f"diff={abs(example['lotSize'] - example['lotAreaValue']):.2f}")
    
    print(f"\n4. Large Differences (> 100): {len(large_diff)}")
    if large_diff:
        example = large_diff[0]
        print(f"   Example: lotSize={example['lotSize']}, lotAreaValue={example['lotAreaValue']}, "
              f"diff={abs(example['lotSize'] - example['lotAreaValue']):.2f}")
    
    print(f"\n5. Missing One Field: {len(missing_one)}")
    
    print("\n" + "=" * 80)
    print("SUMMARY:")
    print("=" * 80)
    print(f"Total mismatches: {len(mismatches)}")
    print(f"  - Unit conversion issues: {len(unit_conversion)} ({len(unit_conversion)/len(mismatches)*100:.1f}%)")
    print(f"  - Small differences: {len(small_diff)} ({len(small_diff)/len(mismatches)*100:.1f}%)")
    print(f"  - Medium differences: {len(medium_diff)} ({len(medium_diff)/len(mismatches)*100:.1f}%)")
    print(f"  - Large differences: {len(large_diff)} ({len(large_diff)/len(mismatches)*100:.1f}%)")
    print(f"  - Missing one field: {len(missing_one)} ({len(missing_one)/len(mismatches)*100:.1f}%)")

if __name__ == "__main__":
    analyze_mismatches()


