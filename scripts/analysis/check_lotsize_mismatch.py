#!/usr/bin/env python3
"""
Check all houses in nodriver_houses to see if lotsize and lotareavalue are the same.
List any that are not the same.
"""

import json
import os
from pathlib import Path

def check_lotsize_mismatches():
    """Check all JSON files in nodriver_houses directory for lotsize/lotareavalue mismatches."""
    nodriver_houses_dir = Path("data/raw_houses")
    
    if not nodriver_houses_dir.exists():
        print(f"Directory {nodriver_houses_dir} does not exist!")
        return
    
    mismatches = []
    total_files = 0
    files_with_both_fields = 0
    
    # Get all JSON files
    json_files = list(nodriver_houses_dir.glob("*.json"))
    total_files = len(json_files)
    
    print(f"Checking {total_files} files in {nodriver_houses_dir}...")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if basic_info exists and has both fields
            if 'basic_info' in data:
                basic_info = data['basic_info']
                lot_size = basic_info.get('lotSize')
                lot_area_value = basic_info.get('lotAreaValue')
                
                # Check if both fields exist
                if lot_size is not None and lot_area_value is not None:
                    files_with_both_fields += 1
                    
                    # Compare the values (handle both numeric and string comparisons)
                    if lot_size != lot_area_value:
                        mismatches.append({
                            'zpid': basic_info.get('zpid', 'unknown'),
                            'file': json_file.name,
                            'lotSize': lot_size,
                            'lotAreaValue': lot_area_value
                        })
                elif lot_size is not None or lot_area_value is not None:
                    # One exists but not the other
                    mismatches.append({
                        'zpid': basic_info.get('zpid', 'unknown'),
                        'file': json_file.name,
                        'lotSize': lot_size,
                        'lotAreaValue': lot_area_value,
                        'note': 'One field missing'
                    })
        
        except json.JSONDecodeError as e:
            print(f"Error parsing {json_file.name}: {e}")
        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
    
    # Print results
    print(f"\n{'='*80}")
    print(f"Summary:")
    print(f"  Total files checked: {total_files}")
    print(f"  Files with both lotSize and lotAreaValue: {files_with_both_fields}")
    print(f"  Mismatches found: {len(mismatches)}")
    print(f"{'='*80}\n")
    
    if mismatches:
        print("Houses with mismatched lotSize and lotAreaValue:")
        print("-" * 80)
        for mismatch in mismatches:
            print(f"ZPID: {mismatch['zpid']}")
            print(f"  File: {mismatch['file']}")
            print(f"  lotSize: {mismatch['lotSize']}")
            print(f"  lotAreaValue: {mismatch['lotAreaValue']}")
            if 'note' in mismatch:
                print(f"  Note: {mismatch['note']}")
            print()
        
        # Save to file
        output_file = "lotsize_mismatches.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mismatches, f, indent=2)
        print(f"\nFull list saved to {output_file}")
    else:
        print("✓ All houses have matching lotSize and lotAreaValue values!")
    
    return mismatches

if __name__ == "__main__":
    check_lotsize_mismatches()

