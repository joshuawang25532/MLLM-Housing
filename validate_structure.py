"""
Structure validation script for preprocessed houses.
Ensures all files have the exact same structure.
"""
import json
from pathlib import Path
from typing import Dict, Set, List, Tuple
from collections import defaultdict


def get_structure(data: dict, path: str = "") -> Set[str]:
    """
    Recursively extract all keys from a nested dictionary structure.
    Returns a set of paths like "basic_info.zpid", "location.latitude", etc.
    """
    structure = set()
    
    for key, value in data.items():
        current_path = f"{path}.{key}" if path else key
        
        if isinstance(value, dict):
            # Recursively get structure from nested dict
            structure.add(current_path)  # Add the dict itself
            structure.update(get_structure(value, current_path))
        elif isinstance(value, list):
            structure.add(current_path)  # Add the list itself
            # Check first element if list is not empty
            if len(value) > 0 and isinstance(value[0], dict):
                structure.add(f"{current_path}[]")  # Mark as list of dicts
                structure.update(get_structure(value[0], f"{current_path}[]"))
        else:
            structure.add(current_path)
    
    return structure


def validate_structure(houses_dir: str = "houses_preprocessed") -> Tuple[Dict[str, Set[str]], List[str]]:
    """
    Validate that all house files have the exact same structure.
    
    Returns:
        (structure_map, inconsistent_files)
        - structure_map: dict mapping filename to its structure (set of paths)
        - inconsistent_files: list of filenames that don't match the reference structure
    """
    houses_path = Path(houses_dir)
    
    if not houses_path.exists():
        print(f"❌ Directory '{houses_dir}' does not exist!")
        return {}, []
    
    json_files = list(houses_path.glob("*.json"))
    
    if not json_files:
        print(f"❌ No JSON files found in '{houses_dir}'!")
        return {}, []
    
    print(f"📋 Analyzing structure of {len(json_files)} files...")
    
    structure_map = {}
    reference_structure = None
    reference_file = None
    
    # First pass: collect all structures
    for filepath in sorted(json_files):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            structure = get_structure(data)
            structure_map[filepath.name] = structure
            
            # Use first file as reference
            if reference_structure is None:
                reference_structure = structure
                reference_file = filepath.name
                print(f"✅ Using '{reference_file}' as reference structure ({len(reference_structure)} keys)")
        
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing {filepath.name}: {e}")
            structure_map[filepath.name] = set()
        except Exception as e:
            print(f"❌ Error reading {filepath.name}: {e}")
            structure_map[filepath.name] = set()
    
    # Second pass: find inconsistencies
    inconsistent_files = []
    
    for filename, structure in structure_map.items():
        if structure != reference_structure:
            inconsistent_files.append(filename)
    
    return structure_map, inconsistent_files


def print_structure_report(structure_map: Dict[str, Set[str]], inconsistent_files: List[str], reference_file: str):
    """Print a detailed report of structure validation."""
    if not structure_map:
        return
    
    reference_structure = structure_map[reference_file]
    
    print("\n" + "=" * 70)
    print("STRUCTURE VALIDATION REPORT")
    print("=" * 70)
    
    print(f"\n📊 Total files analyzed: {len(structure_map)}")
    print(f"✅ Files with consistent structure: {len(structure_map) - len(inconsistent_files)}")
    print(f"❌ Files with inconsistent structure: {len(inconsistent_files)}")
    
    if inconsistent_files:
        print(f"\n❌ INCONSISTENT FILES ({len(inconsistent_files)}):")
        print("-" * 70)
        
        for filename in sorted(inconsistent_files):
            file_structure = structure_map[filename]
            
            # Find differences
            missing_keys = reference_structure - file_structure
            extra_keys = file_structure - reference_structure
            
            print(f"\n  {filename}:")
            if missing_keys:
                print(f"    Missing keys ({len(missing_keys)}):")
                for key in sorted(missing_keys)[:10]:  # Show first 10
                    print(f"      - {key}")
                if len(missing_keys) > 10:
                    print(f"      ... and {len(missing_keys) - 10} more")
            
            if extra_keys:
                print(f"    Extra keys ({len(extra_keys)}):")
                for key in sorted(extra_keys)[:10]:  # Show first 10
                    print(f"      + {key}")
                if len(extra_keys) > 10:
                    print(f"      ... and {len(extra_keys) - 10} more")
    else:
        print("\n✅ All files have consistent structure!")
    
    # Print reference structure
    print(f"\n📋 Reference structure (from {reference_file}):")
    print("-" * 70)
    print(f"Total keys: {len(reference_structure)}")
    print("\nTop-level keys:")
    top_level = sorted([k for k in reference_structure if '.' not in k])
    for key in top_level:
        print(f"  • {key}")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate structure consistency of preprocessed houses")
    parser.add_argument(
        "--dir",
        type=str,
        default="houses_preprocessed",
        help="Directory containing house JSON files (default: houses_preprocessed)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output JSON file to save inconsistent files list"
    )
    
    args = parser.parse_args()
    
    structure_map, inconsistent_files = validate_structure(args.dir)
    
    if structure_map:
        reference_file = min(structure_map.keys())  # Get first file alphabetically
        print_structure_report(structure_map, inconsistent_files, reference_file)
        
        # Save inconsistent files list if requested
        if args.output:
            output_data = {
                "total_files": len(structure_map),
                "consistent_files": len(structure_map) - len(inconsistent_files),
                "inconsistent_files_count": len(inconsistent_files),
                "inconsistent_files": sorted(inconsistent_files),
                "reference_file": reference_file,
                "reference_structure": sorted(list(structure_map[reference_file]))
            }
            
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2)
            
            print(f"\n💾 Inconsistent files list saved to: {args.output}")
    
    # Exit with error code if inconsistencies found
    if inconsistent_files:
        exit(1)
    else:
        exit(0)


if __name__ == "__main__":
    main()

