"""
Analyze categorical features for one-hot encoding.
Collects all possible values for homeType, zipcode, neighborhood, heating, flooring, and other list features.
"""
import json
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Set


def analyze_categorical_features(houses_dir: str = "data/preprocessed_houses", output_file: str = None):
    """
    Analyze categorical features across all houses.
    
    Args:
        houses_dir: Directory containing house JSON files
        output_file: Optional path to save the full analysis report
    """
    houses_path = Path(houses_dir)
    
    if not houses_path.exists():
        print(f"❌ Directory '{houses_dir}' does not exist!")
        return
    
    json_files = list(houses_path.glob("*.json"))
    
    if not json_files:
        print(f"❌ No JSON files found in '{houses_dir}'!")
        return
    
    print(f"📊 Analyzing {len(json_files)} houses...\n")
    
    # Counters for different feature types
    home_type_counter = Counter()
    zipcode_counter = Counter()
    neighborhood_counter = Counter()
    heating_counter = Counter()
    cooling_counter = Counter()
    flooring_counter = Counter()
    appliances_counter = Counter()
    interior_features_counter = Counter()
    exterior_features_counter = Counter()
    parking_features_counter = Counter()
    security_features_counter = Counter()
    
    # Track null/missing values
    null_counts = defaultdict(int)
    
    # Process all files
    for i, filepath in enumerate(sorted(json_files), 1):
        if i % 500 == 0:
            print(f"Progress: {i}/{len(json_files)} ({i*100//len(json_files)}%)")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            basic_info = data.get("basic_info", {})
            location = data.get("location", {})
            property_details = data.get("property_details", {})
            
            # homeType
            home_type = basic_info.get("homeType")
            if home_type is not None:
                home_type_counter[home_type] += 1
            else:
                null_counts["homeType"] += 1
            
            # zipcode
            zipcode = basic_info.get("zipcode")
            if zipcode is not None:
                zipcode_counter[str(zipcode)] += 1  # Convert to string for consistency
            else:
                null_counts["zipcode"] += 1
            
            # neighborhood
            neighborhood = location.get("neighborhood")
            if neighborhood is not None:
                neighborhood_counter[str(neighborhood)] += 1
            else:
                null_counts["neighborhood"] += 1
            
            # heating (can be string or list)
            heating = property_details.get("heating")
            if heating is not None:
                if isinstance(heating, list):
                    for item in heating:
                        if item is not None:
                            heating_counter[str(item)] += 1
                elif isinstance(heating, str):
                    heating_counter[heating] += 1
            else:
                null_counts["heating"] += 1
            
            # cooling (can be string or list)
            cooling = property_details.get("cooling")
            if cooling is not None:
                if isinstance(cooling, list):
                    for item in cooling:
                        if item is not None:
                            cooling_counter[str(item)] += 1
                elif isinstance(cooling, str):
                    cooling_counter[cooling] += 1
            else:
                null_counts["cooling"] += 1
            
            # flooring (can be string or list)
            flooring = property_details.get("flooring")
            if flooring is not None:
                if isinstance(flooring, list):
                    for item in flooring:
                        if item is not None:
                            flooring_counter[str(item)] += 1
                elif isinstance(flooring, str):
                    flooring_counter[flooring] += 1
            else:
                null_counts["flooring"] += 1
            
            # appliances (list)
            appliances = property_details.get("appliances")
            if appliances is not None:
                if isinstance(appliances, list):
                    for item in appliances:
                        if item is not None:
                            appliances_counter[str(item)] += 1
            else:
                null_counts["appliances"] += 1
            
            # interiorFeatures (list)
            interior_features = property_details.get("interiorFeatures")
            if interior_features is not None:
                if isinstance(interior_features, list):
                    for item in interior_features:
                        if item is not None:
                            interior_features_counter[str(item)] += 1
            else:
                null_counts["interiorFeatures"] += 1
            
            # exteriorFeatures (list)
            exterior_features = property_details.get("exteriorFeatures")
            if exterior_features is not None:
                if isinstance(exterior_features, list):
                    for item in exterior_features:
                        if item is not None:
                            exterior_features_counter[str(item)] += 1
            else:
                null_counts["exteriorFeatures"] += 1
            
            # parkingFeatures (list)
            parking_features = property_details.get("parkingFeatures")
            if parking_features is not None:
                if isinstance(parking_features, list):
                    for item in parking_features:
                        if item is not None:
                            parking_features_counter[str(item)] += 1
            else:
                null_counts["parkingFeatures"] += 1
            
            # securityFeatures (list)
            security_features = property_details.get("securityFeatures")
            if security_features is not None:
                if isinstance(security_features, list):
                    for item in security_features:
                        if item is not None:
                            security_features_counter[str(item)] += 1
            else:
                null_counts["securityFeatures"] += 1
                
        except Exception as e:
            print(f"⚠️  Error processing {filepath.name}: {e}")
    
    # Collect output lines
    output_lines = []
    
    def add_output(text):
        """Add text to both output_lines and print it."""
        output_lines.append(text)
        print(text)
    
    # Print results
    add_output("\n" + "=" * 70)
    add_output("CATEGORICAL FEATURE ANALYSIS")
    add_output("=" * 70)
    
    # homeType
    add_output(f"\n📋 homeType (Total unique values: {len(home_type_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['homeType']}")
    for value, count in home_type_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:20s}: {count:5d} ({percentage:5.2f}%)")
    
    # zipcode
    add_output(f"\n📋 zipcode (Total unique values: {len(zipcode_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['zipcode']}")
    add_output(f"Showing top 20 zipcodes:")
    for value, count in zipcode_counter.most_common(20):
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:10s}: {count:5d} ({percentage:5.2f}%)")
    if len(zipcode_counter) > 20:
        add_output(f"  ... and {len(zipcode_counter) - 20} more zipcodes")
    
    # neighborhood
    add_output(f"\n📋 neighborhood (Total unique values: {len(neighborhood_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['neighborhood']}")
    add_output(f"Showing top 30 neighborhoods:")
    for value, count in neighborhood_counter.most_common(30):
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    if len(neighborhood_counter) > 30:
        add_output(f"  ... and {len(neighborhood_counter) - 30} more neighborhoods")
    
    # heating
    add_output(f"\n📋 heating (Total unique values: {len(heating_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['heating']}")
    for value, count in heating_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    
    # cooling
    add_output(f"\n📋 cooling (Total unique values: {len(cooling_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['cooling']}")
    for value, count in cooling_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    
    # flooring
    add_output(f"\n📋 flooring (Total unique values: {len(flooring_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['flooring']}")
    for value, count in flooring_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    
    # appliances
    add_output(f"\n📋 appliances (Total unique values: {len(appliances_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['appliances']}")
    for value, count in appliances_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    
    # interiorFeatures
    add_output(f"\n📋 interiorFeatures (Total unique values: {len(interior_features_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['interiorFeatures']}")
    for value, count in interior_features_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    
    # exteriorFeatures
    add_output(f"\n📋 exteriorFeatures (Total unique values: {len(exterior_features_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['exteriorFeatures']}")
    for value, count in exterior_features_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    
    # parkingFeatures
    add_output(f"\n📋 parkingFeatures (Total unique values: {len(parking_features_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['parkingFeatures']}")
    for value, count in parking_features_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    
    # securityFeatures
    add_output(f"\n📋 securityFeatures (Total unique values: {len(security_features_counter)})")
    add_output("-" * 70)
    add_output(f"Null/Missing: {null_counts['securityFeatures']}")
    for value, count in security_features_counter.most_common():
        percentage = (count / len(json_files)) * 100
        add_output(f"  {value:40s}: {count:5d} ({percentage:5.2f}%)")
    
    add_output("\n" + "=" * 70)
    add_output("SUMMARY")
    add_output("=" * 70)
    add_output(f"Total houses analyzed: {len(json_files)}")
    add_output(f"\nFeature counts:")
    add_output(f"  homeType:           {len(home_type_counter):4d} unique values")
    add_output(f"  zipcode:             {len(zipcode_counter):4d} unique values")
    add_output(f"  neighborhood:       {len(neighborhood_counter):4d} unique values")
    add_output(f"  heating:             {len(heating_counter):4d} unique values")
    add_output(f"  cooling:             {len(cooling_counter):4d} unique values")
    add_output(f"  flooring:           {len(flooring_counter):4d} unique values")
    add_output(f"  appliances:         {len(appliances_counter):4d} unique values")
    add_output(f"  interiorFeatures:   {len(interior_features_counter):4d} unique values")
    add_output(f"  exteriorFeatures:   {len(exterior_features_counter):4d} unique values")
    add_output(f"  parkingFeatures:    {len(parking_features_counter):4d} unique values")
    add_output(f"  securityFeatures:   {len(security_features_counter):4d} unique values")
    add_output("=" * 70)
    
    # Save to file if requested
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n💾 Full analysis saved to: {output_file}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze categorical features for one-hot encoding")
    parser.add_argument(
        "--dir",
        type=str,
        default="data/preprocessed_houses",
        help="Directory containing house JSON files (default: data/preprocessed_houses)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file to save the full analysis report (default: None)"
    )
    
    args = parser.parse_args()
    analyze_categorical_features(args.dir, args.output)

