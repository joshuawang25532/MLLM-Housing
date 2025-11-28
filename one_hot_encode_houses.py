"""
One-hot encode categorical features in house data.
Applies encoding rules:
- homeType: All values (keep as-is)
- zipcode: All values (keep as-is)
- neighborhood: Top 20 + Other
- heating: Top 5 + Other
- flooring: Top 4 + Other
- parkingFeatures: Top 5 + Other
- exteriorFeatures: Top 3 + Other
- Remove entirely: interiorFeatures, securityFeatures, cooling, appliances
- Normalizes boolean fields (hasView, hasSpa, etc.): True → 1, False/None → 0
- Handles taxAssessedValue: adds taxAssessedValue_is_missing flag, fills nulls with median
- Flattens entire structure: converts nested dictionaries to flat key-value pairs
"""
import json
from pathlib import Path
from collections import Counter
from typing import Dict, List, Set, Any
import statistics


def get_top_values(houses_dir: str, feature_path: str, top_n: int) -> List[str]:
    """
    Get top N values for a feature by counting occurrences.
    
    Args:
        houses_dir: Directory containing house JSON files
        feature_path: Path to feature like "location.neighborhood" or "property_details.heating"
        top_n: Number of top values to return (if top_n is very large, returns all unique values)
        
    Returns:
        List of top N values (strings), sorted by frequency
    """
    houses_path = Path(houses_dir)
    json_files = list(houses_path.glob("*.json"))
    
    counter = Counter()
    
    for filepath in json_files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Navigate to the feature
            parts = feature_path.split('.')
            value = data
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
                    break
            
            # Handle different value types
            if value is None:
                continue
            elif isinstance(value, list):
                # For list features, count each item
                for item in value:
                    if item is not None:
                        counter[str(item)] += 1
            elif isinstance(value, str):
                # For string features
                counter[value] += 1
            else:
                # For other types (int, etc.), convert to string
                counter[str(value)] += 1
        except Exception:
            continue
    
    # Return top N values (sorted by frequency)
    return [val for val, _ in counter.most_common(top_n)]


def normalize_value(value: str) -> str:
    """
    Normalize a value for use as a column name.
    Replaces spaces and special characters with underscores.
    
    Args:
        value: Original value string
        
    Returns:
        Normalized string suitable for column name
    """
    # Replace spaces and special chars with underscores
    normalized = value.replace(' ', '_').replace('/', '_').replace('-', '_')
    normalized = normalized.replace('(', '').replace(')', '').replace("'", '')
    normalized = normalized.replace(',', '').replace('.', '').replace(':', '')
    # Remove multiple underscores
    while '__' in normalized:
        normalized = normalized.replace('__', '_')
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    return normalized


def one_hot_encode_value(value: Any, top_values: List[str], feature_name: str, include_other: bool = True) -> Dict[str, int]:
    """
    One-hot encode a single value.
    
    Args:
        value: The value to encode (can be string, list, or None)
        top_values: List of values to create columns for
        feature_name: Name of the feature (for column naming)
        include_other: Whether to include an "Other" column for values not in top_values
        
    Returns:
        Dictionary of one-hot encoded columns {column_name: 0 or 1}
    """
    encoded = {}
    
    # Normalize top values for column names
    normalized_top_values = {val: normalize_value(val) for val in top_values}
    
    # Create columns for all values (all start at 0)
    for val in top_values:
        col_name = f"{feature_name}_{normalized_top_values[val]}"
        encoded[col_name] = 0
    
    # Create "Other" column if needed
    if include_other:
        encoded[f"{feature_name}_Other"] = 0
    
    # Encode the actual value(s)
    if value is None:
        # If null and include_other, set Other to 1
        if include_other:
            encoded[f"{feature_name}_Other"] = 1
    elif isinstance(value, list):
        # For list features, set 1 for each value that appears
        found_any_not_in_top = False
        
        for item in value:
            if item is not None:
                item_str = str(item)
                if item_str in top_values:
                    col_name = f"{feature_name}_{normalized_top_values[item_str]}"
                    encoded[col_name] = 1
                else:
                    found_any_not_in_top = True
        
        # If any value not in top and include_other, set Other to 1
        if include_other and found_any_not_in_top:
            encoded[f"{feature_name}_Other"] = 1
    elif isinstance(value, str):
        # For string features
        value_str = str(value)
        if value_str in top_values:
            col_name = f"{feature_name}_{normalized_top_values[value_str]}"
            encoded[col_name] = 1
        elif include_other:
            encoded[f"{feature_name}_Other"] = 1
    
    return encoded


def one_hot_encode_house(house_data: dict, encoding_config: dict, median_tax_assessed_value: float = None) -> dict:
    """
    Apply one-hot encoding to a house data dictionary.
    Also handles null imputation and flattens the structure.
    
    Null Handling Strategy:
    - Phase 2 (Simple Imputation): monthlyHoaFee, hasSpa, hasView, hasFireplace, fireplaces, garageParkingCapacity → 0
    - Phase 3 (Impute with Flagging): taxAssessedValue → median + missing flag
    - Phase 5 (Features to Ignore): hasAssociation → 0, numberOfUnitsInCommunity → 0
    
    Note: Phase 1 (row removal) is handled in process_houses() before calling this function.
    
    Args:
        house_data: Original house data dictionary
        encoding_config: Configuration dict with top values for each feature
        median_tax_assessed_value: Median value to use for filling null taxAssessedValue
        
    Returns:
        Flattened dictionary with one-hot encoded features (all keys at top level)
    """
    # Create a deep copy
    encoded_data = json.loads(json.dumps(house_data))
    
    # Remove features that should be deleted entirely
    features_to_remove = [
        ("property_details", "interiorFeatures"),
        ("property_details", "securityFeatures"),
        ("property_details", "cooling"),
        ("property_details", "appliances"),
    ]
    
    for section, feature in features_to_remove:
        if section in encoded_data and isinstance(encoded_data[section], dict):
            encoded_data[section].pop(feature, None)
    
    # One-hot encode neighborhood (Top 20 + Other)
    if "location" in encoded_data:
        neighborhood = encoded_data["location"].get("neighborhood")
        top_neighborhoods = encoding_config.get("neighborhood", [])
        encoded_neighborhood = one_hot_encode_value(neighborhood, top_neighborhoods, "neighborhood")
        # Remove original neighborhood field
        encoded_data["location"].pop("neighborhood", None)
        # Add one-hot encoded columns
        encoded_data["location"].update(encoded_neighborhood)
    
    # One-hot encode heating (Top 5 + Other)
    if "property_details" in encoded_data:
        heating = encoded_data["property_details"].get("heating")
        top_heating = encoding_config.get("heating", [])
        encoded_heating = one_hot_encode_value(heating, top_heating, "heating")
        # Remove original heating field
        encoded_data["property_details"].pop("heating", None)
        # Add one-hot encoded columns
        encoded_data["property_details"].update(encoded_heating)
    
    # One-hot encode flooring (Top 4 + Other)
    if "property_details" in encoded_data:
        flooring = encoded_data["property_details"].get("flooring")
        top_flooring = encoding_config.get("flooring", [])
        encoded_flooring = one_hot_encode_value(flooring, top_flooring, "flooring")
        # Remove original flooring field
        encoded_data["property_details"].pop("flooring", None)
        # Add one-hot encoded columns
        encoded_data["property_details"].update(encoded_flooring)
    
    # One-hot encode parkingFeatures (Top 5 + Other)
    if "property_details" in encoded_data:
        parking_features = encoded_data["property_details"].get("parkingFeatures")
        top_parking = encoding_config.get("parkingFeatures", [])
        encoded_parking = one_hot_encode_value(parking_features, top_parking, "parkingFeatures")
        # Remove original parkingFeatures field
        encoded_data["property_details"].pop("parkingFeatures", None)
        # Add one-hot encoded columns
        encoded_data["property_details"].update(encoded_parking)
    
    # One-hot encode exteriorFeatures (Top 3 + Other)
    if "property_details" in encoded_data:
        exterior_features = encoded_data["property_details"].get("exteriorFeatures")
        top_exterior = encoding_config.get("exteriorFeatures", [])
        encoded_exterior = one_hot_encode_value(exterior_features, top_exterior, "exteriorFeatures")
        # Remove original exteriorFeatures field
        encoded_data["property_details"].pop("exteriorFeatures", None)
        # Add one-hot encoded columns
        encoded_data["property_details"].update(encoded_exterior)
    
    # One-hot encode homeType (all values, no Other column)
    if "basic_info" in encoded_data:
        home_type = encoded_data["basic_info"].get("homeType")
        all_home_types = encoding_config.get("homeType", [])
        encoded_home_type = one_hot_encode_value(home_type, all_home_types, "homeType", include_other=False)
        # Remove original homeType field
        encoded_data["basic_info"].pop("homeType", None)
        # Add one-hot encoded columns
        encoded_data["basic_info"].update(encoded_home_type)
    
    # One-hot encode zipcode (all values, no Other column)
    if "basic_info" in encoded_data:
        zipcode = encoded_data["basic_info"].get("zipcode")
        all_zipcodes = encoding_config.get("zipcode", [])
        encoded_zipcode = one_hot_encode_value(str(zipcode) if zipcode else None, all_zipcodes, "zipcode", include_other=False)
        # Remove original zipcode field
        encoded_data["basic_info"].pop("zipcode", None)
        # Add one-hot encoded columns
        encoded_data["basic_info"].update(encoded_zipcode)
    
    # Normalize boolean fields: True → 1, False/None → 0
    boolean_fields = [
        ("property_details", "hasView"),
        ("property_details", "hasSpa"),
        ("property_details", "hasFireplace"),
        ("property_details", "hasGarage"),
        ("property_details", "hasHeating"),
        ("features", "hasAssociation"),
    ]
    
    for section, field in boolean_fields:
        if section in encoded_data and isinstance(encoded_data[section], dict):
            value = encoded_data[section].get(field)
            if value is True:
                encoded_data[section][field] = 1
            else:  # False or None
                encoded_data[section][field] = 0
    
    # Handle taxAssessedValue: add missing flag and fill nulls with median
    if "financial" in encoded_data and isinstance(encoded_data["financial"], dict):
        tax_assessed_value = encoded_data["financial"].get("taxAssessedValue")
        
        # Add missing flag
        if tax_assessed_value is None:
            encoded_data["financial"]["taxAssessedValue_is_missing"] = 1
            # Fill with median if provided
            if median_tax_assessed_value is not None:
                encoded_data["financial"]["taxAssessedValue"] = median_tax_assessed_value
        else:
            encoded_data["financial"]["taxAssessedValue_is_missing"] = 0
        
        # Handle monthlyHoaFee: fill nulls with 0
        monthly_hoa_fee = encoded_data["financial"].get("monthlyHoaFee")
        if monthly_hoa_fee is None:
            encoded_data["financial"]["monthlyHoaFee"] = 0
    
    # Phase 2: Simple, Logical Imputation - Fill nulls with 0
    # property_details.hasSpa and hasView (already handled by boolean normalization, but ensure explicit)
    if "property_details" not in encoded_data:
        encoded_data["property_details"] = {}
    
    if isinstance(encoded_data["property_details"], dict):
        # hasSpa and hasView are already normalized to 0 for None in boolean_fields section above
        # hasFireplace is also already normalized to 0 for None
        
        # fireplaces: Fill nulls with 0
        if encoded_data["property_details"].get("fireplaces") is None:
            encoded_data["property_details"]["fireplaces"] = 0
        
        # garageParkingCapacity: Fill nulls with 0
        if encoded_data["property_details"].get("garageParkingCapacity") is None:
            encoded_data["property_details"]["garageParkingCapacity"] = 0
        
        # CRITICAL: Ensure appliance features always exist (even if missing from input)
        # This handles houses that were processed before the preprocessing fix
        appliance_features = [
            "standard_appliance_score",
            "has_built_in_refrigerator",
            "has_double_oven",
            "has_warming_drawer",
            "has_wine_refrigerator"
        ]
        
        for feature in appliance_features:
            if feature not in encoded_data["property_details"]:
                # Set default values: 0 for all appliance features
                encoded_data["property_details"][feature] = 0
    
    # Phase 5: Features to Ignore/Remove - Fill nulls
    if "features" in encoded_data and isinstance(encoded_data["features"], dict):
        # hasAssociation is already normalized to 0 for None in boolean_fields section above
        
        # numberOfUnitsInCommunity: Fill nulls with 0
        if encoded_data["features"].get("numberOfUnitsInCommunity") is None:
            encoded_data["features"]["numberOfUnitsInCommunity"] = 0
    
    # Flatten the entire structure
    flattened_data = flatten_dict(encoded_data)
    
    return flattened_data


def calculate_median_tax_assessed_value(houses_dir: str) -> float:
    """
    Calculate the median taxAssessedValue across all houses.
    
    Args:
        houses_dir: Directory containing house JSON files
        
    Returns:
        Median taxAssessedValue (float)
    """
    houses_path = Path(houses_dir)
    json_files = list(houses_path.glob("*.json"))
    
    tax_values = []
    
    for filepath in json_files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            tax_value = data.get("financial", {}).get("taxAssessedValue")
            if tax_value is not None:
                try:
                    tax_values.append(float(tax_value))
                except (ValueError, TypeError):
                    continue
        except Exception:
            continue
    
    if not tax_values:
        return 0.0
    
    return statistics.median(tax_values)


def flatten_dict(data: dict, parent_key: str = "", sep: str = "_") -> dict:
    """
    Flatten a nested dictionary structure.
    
    Args:
        data: Nested dictionary to flatten
        parent_key: Parent key prefix (for recursion)
        sep: Separator for nested keys
        
    Returns:
        Flattened dictionary with keys like "basic_info_bedrooms"
    """
    items = []
    
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        
        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_dict(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            # For lists, we could handle them differently, but for now just skip
            # (most lists should have been converted to one-hot already)
            continue
        else:
            items.append((new_key, value))
    
    return dict(items)


def process_houses(source_dir: str = "filtered_houses", dest_dir: str = "houses_onehot_encoded"):
    """
    Process all houses and apply one-hot encoding.
    
    Args:
        source_dir: Source directory containing house JSON files
        dest_dir: Destination directory for one-hot encoded houses
    """
    source_path = Path(source_dir)
    dest_path = Path(dest_dir)
    
    if not source_path.exists():
        print(f"❌ Source directory '{source_dir}' does not exist!")
        return
    
    dest_path.mkdir(exist_ok=True)
    
    json_files = list(source_path.glob("*.json"))
    total_files = len(json_files)
    
    print("=" * 70)
    print("ONE-HOT ENCODING HOUSES")
    print("=" * 70)
    print(f"📊 Found {total_files} houses to process\n")
    
    # Step 1: Analyze and get top values
    print("Step 1: Analyzing features to determine top values...")
    print("-" * 70)
    
    # Get all unique values for homeType and zipcode
    print("  Analyzing homeType...")
    all_home_types = sorted(list(set(get_top_values(source_dir, "basic_info.homeType", 100))))  # Get all unique (should be 2)
    
    print("  Analyzing zipcode...")
    all_zipcodes = sorted(list(set(get_top_values(source_dir, "basic_info.zipcode", 100))))  # Get all unique (should be 30)
    
    print("  Analyzing neighborhood (Top 20)...")
    top_neighborhoods = get_top_values(source_dir, "location.neighborhood", 20)
    
    print("  Analyzing heating (Top 5)...")
    top_heating = get_top_values(source_dir, "property_details.heating", 5)
    
    print("  Analyzing flooring (Top 4)...")
    top_flooring = get_top_values(source_dir, "property_details.flooring", 4)
    
    print("  Analyzing parkingFeatures (Top 5)...")
    top_parking = get_top_values(source_dir, "property_details.parkingFeatures", 5)
    
    print("  Analyzing exteriorFeatures (Top 3)...")
    top_exterior = get_top_values(source_dir, "property_details.exteriorFeatures", 3)
    
    # Create encoding configuration
    encoding_config = {
        "homeType": all_home_types,
        "zipcode": all_zipcodes,
        "neighborhood": top_neighborhoods,
        "heating": top_heating,
        "flooring": top_flooring,
        "parkingFeatures": top_parking,
        "exteriorFeatures": top_exterior,
    }
    
    print("\n" + "=" * 70)
    print("ENCODING CONFIGURATION")
    print("=" * 70)
    print(f"homeType: {len(all_home_types)} values")
    print(f"zipcode: {len(all_zipcodes)} values")
    print(f"neighborhood: Top {len(top_neighborhoods)} + Other")
    print(f"heating: Top {len(top_heating)} + Other")
    print(f"flooring: Top {len(top_flooring)} + Other")
    print(f"parkingFeatures: Top {len(top_parking)} + Other")
    print(f"exteriorFeatures: Top {len(top_exterior)} + Other")
    print("=" * 70)
    
    # Step 1.5: Calculate median for taxAssessedValue imputation
    print("\nStep 1.5: Calculating median for taxAssessedValue imputation...")
    print("-" * 70)
    median_tax_assessed_value = calculate_median_tax_assessed_value(source_dir)
    print(f"  Median taxAssessedValue: {median_tax_assessed_value:,.2f}")
    
    # Step 2: Process and encode all houses
    print("\nStep 2: Encoding houses...")
    print("-" * 70)
    
    processed_count = 0
    error_count = 0
    skipped_count = 0
    
    for i, filepath in enumerate(sorted(json_files), 1):
        if i % 500 == 0:
            print(f"Progress: {i}/{total_files} ({i*100//total_files}%) - Processed: {processed_count}, Skipped: {skipped_count}, Errors: {error_count}")
        
        try:
            # Load house data
            with open(filepath, 'r', encoding='utf-8') as f:
                house_data = json.load(f)
            
            # Phase 1: Filter rows with unfixable nulls
            # Skip if lastSoldPrice is null (target variable)
            if house_data.get("financial", {}).get("lastSoldPrice") is None:
                skipped_count += 1
                continue
            
            # Skip if latitude or longitude is null (critical location feature)
            if (house_data.get("location", {}).get("latitude") is None or 
                house_data.get("location", {}).get("longitude") is None):
                skipped_count += 1
                continue
            
            # Apply one-hot encoding (with flattening, null handling, and imputation)
            encoded_data = one_hot_encode_house(house_data, encoding_config, median_tax_assessed_value)
            
            # Save encoded data
            dest_file = dest_path / filepath.name
            with open(dest_file, 'w', encoding='utf-8') as f:
                json.dump(encoded_data, f, indent=2, ensure_ascii=False)
            
            processed_count += 1
            
        except Exception as e:
            print(f"⚠️  Error processing {filepath.name}: {e}")
            error_count += 1
    
    print("\n" + "=" * 70)
    print("ENCODING SUMMARY")
    print("=" * 70)
    print(f"Total files: {total_files}")
    print(f"✅ Successfully encoded: {processed_count}")
    print(f"⏭️  Skipped (unfixable nulls): {skipped_count}")
    print(f"❌ Errors: {error_count}")
    print(f"📁 Output directory: {dest_dir}")
    print("=" * 70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="One-hot encode categorical features in house data")
    parser.add_argument(
        "--source",
        type=str,
        default="filtered_houses",
        help="Source directory containing house JSON files (default: filtered_houses)"
    )
    parser.add_argument(
        "--dest",
        type=str,
        default="houses_onehot_encoded",
        help="Destination directory for one-hot encoded houses (default: houses_onehot_encoded)"
    )
    
    args = parser.parse_args()
    process_houses(args.source, args.dest)

