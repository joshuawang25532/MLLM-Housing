"""
House preprocessing script.
First categorizes houses into SINGLE_FAMILY and CONDO lists, then processes only those types.
Copies only valid houses (no errors or warnings) to the destination directory.
Removes leaking/noisy features and transforms data structure.
"""
import json
import shutil
import sys
from pathlib import Path

# Add project root to path so imports work when run directly
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.data_validator import HouseValidator

# Fields to remove by section (leaking or noisy features)
# Format: {section_name: [list of field names to remove]}
FIELDS_TO_REMOVE = {
    "basic_info": [
        "address",
        "city",
        "state",
        "zpid",
        "lotSize",
        "yearBuilt",
        "lotAreaValue",
        "lotAreaUnits",
        "livingAreaUnits"
    ],
    "location": [
        "county",
        "timeZone"
    ],
    "financial": [
        "price",
        "zestimate",
        "zestimateHighPercent",
        "zestimateLowPercent",
        "rentZestimate",
        "pricePerSquareFoot",
        "hoaFee",
        "dateSoldString",
        "dateSold",
        "taxAssessedYear",
        "propertyTaxRate"
    ],
    "features": [
        "associationFee",
        "associationFeeIncludes",
        "hasPetsAllowed",
        "hasHomeWarranty",
        "hasLandLease",
        "isNewConstruction"
    ],
    "property_details": [
        "rooms",
        "interiorFeatures",
        "securityFeatures",
        "cooling",
        "appliances",
        "architecturalStyle",
        "propertyCondition",
        "levels",
        "stories",
        "hasCooling"
    ],
}

# Appliance preprocessing glossary
# Standard appliance categories for scoring (0-6)
STANDARD_APPLIANCE_CATEGORIES = {
    "laundry": [
        "Washer", "Dryer", "Washer/Dryer", "Washer/Dryer Stacked",
        "Washer/Dryer Stacked Included", "WD Hookup"
    ],
    "dishwasher": [
        "Dishwasher"
    ],
    "refrigerator": [
        "Free-Standing Refrigerator", "Refrigerator", "Refrigerator-Normal"
    ],
    "oven_range": [
        "Free-Standing Gas Range", "Free-Standing Electric Range", "Free-Standing Range",
        "Gas Range", "Electric Range", "Range", "Range / Oven",
        "Free-Standing Gas Oven", "Free-Standing Electric Oven", "Gas Oven", "Electric Oven",
        "Oven", "Oven/Range", "Gas Oven/Range", "Electric Oven/Range",
        "Built In Gas Oven/Range", "Built In Oven/Range", "Built-In Gas Range",
        "Built-In Electric Range", "Built In Oven", "Built-In Gas Oven",
        "Built-In Electric Oven", "Gas Cooktop", "Electric Cooktop", "Cooktop"
    ],
    "disposal": [
        "Disposal", "Garbage disposal", "Garbage Disposal"
    ],
    "microwave": [
        "Microwave"
    ]
}

# Luxury appliance features (binary flags)
LUXURY_APPLIANCES = [
    "Wine Refrigerator",
    "Double Oven",
    "Built-In Refrigerator",
    "Warming Drawer",
]


def preprocess_appliances(appliances_list: list) -> dict:
    """
    Transform appliances list into meaningful features.
    
    Creates:
    - standard_appliance_score: 0-6 score based on standard categories
    - Binary flags for luxury appliances
    
    Args:
        appliances_list: List of appliance strings (can be None or empty)
        
    Returns:
        Dictionary with standard_appliance_score and luxury flags
    """
    if not appliances_list or not isinstance(appliances_list, list):
        # Return all zeros if no appliances
        result = {
            "standard_appliance_score": 0
        }
        for luxury in LUXURY_APPLIANCES:
            result[f"has_{normalize_appliance_name(luxury)}"] = 0
        return result
    
    # Normalize appliance names (lowercase for matching)
    appliances_normalized = [str(app).strip() for app in appliances_list if app is not None]
    
    # Calculate standard appliance score
    standard_score = 0
    
    for category, keywords in STANDARD_APPLIANCE_CATEGORIES.items():
        # Check if any appliance matches this category
        found = False
        for appliance in appliances_normalized:
            for keyword in keywords:
                if keyword.lower() in appliance.lower() or appliance.lower() in keyword.lower():
                    found = True
                    break
            if found:
                break
        
        if found:
            standard_score += 1
    
    # Check for luxury appliances
    luxury_flags = {}
    for luxury in LUXURY_APPLIANCES:
        flag_name = f"has_{normalize_appliance_name(luxury)}"
        found = False
        
        for appliance in appliances_normalized:
            if luxury.lower() in appliance.lower() or appliance.lower() in luxury.lower():
                found = True
                break
        
        luxury_flags[flag_name] = 1 if found else 0
    
    result = {
        "standard_appliance_score": standard_score
    }
    result.update(luxury_flags)
    
    return result


def normalize_appliance_name(name: str) -> str:
    """
    Normalize appliance name for use as a feature name.
    
    Args:
        name: Original appliance name
        
    Returns:
        Normalized string suitable for feature name
    """
    normalized = name.replace(' ', '_').replace('/', '_').replace('-', '_')
    normalized = normalized.replace('(', '').replace(')', '').replace("'", '')
    normalized = normalized.replace(',', '').replace('.', '').replace(':', '')
    # Remove multiple underscores
    while '__' in normalized:
        normalized = normalized.replace('__', '_')
    # Remove leading/trailing underscores
    normalized = normalized.strip('_').lower()
    return normalized


def validate_zpid_consistency(data: dict) -> None:
    """
    Validate that zpid in metadata matches zpid in basic_info.
    
    Args:
        data: House data dictionary
        
    Raises:
        ValueError: If zpid values don't match
    """
    basic_info_zpid = data.get("basic_info", {}).get("zpid")
    metadata_zpid = data.get("metadata", {}).get("zpid")
    
    # Skip validation if either is missing
    if basic_info_zpid is None or metadata_zpid is None:
        return
    
    # Compare zpids
    if basic_info_zpid != metadata_zpid:
        raise ValueError(
            f"ZPID mismatch: basic_info.zpid={basic_info_zpid}, metadata.zpid={metadata_zpid}"
        )


def validate_schools(data: dict) -> None:
    """
    Validate that house has all 3 schools (Elementary, Middle, High) with non-null ratings.
    
    Args:
        data: House data dictionary
        
    Raises:
        ValueError: If schools are missing or ratings are null
    """
    schools = data.get("schools", [])
    
    if not schools:
        raise ValueError("Missing schools section or schools array is empty")
    
    # Build a dictionary by level for easy lookup
    schools_by_level = {}
    for school in schools:
        if not isinstance(school, dict):
            continue
        level = school.get("level")
        if level:
            schools_by_level[level] = school
    
    # Check for all 3 required levels
    required_levels = ["Elementary", "Middle", "High"]
    missing_levels = [level for level in required_levels if level not in schools_by_level]
    
    if missing_levels:
        raise ValueError(f"Missing school levels: {', '.join(missing_levels)}")
    
    # Check that all ratings are not null
    for level in required_levels:
        school = schools_by_level[level]
        rating = school.get("rating")
        if rating is None:
            raise ValueError(f"{level} school rating is null")


def validate_scores(data: dict) -> None:
    """
    Validate that house has all 3 scores (walkScore, transitScore, bikeScore) with non-null values.
    
    Args:
        data: House data dictionary
        
    Raises:
        ValueError: If scores are missing or any score is null
    """
    scores = data.get("scores", {})
    
    if not scores:
        raise ValueError("Missing scores section")
    
    required_scores = ["walkScore", "transitScore", "bikeScore"]
    missing_scores = [score for score in required_scores if score not in scores]
    
    if missing_scores:
        raise ValueError(f"Missing scores: {', '.join(missing_scores)}")
    
    # Check that all scores are not null
    null_scores = [score for score in required_scores if scores[score] is None]
    
    if null_scores:
        raise ValueError(f"Null scores: {', '.join(null_scores)}")


def transform_schools(data: dict) -> dict:
    """
    Transform schools array into flat structure with 6 fields:
    - elementary_school_rating
    - elementary_school_distance
    - middle_school_rating
    - middle_school_distance
    - high_school_rating
    - high_school_distance
    
    Args:
        data: House data dictionary (will be modified in place)
        
    Returns:
        Modified data dictionary with transformed schools section
    """
    schools = data.get("schools", [])
    
    # Build a dictionary by level
    schools_by_level = {}
    for school in schools:
        if isinstance(school, dict):
            level = school.get("level")
            if level:
                schools_by_level[level] = school
    
    # Extract ratings and distances
    transformed_schools = {}
    
    for level, field_prefix in [("Elementary", "elementary"), ("Middle", "middle"), ("High", "high")]:
        school = schools_by_level.get(level, {})
        transformed_schools[f"{field_prefix}_school_rating"] = school.get("rating")
        transformed_schools[f"{field_prefix}_school_distance"] = school.get("distance")
    
    # Replace the schools array with the transformed flat structure
    data["schools"] = transformed_schools
    
    return data


def clean_house_data(data: dict) -> dict:
    """
    Remove leaking/noisy features from house data.
    Removes entire history, nearby, photos, and metadata sections.
    Removes specified fields from other sections (zpid from basic_info, county/timeZone from location, etc.).
    Transforms schools array into flat structure.
    Calculates house_age from yearBuilt (2025 - yearBuilt) and replaces yearBuilt.
    Normalizes lotAreaValue to lotSize_sqft based on lotAreaUnits (converts Acres to sqft).
    Preprocesses appliances: converts appliances list into standard_appliance_score (0-6) and luxury flags,
    then removes the original appliances list.
    
    Args:
        data: House data dictionary
        
    Returns:
        Cleaned house data dictionary
    """
    # Create a deep copy to avoid modifying the original
    cleaned_data = json.loads(json.dumps(data))
    
    # Remove entire sections
    cleaned_data.pop("history", None)
    cleaned_data.pop("nearby", None)
    cleaned_data.pop("photos", None)
    cleaned_data.pop("metadata", None)
    
    # Calculate house_age from yearBuilt before removing yearBuilt
    if "basic_info" in cleaned_data and isinstance(cleaned_data["basic_info"], dict):
        year_built = cleaned_data["basic_info"].get("yearBuilt")
        if year_built is not None:
            try:
                year_built_int = int(year_built)
                house_age = 2025 - year_built_int
                cleaned_data["basic_info"]["house_age"] = house_age
            except (ValueError, TypeError):
                # If yearBuilt is not a valid integer, set house_age to None
                cleaned_data["basic_info"]["house_age"] = None
    
    # Normalize lotAreaValue to lotSize_sqft based on lotAreaUnits (before removing them)
    if "basic_info" in cleaned_data and isinstance(cleaned_data["basic_info"], dict):
        home_type = cleaned_data["basic_info"].get("homeType")
        lot_area_value = cleaned_data["basic_info"].get("lotAreaValue")
        lot_area_units = cleaned_data["basic_info"].get("lotAreaUnits")
        
        # If homeType is CONDO, set lotSize_sqft to 0
        if home_type == "CONDO":
            cleaned_data["basic_info"]["lotSize_sqft"] = 0
        elif lot_area_value is not None:
            try:
                lot_area_value_float = float(lot_area_value)
                
                # Convert to square feet based on units
                if lot_area_units == "Acres":
                    # 1 acre = 43,560 square feet
                    lot_size_sqft = lot_area_value_float * 43560
                elif lot_area_units in ["Square Feet", "sqft"]:
                    # Already in square feet
                    lot_size_sqft = lot_area_value_float
                elif lot_area_units is None:
                    # If units are missing, assume square feet (most common)
                    lot_size_sqft = lot_area_value_float
                else:
                    # Unknown unit, set to None
                    lot_size_sqft = None
                
                cleaned_data["basic_info"]["lotSize_sqft"] = lot_size_sqft
            except (ValueError, TypeError):
                # If lotAreaValue is not a valid number, set lotSize_sqft to None
                cleaned_data["basic_info"]["lotSize_sqft"] = None
        else:
            # If lotAreaValue is None, set lotSize_sqft to None
            cleaned_data["basic_info"]["lotSize_sqft"] = None
    
    # Preprocess appliances before removing them
    appliances_list = None
    if "property_details" in cleaned_data and isinstance(cleaned_data["property_details"], dict):
        appliances_list = cleaned_data["property_details"].get("appliances")
    
    # Remove specified fields from each section
    for section, fields_to_remove in FIELDS_TO_REMOVE.items():
        if section in cleaned_data and isinstance(cleaned_data[section], dict):
            for field in fields_to_remove:
                cleaned_data[section].pop(field, None)
    
    # Transform schools array into flat structure
    cleaned_data = transform_schools(cleaned_data)
    
    # Add preprocessed appliance features to property_details
    # IMPORTANT: Always create appliance features, even if appliances field was missing or property_details doesn't exist
    # This ensures all houses have these features (with default 0 values if no appliances)
    if "property_details" not in cleaned_data:
        cleaned_data["property_details"] = {}
    
    # preprocess_appliances handles None/empty lists correctly (returns all zeros)
    appliance_features = preprocess_appliances(appliances_list)
    cleaned_data["property_details"].update(appliance_features)
    
    return cleaned_data


def categorize_houses(source_dir="data/raw_houses"):
    """
    Scan all houses and categorize them into SINGLE_FAMILY and CONDO lists.
    
    Args:
        source_dir: Source directory containing house JSON files
        
    Returns:
        Tuple of (single_family_files, condo_files) where each is a list of Path objects
    """
    source_path = Path(source_dir)
    
    if not source_path.exists():
        print(f"❌ Source directory '{source_dir}' does not exist!")
        return [], []
    
    # Get all JSON files
    json_files = list(source_path.glob("*.json"))
    # Exclude visited_houses.json if it exists
    json_files = [f for f in json_files if f.name != "visited_houses.json"]
    
    single_family_files = []
    condo_files = []
    other_files = []
    error_files = []
    
    print("=" * 70)
    print("CATEGORIZING HOUSES")
    print("=" * 70)
    print(f"📊 Scanning {len(json_files)} house files...\n")
    
    for i, filepath in enumerate(sorted(json_files), 1):
        if i % 500 == 0:
            print(f"Progress: {i}/{len(json_files)} ({i*100//len(json_files)}%) - Singles: {len(single_family_files)}, Condos: {len(condo_files)}, Other: {len(other_files)}")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                house_data = json.load(f)
            
            home_type = house_data.get("basic_info", {}).get("homeType")
            
            if home_type == "SINGLE_FAMILY":
                single_family_files.append(filepath)
            elif home_type == "CONDO":
                condo_files.append(filepath)
            else:
                other_files.append(filepath)
        except json.JSONDecodeError as e:
            error_files.append(filepath)
        except Exception as e:
            error_files.append(filepath)
    
    print("\n" + "=" * 70)
    print("CATEGORIZATION SUMMARY")
    print("=" * 70)
    print(f"Total files scanned: {len(json_files)}")
    print(f"🏠 SINGLE_FAMILY: {len(single_family_files)}")
    print(f"🏢 CONDO: {len(condo_files)}")
    print(f"📦 Other types: {len(other_files)}")
    if error_files:
        print(f"❌ Error parsing: {len(error_files)}")
    print("=" * 70)
    
    return single_family_files, condo_files


def preprocess_houses(source_dir="data/raw_houses", dest_dir="data/preprocessed_houses"):
    """
    Preprocess only SINGLE_FAMILY and CONDO houses.
    First categorizes houses, then processes only those types.
    Removes leaking/noisy features:
    - Entire sections: history, nearby, photos, metadata
    - From basic_info: address, city, state, zpid, lotSize, yearBuilt, lotAreaValue, lotAreaUnits
    - From location: county, timeZone
    - From financial: price, zestimate, zestimateHighPercent, zestimateLowPercent, rentZestimate, pricePerSquareFoot, hoaFee, dateSoldString, dateSold, taxAssessedYear, propertyTaxRate
    - From features: associationFee, associationFeeIncludes
    - From property_details: rooms, interiorFeatures, securityFeatures, cooling, appliances
    Transforms:
    - Calculates house_age from yearBuilt (2025 - yearBuilt) and replaces yearBuilt
    - Normalizes lotAreaValue to lotSize_sqft based on lotAreaUnits (converts Acres to sqft, keeps Square Feet/sqft as-is)
    - Schools array into flat structure (elementary_school_rating, elementary_school_distance, etc.)
    Validates:
    - ZPID consistency between basic_info and metadata
    - All 3 schools (Elementary, Middle, High) exist with non-null ratings
    - All 3 scores (walkScore, transitScore, bikeScore) exist with non-null values
    
    Args:
        source_dir: Source directory containing house JSON files
        dest_dir: Destination directory for preprocessed houses
    """
    source_path = Path(source_dir)
    dest_path = Path(dest_dir)
    
    # Check if source directory exists
    if not source_path.exists():
        print(f"❌ Source directory '{source_dir}' does not exist!")
        return
    
    # Create destination directory if it doesn't exist
    dest_path.mkdir(exist_ok=True)
    print(f"\n📁 Destination directory: {dest_path}\n")
    
    # Step 1: Categorize houses
    single_family_files, condo_files = categorize_houses(source_dir)
    
    # Combine the lists of files we care about
    files_to_process = single_family_files + condo_files
    total_files = len(files_to_process)
    
    if total_files == 0:
        print("❌ No SINGLE_FAMILY or CONDO houses found to process!")
        return
    
    print(f"\n📊 Processing {total_files} houses ({len(single_family_files)} SINGLE_FAMILY + {len(condo_files)} CONDO)\n")
    
    # Initialize validator
    validator = HouseValidator(houses_dir=source_dir)
    
    # Statistics
    valid_no_issues_count = 0  # Valid with no errors or warnings
    has_issues_count = 0  # Has errors or warnings
    copied_count = 0
    skipped_count = 0
    
    print("=" * 70)
    print("PROCESSING HOUSES")
    print("=" * 70)
    
    for i, filepath in enumerate(sorted(files_to_process), 1):
        # Validate the house
        is_valid, errors, warnings = validator.validate_file(filepath)
        
        # Show progress every 100 files
        if i % 100 == 0:
            print(f"Progress: {i}/{total_files} ({i*100//total_files}%) - Valid (no issues): {valid_no_issues_count}, Has issues: {has_issues_count}")
        
        # Only copy if valid AND has no warnings (no issues at all)
        if is_valid and len(warnings) == 0:
            valid_no_issues_count += 1
            # Load, clean, and save to destination
            dest_file = dest_path / filepath.name
            try:
                # Load the house data
                with open(filepath, 'r', encoding='utf-8') as f:
                    house_data = json.load(f)
                
                # CRITICAL: Explicit check for null lastSoldPrice (target variable)
                # This is a hard requirement - cannot proceed without it
                if house_data.get("financial", {}).get("lastSoldPrice") is None:
                    skipped_count += 1
                    has_issues_count += 1
                    continue
                
                # CRITICAL: Explicit check for null latitude/longitude (critical location features)
                if (house_data.get("location", {}).get("latitude") is None or 
                    house_data.get("location", {}).get("longitude") is None):
                    skipped_count += 1
                    has_issues_count += 1
                    continue
                
                # Validate zpid consistency (throws error if mismatch)
                validate_zpid_consistency(house_data)
                
                # NOTE: Schools and scores validation removed - missing values are allowed
                # and will be imputed later using KNN. This preserves ~3% more data.
                # validate_schools(house_data)  # REMOVED - allow missing schools
                # validate_scores(house_data)   # REMOVED - allow missing scores
                
                # Clean the data (remove leaking/noisy features and history section, transform schools)
                cleaned_data = clean_house_data(house_data)
                
                # Save cleaned data to destination
                with open(dest_file, 'w', encoding='utf-8') as f:
                    json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
                
                copied_count += 1
            except ValueError as e:
                # Validation error (ZPID only - schools/scores validation removed)
                # Print ZPID errors
                error_msg = str(e)
                if "ZPID" in error_msg:
                    print(f"❌ ZPID VALIDATION ERROR {filepath.name}: {e}")
                    skipped_count += 1
                    has_issues_count += 1
                else:
                    # Other validation errors (shouldn't happen now, but handle gracefully)
                    print(f"⚠️  Validation error {filepath.name}: {e}")
                    skipped_count += 1
                    has_issues_count += 1
            except json.JSONDecodeError as e:
                print(f"⚠️  Error parsing JSON {filepath.name}: {e}")
                skipped_count += 1
            except Exception as e:
                print(f"⚠️  Error processing {filepath.name}: {e}")
                skipped_count += 1
        else:
            has_issues_count += 1
            skipped_count += 1
    
    # Final summary
    print("\n" + "=" * 70)
    print("PREPROCESSING SUMMARY")
    print("=" * 70)
    print(f"Total files processed: {total_files}")
    print(f"  🏠 SINGLE_FAMILY: {len(single_family_files)}")
    print(f"  🏢 CONDO: {len(condo_files)}")
    print(f"✅ Valid houses (no issues): {valid_no_issues_count}")
    print(f"❌ Houses with issues (errors or warnings): {has_issues_count}")
    print(f"📋 Copied to {dest_dir}: {copied_count}")
    print(f"⏭️  Skipped: {skipped_count}")
    print("=" * 70)


def filter_houses(source_dir="data/preprocessed_houses", dest_dir="data/preprocessed_houses", home_types=None):
    """
    Filter already-preprocessed houses to only include specified home types.
    This is a legacy function for filtering already-preprocessed houses.
    Note: The main preprocessing now filters by home type upfront.
    
    Args:
        source_dir: Source directory containing preprocessed house JSON files (default: houses_preprocessed)
        dest_dir: Destination directory for filtered houses (default: filtered_houses)
        home_types: List of home types to keep. If None, defaults to ["SINGLE_FAMILY", "CONDO"]
    """
    if home_types is None:
        home_types = ["SINGLE_FAMILY", "CONDO"]
    
    source_path = Path(source_dir)
    dest_path = Path(dest_dir)
    
    # Check if source directory exists
    if not source_path.exists():
        print(f"❌ Source directory '{source_dir}' does not exist!")
        return
    
    # Create destination directory if it doesn't exist
    dest_path.mkdir(exist_ok=True)
    print(f"📁 Filtering houses from '{source_dir}' to '{dest_dir}'")
    print(f"📋 Keeping only: {', '.join(home_types)}\n")
    
    # Get all JSON files
    json_files = list(source_path.glob("*.json"))
    
    total_files = len(json_files)
    print(f"📊 Found {total_files} house files to filter\n")
    
    # Statistics
    kept_count = 0
    filtered_out_count = 0
    home_type_counts = {}
    
    print("=" * 70)
    print("FILTERING HOUSES")
    print("=" * 70)
    
    for i, filepath in enumerate(sorted(json_files), 1):
        # Show progress every 500 files
        if i % 500 == 0:
            print(f"Progress: {i}/{total_files} ({i*100//total_files}%) - Kept: {kept_count}, Filtered out: {filtered_out_count}")
        
        try:
            # Load the house data
            with open(filepath, 'r', encoding='utf-8') as f:
                house_data = json.load(f)
            
            # Get home type
            basic_info = house_data.get("basic_info", {})
            home_type = basic_info.get("homeType")
            
            # Count home types
            if home_type not in home_type_counts:
                home_type_counts[home_type] = 0
            home_type_counts[home_type] += 1
            
            # Check if home type should be kept
            if home_type in home_types:
                # Data should already be cleaned, but ensure it is
                cleaned_data = clean_house_data(house_data)
                
                # Save cleaned data to destination
                dest_file = dest_path / filepath.name
                with open(dest_file, 'w', encoding='utf-8') as f:
                    json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
                kept_count += 1
            else:
                filtered_out_count += 1
                
        except json.JSONDecodeError as e:
            print(f"⚠️  Error parsing JSON {filepath.name}: {e}")
            filtered_out_count += 1
        except Exception as e:
            print(f"⚠️  Error processing {filepath.name}: {e}")
            filtered_out_count += 1
    
    # Final summary
    print("\n" + "=" * 70)
    print("FILTERING SUMMARY")
    print("=" * 70)
    print(f"Total files processed: {total_files}")
    print(f"✅ Kept ({', '.join(home_types)}): {kept_count}")
    print(f"⏭️  Filtered out: {filtered_out_count}")
    print(f"\n📊 Breakdown by home type:")
    for home_type, count in sorted(home_type_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_files) * 100
        status = "✅ KEPT" if home_type in home_types else "⏭️  FILTERED"
        print(f"  {home_type:20s}: {count:5d} ({percentage:5.2f}%) - {status}")
    print("=" * 70)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Preprocess houses: categorize SINGLE_FAMILY and CONDO houses, then process only those types"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="data/raw_houses",
        help="Source directory containing house JSON files (default: data/raw_houses)"
    )
    parser.add_argument(
        "--dest",
        type=str,
        default="data/preprocessed_houses",
        help="Destination directory for preprocessed houses (default: data/preprocessed_houses)"
    )
    parser.add_argument(
        "--filter-only",
        action="store_true",
        help="Only run filtering step on already-preprocessed houses. Requires --filter-source and --filter-dest"
    )
    parser.add_argument(
        "--filter-source",
        type=str,
        default="houses_preprocessed",
        help="Source directory for filtering (default: houses_preprocessed)"
    )
    parser.add_argument(
        "--filter-dest",
        type=str,
        default="filtered_houses",
        help="Destination directory for filtered houses (default: filtered_houses)"
    )
    
    args = parser.parse_args()
    
    if args.filter_only:
        # Only run filtering (legacy mode for already-preprocessed houses)
        filter_houses(source_dir=args.filter_source, dest_dir=args.filter_dest)
    else:
        # Run preprocessing (now includes filtering by home type upfront)
        preprocess_houses(source_dir=args.source, dest_dir=args.dest)


if __name__ == "__main__":
    main()

