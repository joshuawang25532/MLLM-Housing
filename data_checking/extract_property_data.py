"""
Extract and display the property data structure from the detail page.
Parses gdpClientCache to show all available fields.
"""
import json

def extract_property_data(file_path="nodriver_detail_test.json"):
    """Extract and display property data structure."""
    print("="*80)
    print("PROPERTY DATA EXTRACTION")
    print("="*80 + "\n")
    
    with open(file_path, "r") as f:
        data = json.load(f)
    
    # Extract gdpClientCache
    next_data = data["__NEXT_DATA__"]
    comp_props = next_data["props"]["pageProps"]["componentProps"]
    gdp_cache_str = comp_props.get("gdpClientCache", "")
    
    if not gdp_cache_str:
        print("❌ No gdpClientCache found")
        return
    
    # Parse the JSON string
    gdp_cache = json.loads(gdp_cache_str)
    
    # Get the property data (the key is dynamic based on query)
    query_key = list(gdp_cache.keys())[0]
    query_result = gdp_cache[query_key]
    property_data = query_result.get("property", {})
    
    print(f"✅ Found property data for ZPID: {property_data.get('zpid')}")
    print(f"   Address: {property_data.get('streetAddress', 'N/A')}")
    print(f"   City: {property_data.get('city', 'N/A')}, {property_data.get('state', 'N/A')}")
    print()
    
    # Display all top-level keys
    print("="*80)
    print("📋 ALL PROPERTY DATA FIELDS")
    print("="*80)
    
    def print_structure(obj, prefix="", max_depth=3, current_depth=0):
        """Recursively print structure."""
        if current_depth >= max_depth:
            return
        
        if isinstance(obj, dict):
            for key, value in sorted(obj.items()):
                current_prefix = f"{prefix}.{key}" if prefix else key
                if isinstance(value, dict):
                    print(f"  {current_prefix}: dict")
                    if current_depth < max_depth - 1:
                        print_structure(value, current_prefix, max_depth, current_depth + 1)
                elif isinstance(value, list):
                    print(f"  {current_prefix}: list[{len(value)}]")
                    if value and isinstance(value[0], dict) and current_depth < max_depth - 1:
                        print(f"    (first item keys: {list(value[0].keys())[:10]})")
                else:
                    value_str = str(value)
                    if len(value_str) > 100:
                        value_str = value_str[:100] + "..."
                    print(f"  {current_prefix}: {type(value).__name__} = {value_str}")
        elif isinstance(obj, list) and obj:
            print(f"  {prefix}: list[{len(obj)}]")
            if isinstance(obj[0], dict):
                print(f"    (first item keys: {list(obj[0].keys())[:10]})")
    
    print_structure(property_data, max_depth=2)
    
    # Highlight important fields
    print("\n" + "="*80)
    print("🔍 KEY FIELDS OF INTEREST")
    print("="*80)
    
    important_fields = {
        "Basic Info": [
            "zpid", "streetAddress", "city", "state", "zipcode", 
            "bedrooms", "bathrooms", "price", "yearBuilt", "homeType"
        ],
        "Location": [
            "address", "neighborhood", "community", "subdivision",
            "latitude", "longitude"
        ],
        "Scores/Ratings": [
            "walkScore", "transitScore", "bikeScore", 
            "schoolRatings", "crimeData"
        ],
        "Property Details": [
            "livingAreaValue", "lotAreaValue", "lotAreaUnits",
            "hoaFee", "taxAssessedValue", "zestimate"
        ],
        "Neighborhood": [
            "neighborhood", "nearbyHomes", "localInfo"
        ]
    }
    
    for category, fields in important_fields.items():
        print(f"\n📁 {category}:")
        found_any = False
        for field in fields:
            # Check nested paths
            value = property_data
            path_parts = field.split(".")
            try:
                for part in path_parts:
                    if isinstance(value, dict) and part in value:
                        value = value[part]
                    elif isinstance(value, list):
                        value = value[0] if value else None
                        if isinstance(value, dict) and part in value:
                            value = value[part]
                        else:
                            value = None
                            break
                    else:
                        value = None
                        break
                
                if value is not None:
                    found_any = True
                    value_str = str(value)
                    if len(value_str) > 150:
                        value_str = value_str[:150] + "..."
                    print(f"  ✅ {field}: {value_str}")
            except:
                pass
        
        if not found_any:
            print(f"  ⚠️  None of these fields found: {', '.join(fields)}")
    
    # Search for score-related fields
    print("\n" + "="*80)
    print("🔎 SEARCHING FOR SCORE/RATING FIELDS")
    print("="*80)
    
    def find_fields(obj, keywords, path=""):
        """Find fields matching keywords."""
        results = []
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key
                key_lower = key.lower()
                if any(kw.lower() in key_lower for kw in keywords):
                    results.append({
                        "path": current_path,
                        "value": value,
                        "type": type(value).__name__
                    })
                if isinstance(value, (dict, list)):
                    results.extend(find_fields(value, keywords, current_path))
        elif isinstance(obj, list):
            for i, item in enumerate(obj[:3]):  # Check first 3 items
                results.extend(find_fields(item, keywords, f"{path}[{i}]"))
        return results
    
    score_keywords = ["score", "rating", "walk", "transit", "bike", "school", "crime"]
    score_fields = find_fields(property_data, score_keywords)
    
    if score_fields:
        print(f"\nFound {len(score_fields)} score/rating related fields:")
        for item in score_fields[:20]:  # Show first 20
            value_str = str(item["value"])
            if len(value_str) > 100:
                value_str = value_str[:100] + "..."
            print(f"  • {item['path']} ({item['type']}): {value_str}")
    else:
        print("\n⚠️  No score/rating fields found in property data")
        print("   (They may be in a different location or loaded dynamically)")
    
    # Save extracted property data
    output_file = "property_data_extracted.json"
    with open(output_file, "w") as f:
        json.dump(property_data, f, indent=2)
    print(f"\n✅ Full property data saved to {output_file}")
    
    print("\n" + "="*80)
    print("✅ EXTRACTION COMPLETE")
    print("="*80)

if __name__ == "__main__":
    extract_property_data()

