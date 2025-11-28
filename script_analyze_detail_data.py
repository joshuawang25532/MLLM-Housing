"""
Analyze the structure of data extracted from Zillow detail pages.
Explores __NEXT_DATA__ and other sources to identify available fields.
"""
import json
from pathlib import Path
from collections import defaultdict

def explore_dict(obj, path="", max_depth=5, current_depth=0):
    """Recursively explore a dictionary structure."""
    if current_depth >= max_depth:
        return {}
    
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key
            if isinstance(value, dict):
                result[key] = {
                    "_type": "dict",
                    "_keys": list(value.keys())[:20],  # First 20 keys
                    "_sample": explore_dict(value, current_path, max_depth, current_depth + 1)
                }
            elif isinstance(value, list):
                result[key] = {
                    "_type": "list",
                    "_length": len(value),
                    "_sample": explore_dict(value[0] if value and isinstance(value[0], dict) else value, current_path, max_depth, current_depth + 1) if value else None
                }
            else:
                result[key] = {
                    "_type": type(value).__name__,
                    "_value": str(value)[:100] if value is not None else None
                }
        return result
    elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return explore_dict(obj[0], path, max_depth, current_depth)
    else:
        return {"_type": type(obj).__name__, "_value": str(obj)[:100]}

def find_key_paths(obj, target_keys, path="", results=None):
    """Find paths to specific keys in nested structure."""
    if results is None:
        results = []
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key
            if key.lower() in [t.lower() for t in target_keys]:
                results.append({
                    "path": current_path,
                    "type": type(value).__name__,
                    "sample": str(value)[:200] if not isinstance(value, (dict, list)) else None
                })
            find_key_paths(value, target_keys, current_path, results)
    elif isinstance(obj, list):
        for i, item in enumerate(obj[:5]):  # Check first 5 items
            find_key_paths(item, target_keys, f"{path}[{i}]", results)
    
    return results

def analyze_detail_data(file_path="nodriver_detail_test.json"):
    """Analyze the detail page data structure."""
    print("="*80)
    print("ZILLOW DETAIL PAGE DATA ANALYSIS")
    print("="*80 + "\n")
    
    with open(file_path, "r") as f:
        data = json.load(f)
    
    print("📊 TOP-LEVEL STRUCTURE")
    print("-"*80)
    for key in data.keys():
        if key == "__NEXT_DATA__":
            print(f"  ✅ {key}: {len(str(data[key])):,} characters")
        elif isinstance(data[key], dict):
            print(f"  📁 {key}: dict with {len(data[key])} keys")
        elif isinstance(data[key], list):
            print(f"  📋 {key}: list with {len(data[key])} items")
        else:
            print(f"  📄 {key}: {type(data[key]).__name__}")
    
    # Analyze __NEXT_DATA__
    if "__NEXT_DATA__" in data:
        print("\n" + "="*80)
        print("🔍 __NEXT_DATA__ STRUCTURE")
        print("="*80)
        
        next_data = data["__NEXT_DATA__"]
        print(f"\nTop-level keys: {list(next_data.keys())}")
        
        # Explore props.pageProps
        if "props" in next_data and "pageProps" in next_data["props"]:
            page_props = next_data["props"]["pageProps"]
            print(f"\n📁 pageProps keys: {list(page_props.keys())}")
            
            # Look for property data
            print("\n" + "-"*80)
            print("🔎 SEARCHING FOR KEY DATA STRUCTURES")
            print("-"*80)
            
            target_keys = [
                "property", "home", "zpid", "neighborhood", "rating", "score",
                "walk", "transit", "bike", "school", "crime", "details",
                "cat1", "hdpData", "homeInfo", "componentProps"
            ]
            
            found_paths = find_key_paths(page_props, target_keys)
            if found_paths:
                print(f"\nFound {len(found_paths)} relevant paths:")
                for item in found_paths[:30]:  # Show first 30
                    print(f"  • {item['path']} ({item['type']})")
                    if item['sample']:
                        print(f"    Sample: {item['sample']}")
            
            # Deep dive into componentProps
            if "componentProps" in page_props:
                print("\n" + "-"*80)
                print("📦 componentProps STRUCTURE")
                print("-"*80)
                comp_props = page_props["componentProps"]
                print(f"Keys: {list(comp_props.keys())}")
                
                # Explore componentProps structure
                comp_structure = explore_dict(comp_props, max_depth=3)
                print("\nStructure preview:")
                print(json.dumps(comp_structure, indent=2)[:2000])  # First 2000 chars
    
    # Analyze script_tags
    if "script_tags" in data and data["script_tags"]:
        print("\n" + "="*80)
        print("📜 SCRIPT TAGS DATA")
        print("="*80)
        for tag_id, tag_data in data["script_tags"].items():
            print(f"\n  Tag ID: {tag_id}")
            if isinstance(tag_data, dict):
                print(f"    Keys: {list(tag_data.keys())[:20]}")
            elif isinstance(tag_data, list):
                print(f"    List with {len(tag_data)} items")
    
    # Analyze visible_content
    if "visible_content" in data:
        print("\n" + "="*80)
        print("👁️ VISIBLE CONTENT EXTRACTION")
        print("="*80)
        visible = data["visible_content"]
        if "neighborhood_section" in visible and visible["neighborhood_section"]:
            print("\n✅ Neighborhood section found:")
            print(f"  Text preview: {visible['neighborhood_section'].get('text', '')[:300]}")
        
        if "rating_sections" in visible:
            print(f"\n✅ Found {len(visible['rating_sections'])} rating sections")
            for i, rating in enumerate(visible['rating_sections'][:5]):
                print(f"  {i+1}. {rating.get('text', '')[:100]}")
        
        if "property_details" in visible:
            print("\n✅ Property details patterns found:")
            for key, matches in visible["property_details"].items():
                print(f"  • {key}: {len(matches)} matches")
                if matches:
                    print(f"    Example: {matches[0][:100]}")
    
    # Try to extract specific property information
    print("\n" + "="*80)
    print("🏠 ATTEMPTING TO EXTRACT PROPERTY INFORMATION")
    print("="*80)
    
    def extract_nested(obj, path_parts):
        """Extract value from nested dict using path parts."""
        current = obj
        for part in path_parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            elif isinstance(current, list) and part.isdigit():
                current = current[int(part)]
            else:
                return None
        return current
    
    # Common paths to check
    property_paths = [
        ["props", "pageProps", "componentProps"],
        ["props", "pageProps", "componentProps", "cat1"],
        ["props", "pageProps", "componentProps", "hdpData"],
        ["props", "pageProps", "componentProps", "property"],
    ]
    
    if "__NEXT_DATA__" in data:
        next_data = data["__NEXT_DATA__"]
        for path in property_paths:
            result = extract_nested(next_data, path)
            if result:
                print(f"\n✅ Found data at: {' > '.join(path)}")
                if isinstance(result, dict):
                    print(f"   Keys: {list(result.keys())[:30]}")
                elif isinstance(result, list):
                    print(f"   List with {len(result)} items")
                    if result and isinstance(result[0], dict):
                        print(f"   First item keys: {list(result[0].keys())[:20]}")
    
    print("\n" + "="*80)
    print("✅ ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    analyze_detail_data()

