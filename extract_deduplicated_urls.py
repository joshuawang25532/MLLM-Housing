"""
Extract all detail URLs from nodriver_results tiles, deduplicate by ZPID,
and save as source of truth file.
"""

import json
from pathlib import Path

RESULTS_FOLDER = "nodriver_results"
OUTPUT_FILE = "nodriver_results/all_house_urls.json"

def extract_and_deduplicate_urls():
    """Extract detail URLs from all tiles and deduplicate by ZPID."""
    tile_files = list(Path(RESULTS_FOLDER).glob("tile_*.json"))
    unique_houses = {}  # zpid -> {detailUrl, address, homeType, etc.}
    
    print(f"Processing {len(tile_files)} tile files...")
    
    for tile_file in sorted(tile_files):
        try:
            with open(tile_file, "r", encoding="utf-8") as f:
                tile_data = json.load(f)
            
            # Process all three result types
            for result_type in ["relaxedResults", "listResults", "mapResults"]:
                if result_type not in tile_data:
                    continue
                
                for house in tile_data[result_type]:
                    # Get ZPID from multiple possible locations
                    zpid = None
                    if "zpid" in house:
                        zpid = str(house["zpid"])
                    elif "id" in house:
                        zpid = str(house["id"])
                    elif "hdpData" in house and "homeInfo" in house["hdpData"]:
                        zpid = str(house["hdpData"]["homeInfo"].get("zpid", ""))
                    
                    # Get detailUrl
                    detail_url = house.get("detailUrl")
                    if not detail_url:
                        continue
                    
                    # Convert relative URLs to absolute
                    if detail_url.startswith("/"):
                        detail_url = f"https://www.zillow.com{detail_url}"
                    
                    # Only add if we haven't seen this ZPID before
                    if zpid and zpid not in unique_houses:
                        unique_houses[zpid] = {
                            "zpid": zpid,
                            "detailUrl": detail_url
                        }
        
        except Exception as e:
            print(f"⚠️  Error reading {tile_file.name}: {e}")
            continue
    
    return unique_houses

def main():
    print("=" * 70)
    print("Extracting and Deduplicating House Detail URLs")
    print("=" * 70)
    print()
    
    unique_houses = extract_and_deduplicate_urls()
    
    print(f"Found {len(unique_houses)} unique houses (by ZPID)")
    print()
    
    # Convert to list for JSON output (maintain order)
    houses_list = list(unique_houses.values())
    
    # Save to file
    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(houses_list, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved {len(houses_list)} unique house URLs to: {OUTPUT_FILE}")
    print(f"Total unique houses: {len(houses_list)}")

if __name__ == "__main__":
    main()

