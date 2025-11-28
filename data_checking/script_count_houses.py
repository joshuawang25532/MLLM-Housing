"""
Quick script to count total houses across all saved tile files.
"""
import os
import json
from pathlib import Path

RESULTS_FOLDER = "nodriver_results"

def house_count(results_sold):
    """Count houses from mapResults (constant per tile - all properties visible on map)."""
    return len(results_sold.get("mapResults", []))

def page_count(results_sold):
    """Count pages from listResults (paginated sidebar list - sum across tiles)."""
    return len(results_sold.get("listResults", []))

def main():
    print("="*70)
    print("Counting Houses and Pages in Saved Tile Files")
    print("="*70 + "\n")
    
    if not os.path.exists(RESULTS_FOLDER):
        print(f"❌ Results folder '{RESULTS_FOLDER}' not found!")
        return
    
    # Find all tile JSON files
    tile_files = list(Path(RESULTS_FOLDER).glob("tile_*.json"))
    
    if not tile_files:
        print(f"❌ No tile files found in '{RESULTS_FOLDER}'")
        return
    
    print(f"Found {len(tile_files)} tile files\n")
    
    total_houses = 0  # Sum of house_count (mapResults) across all tiles
    total_pages = 0   # Sum of page_count (listResults) across all tiles
    tiles_with_houses = 0
    tiles_empty = 0
    house_counts = []
    page_counts = []
    
    for tile_file in sorted(tile_files):
        try:
            with open(tile_file, "r") as f:
                data = json.load(f)
            
            hc = house_count(data)
            pc = page_count(data)
            
            house_counts.append(hc)
            page_counts.append(pc)
            total_houses += hc
            total_pages += pc
            
            if hc > 0:
                tiles_with_houses += 1
            else:
                tiles_empty += 1
                
        except Exception as e:
            print(f"⚠️  Error reading {tile_file.name}: {e}")
    
    # Calculate statistics
    avg_houses = total_houses / len(tile_files) if tile_files else 0
    avg_pages = total_pages / len(tile_files) if tile_files else 0
    max_houses = max(house_counts) if house_counts else 0
    min_houses = min(house_counts) if house_counts else 0
    max_pages = max(page_counts) if page_counts else 0
    min_pages = min(page_counts) if page_counts else 0
    
    # Print results
    print("="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Total tile files: {len(tile_files)}")
    print(f"Tiles with houses: {tiles_with_houses}")
    print(f"Empty tiles: {tiles_empty}")
    print(f"\n🏠 Total house_count (mapResults): {total_houses:,}")
    print(f"📄 Total page_count (listResults): {total_pages:,}")
    print(f"📊 Combined total: {total_houses + total_pages:,}")
    print(f"\n📊 Average house_count per tile: {avg_houses:.1f}")
    print(f"📊 Average page_count per tile: {avg_pages:.1f}")
    print(f"📈 Max house_count in a tile: {max_houses}")
    print(f"📈 Max page_count in a tile: {max_pages}")
    print(f"📉 Min house_count in a tile: {min_houses}")
    print(f"📉 Min page_count in a tile: {min_pages}")
    print("="*70)
    
    # Show top tiles by house count
    if house_counts and max_houses > 0:
        print("\nTop 10 tiles by house_count:")
        tile_data = list(zip(tile_files, house_counts, page_counts))
        tile_data.sort(key=lambda x: x[1], reverse=True)
        for i, (tile_file, hc, pc) in enumerate(tile_data[:10], 1):
            print(f"  {i}. {tile_file.name}: {hc:,} houses, {pc:,} pages")

if __name__ == "__main__":
    main()

