"""
Large-scale scraper for Zillow property detail pages.
Iterates over all houses in nodriver_results tile files and scrapes their detail pages.
Saves parsed results to nodriver_houses folder, tracking visited houses.
"""
import json
import os
import random
import time
import asyncio
from pathlib import Path
from dotenv import load_dotenv
# Lazy imports to avoid requiring nodriver at import time
# import nodriver_detail
# import nodriver_parser

load_dotenv()

# Configuration
RESULTS_FOLDER = "nodriver_results"
HOUSES_FOLDER = "nodriver_houses"
VISITED_HOUSES_FILE = os.path.join(HOUSES_FOLDER, "visited_houses.json")

# Performance tuning: switch between high/low throughput modes
ISOvernight = False  # Set to True when sleeping (slower/safer), False when awake (faster/monitored)

# Low throughput settings (for overnight scraping - slower and safer when you can't monitor)
if ISOvernight:
    INITIAL_PAGE_LOAD_WAIT = 3.0
    BROWSER_INIT_WAIT = 3.0
    GAUSSIAN_SLEEP_MEAN = 4.0
    GAUSSIAN_SLEEP_STD = 1.5
    GAUSSIAN_SLEEP_MIN = 2.0
    GAUSSIAN_SLEEP_MAX = 10.0
else:
    # High throughput settings (when awake and monitoring - faster)
    INITIAL_PAGE_LOAD_WAIT = 2.0
    BROWSER_INIT_WAIT = 2.0
    GAUSSIAN_SLEEP_MEAN = 1.5
    GAUSSIAN_SLEEP_STD = 0.5
    GAUSSIAN_SLEEP_MIN = 0.5
    GAUSSIAN_SLEEP_MAX = 3.0


def ensure_houses_dir():
    """Create the houses folder if it doesn't exist."""
    if not os.path.exists(HOUSES_FOLDER):
        os.makedirs(HOUSES_FOLDER, exist_ok=True)
    return HOUSES_FOLDER


def load_visited_houses():
    """Load the set of visited house zpid/detailUrls from file and sync with existing files."""
    ensure_houses_dir()
    visited_zpids = set()
    visited_urls = set()
    
    # Load from JSON file if it exists
    if os.path.exists(VISITED_HOUSES_FILE):
        try:
            with open(VISITED_HOUSES_FILE, "r") as f:
                data = json.load(f)
                visited_zpids = set(data.get("visited_zpids", []))
                visited_urls = set(data.get("visited_urls", []))
        except Exception as e:
            print(f"Warning: Could not load visited houses: {e}")
    
    # Sync with existing files - scan all files and add any missing zpids
    existing_files = list(Path(HOUSES_FOLDER).glob("*.json"))
    for file in existing_files:
        # Skip visited_houses.json itself
        if file.name == "visited_houses.json":
            continue
        
        # Extract zpid from filename (format: {zpid}.json or nullzpid_N.json)
        if file.stem.startswith("nullzpid_"):
            # Skip nullzpid files - they don't have zpids to track
            continue
        else:
            # Try to parse as zpid
            try:
                zpid = file.stem
                # Validate it's numeric (zpids are numeric strings)
                if zpid.isdigit():
                    visited_zpids.add(zpid)
            except:
                pass
    
    # Save synced data back to file
    if visited_zpids or visited_urls:
        # Count unique zpids (not sum) since all URLs have a zpid
        # zpids are the unique identifier for houses
        data = {
            "visited_zpids": sorted(list(visited_zpids)),
            "visited_urls": sorted(list(visited_urls)),
            "total_visited": len(visited_zpids)  # Count unique zpids to avoid double counting
        }
        try:
            with open(VISITED_HOUSES_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save synced visited houses: {e}")
    
    return visited_zpids, visited_urls


def save_visited_house(zpid, detail_url, visited_zpids=None, visited_urls=None):
    """Add a house zpid and URL to the visited houses file.
    
    Args:
        zpid: The zpid to mark as visited (can be None)
        detail_url: The detail URL to mark as visited
        visited_zpids: Optional set to update (avoids reloading)
        visited_urls: Optional set to update (avoids reloading)
    """
    ensure_houses_dir()
    
    # Load if not provided (for efficiency, caller can pass sets to avoid reloading)
    if visited_zpids is None or visited_urls is None:
        loaded_zpids, loaded_urls = load_visited_houses()
        visited_zpids = visited_zpids or loaded_zpids
        visited_urls = visited_urls or loaded_urls
    
    if zpid:
        visited_zpids.add(str(zpid))
    if detail_url:
        visited_urls.add(detail_url)
    
    # Count unique zpids (not sum) since all URLs have a zpid
    # zpids are the unique identifier for houses
    data = {
        "visited_zpids": sorted(list(visited_zpids)),
        "visited_urls": sorted(list(visited_urls)),
        "total_visited": len(visited_zpids)  # Count unique zpids to avoid double counting
    }
    
    try:
        with open(VISITED_HOUSES_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save visited house: {e}")


def extract_all_detail_urls():
    """Load all detail URLs from nodriver_results/all_house_urls.json (source of truth)."""
    all_house_urls_file = os.path.join(RESULTS_FOLDER, "all_house_urls.json")
    
    if not os.path.exists(all_house_urls_file):
        print(f"❌ Source file not found: {all_house_urls_file}")
        return []
    
    try:
        with open(all_house_urls_file, "r") as f:
            all_houses = json.load(f)
        
        # Validate structure
        if not isinstance(all_houses, list):
            print(f"❌ Invalid format: expected list, got {type(all_houses)}")
            return []
        
        # Ensure all entries have required fields
        valid_houses = []
        for house in all_houses:
            if not isinstance(house, dict):
                continue
            
            zpid = house.get("zpid")
            detail_url = house.get("detailUrl")
            
            if detail_url:
                # Ensure zpid is string
                if zpid:
                    zpid = str(zpid)
                
                valid_houses.append({
                    "zpid": zpid,
                    "detailUrl": detail_url
                })
        
        print(f"✅ Loaded {len(valid_houses)} houses from {all_house_urls_file}")
        return valid_houses
        
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON from {all_house_urls_file}: {e}")
        return []
    except Exception as e:
        print(f"❌ Error loading {all_house_urls_file}: {e}")
        return []


def get_next_nullzpid_counter():
    """Get the next available nullzpid counter by checking existing files."""
    ensure_houses_dir()
    existing_files = list(Path(HOUSES_FOLDER).glob("nullzpid_*.json"))
    
    if not existing_files:
        return 1
    
    # Extract numbers from existing filenames
    counters = []
    for file in existing_files:
        try:
            # Extract number from "nullzpid_N.json"
            num_str = file.stem.split("_")[1]
            counters.append(int(num_str))
        except (ValueError, IndexError):
            continue
    
    if not counters:
        return 1
    
    return max(counters) + 1


def save_house_data(parsed_data, zpid):
    """Save parsed house data to nodriver_houses folder."""
    ensure_houses_dir()
    
    # Check if zpid exists in parsed data
    if not zpid:
        zpid = parsed_data.get("metadata", {}).get("zpid") or parsed_data.get("basic_info", {}).get("zpid")
    
    # Convert to string if it exists
    if zpid:
        zpid = str(zpid)
    
    # Generate filename
    if zpid:
        filename = f"{zpid}.json"
    else:
        # Find next available nullzpid filename
        null_counter = get_next_nullzpid_counter()
        filename = f"nullzpid_{null_counter}.json"
        print(f"  ⚠️  No ZPID found, using filename: {filename}")
    
    filepath = os.path.join(HOUSES_FOLDER, filename)
    
    # Check if file already exists (shouldn't happen, but safety check)
    if os.path.exists(filepath):
        print(f"  ⚠️  File already exists: {filename}, skipping save")
        return None
    
    try:
        with open(filepath, "w") as f:
            json.dump(parsed_data, f, indent=2)
        return filepath
    except Exception as e:
        print(f"  ⚠️  Error saving house data: {e}")
        import traceback
        traceback.print_exc()
        return None


async def scrape_all_houses():
    """Scrape all houses from all_house_urls.json (source of truth)."""
    # Import modules once at the start (lazy import for efficiency)
    import nodriver_detail
    import nodriver_parser
    
    print("="*70)
    print("Large-Scale Zillow Detail Page Scraper")
    print("="*70 + "\n")
    
    # Extract all detail URLs
    all_houses = extract_all_detail_urls()
    
    if not all_houses:
        print("❌ No houses found in all_house_urls.json!")
        return
    
    # Load visited houses
    visited_zpids, visited_urls = load_visited_houses()
    print(f"Already visited: {len(visited_zpids)} zpids, {len(visited_urls)} URLs\n")
    
    # Filter out already visited houses
    # visited_zpids and visited_urls are already synced with existing files
    remaining_houses = []
    for house in all_houses:
        zpid = house.get("zpid")
        if zpid:
            zpid = str(zpid)
        else:
            zpid = None
        url = house.get("detailUrl")
        
        if not url:
            print(f"⚠️  Skipping house with no detailUrl (zpid: {zpid or 'N/A'})")
            continue
        
        # Check if already visited (by zpid or URL)
        # This is the single source of truth after sync
        if zpid and zpid in visited_zpids:
            continue
        if url in visited_urls:
            continue
        
        remaining_houses.append(house)
    
    print(f"Total houses: {len(all_houses)}")
    print(f"Already visited: {len(all_houses) - len(remaining_houses)}")
    print(f"Remaining to process: {len(remaining_houses)}\n")
    
    if len(remaining_houses) == 0:
        print("✅ All houses have been scraped!")
        return
    
    # Randomize order
    random.shuffle(remaining_houses)
    print(f"🔀 Randomized order: Processing {len(remaining_houses)} houses\n")
    
    # Performance settings summary
    mode = "LOW THROUGHPUT (overnight/sleeping)" if ISOvernight else "HIGH THROUGHPUT (awake/monitoring)"
    print(f"⚡ Performance Mode: {mode}")
    print(f"   Initial page load wait: {INITIAL_PAGE_LOAD_WAIT}s")
    print(f"   Sleep between requests: {GAUSSIAN_SLEEP_MEAN}s (mean, range: {GAUSSIAN_SLEEP_MIN}-{GAUSSIAN_SLEEP_MAX}s)")
    print()
    
    # Initialize browser
    print("Initializing browser...")
    browser = await nodriver_detail.get_shared_browser()
    print("✅ Browser ready\n")
    
    # Statistics
    houses_processed = 0
    houses_saved = 0
    houses_failed = 0
    houses_skipped = 0
    start_time = time.time()
    
    try:
        for i, house in enumerate(remaining_houses, 1):
            zpid = house.get("zpid")
            detail_url = house.get("detailUrl")
            
            # Safety check (shouldn't happen after filtering, but just in case)
            if not detail_url:
                print(f"  ⚠️  Skipping house with no detailUrl")
                houses_skipped += 1
                continue
            
            # Progress indicator with time estimate
            elapsed_so_far = time.time() - start_time
            if houses_processed > 0:
                avg_time = elapsed_so_far / houses_processed
                remaining_count = len(remaining_houses) - (i - 1)
                est_remaining = avg_time * remaining_count
                print(f"\n[{i}/{len(remaining_houses)}] Processing house... (Elapsed: {elapsed_so_far/60:.1f}m, Est remaining: {est_remaining/60:.1f}m)")
            else:
                print(f"\n[{i}/{len(remaining_houses)}] Processing house...")
            print(f"  ZPID: {zpid or 'NULL'}")
            print(f"  URL: {detail_url}")
            
            try:
                # Scrape detail page (raw data, no parsing)
                # nodriver_detail already imported at top of function
                print(f"  ⏳ Scraping page...")
                scrape_start = time.time()
                raw_data = await nodriver_detail.scrape_detail_page(detail_url, parse_data=False)
                scrape_time = time.time() - scrape_start
                print(f"  ⏱️  Scraping took {scrape_time:.1f}s")
                
                if "error" in raw_data:
                    print(f"  ❌ Error scraping: {raw_data['error']}")
                    houses_failed += 1
                    continue
                
                # Check for required data (next_data is critical, scores_html is optional)
                if "next_data" not in raw_data:
                    print(f"  ❌ Missing next_data in raw_data")
                    houses_failed += 1
                    continue
                
                # scores_html is optional - if missing, use empty list
                if "scores_html" not in raw_data:
                    print(f"  ⚠️  Warning: scores_html missing, using empty list")
                    raw_data["scores_html"] = []
                
                # Parse the raw data
                # nodriver_parser already imported at top of function
                print(f"  ⏳ Parsing data...")
                parse_start = time.time()
                parsed_data = nodriver_parser.parse_from_next_data(
                    raw_data["next_data"],
                    scores_html=raw_data.get("scores_html", []),
                    url=raw_data.get("url"),
                    scraped_url=raw_data.get("scraped_url")
                )
                parse_time = time.time() - parse_start
                print(f"  ⏱️  Parsing took {parse_time:.1f}s")
                
                if "error" in parsed_data:
                    print(f"  ❌ Error parsing: {parsed_data['error']}")
                    houses_failed += 1
                    continue
                
                # Extract zpid from parsed data if not available
                parsed_zpid = parsed_data.get("metadata", {}).get("zpid") or parsed_data.get("basic_info", {}).get("zpid")
                if not zpid and parsed_zpid:
                    zpid = str(parsed_zpid)
                elif zpid:
                    zpid = str(zpid)
                
                # Save house data
                saved_path = save_house_data(parsed_data, zpid)
                
                if saved_path:
                    print(f"  ✅ Saved to: {os.path.basename(saved_path)}")
                    houses_saved += 1
                    
                    # Mark as visited (pass None if zpid is still missing)
                    save_visited_house(zpid if zpid else None, detail_url)
                    houses_processed += 1
                else:
                    print(f"  ⚠️  Failed to save house data")
                    houses_failed += 1
                
                # Optional: Sleep between requests to avoid rate limiting
                # Comment out if you want maximum speed (but higher risk of blocking)
                # sleep_time = random.gauss(GAUSSIAN_SLEEP_MEAN, GAUSSIAN_SLEEP_STD)
                # sleep_time = max(GAUSSIAN_SLEEP_MIN, min(sleep_time, GAUSSIAN_SLEEP_MAX))
                # print(f"  💤 Sleeping {sleep_time:.1f}s before next house...")
                # await asyncio.sleep(sleep_time)
                
            except KeyboardInterrupt:
                print(f"\n  ⚠️  Interrupted while processing house")
                raise
            except Exception as e:
                print(f"  ❌ Error processing house: {e}")
                import traceback
                traceback.print_exc()
                houses_failed += 1
            
            print()  # Blank line between houses
        
    finally:
        # Close browser
        # nodriver_detail already imported at top of function
        print("\nClosing browser...")
        await nodriver_detail.close_shared_browser()
        
        # Final summary
        elapsed = time.time() - start_time
        print("\n" + "="*70)
        print("FINAL SUMMARY")
        print("="*70)
        print(f"Total houses processed: {houses_processed}")
        print(f"Houses saved: {houses_saved}")
        print(f"Houses failed: {houses_failed}")
        print(f"Total time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        if houses_processed > 0:
            print(f"Average time per house: {elapsed/houses_processed:.1f}s")
        print("="*70)


def main():
    """Main entry point."""
    import nodriver as uc
    loop = uc.loop()
    loop.run_until_complete(scrape_all_houses())


if __name__ == "__main__":
    main()

