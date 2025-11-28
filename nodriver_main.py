"""
Main script using nodriver scraper to bypass CAPTCHA/blocking.
This replaces pyzill API calls with nodriver browser automation.
"""
print("Remember to use Singapore full mask")
import os
import json
import nodriver_scraper
import zillow_link_generator
import pyzill_files
import sleep_utils
from pyzill_scraper import (
    page_count,
    house_count,
    dedupe_results,
    check_empty,
)

from dotenv import load_dotenv

load_dotenv()

def nodriver_scraper_wrapper(north_lat, east_long, south_lat, west_long, zoom=17, search_term="San Francisco, CA", pagination=1):
    """
    Wrapper that uses nodriver_scraper instead of pyzill.
    Returns the same structure as pyzill_scraper().
    """
    sleep_utils.gaussian_sleep()
    try:
        result = nodriver_scraper.scrape_zillow_sold_sync(
            north_lat, east_long, south_lat, west_long,
            zoom=zoom, search_term=search_term, pagination=pagination
        )
        # Ensure it has the expected structure
        if not isinstance(result, dict):
            return {
                "listResults": [],
                "mapResults": [],
                "cat1": {"searchResults": {"totalResultCount": 0}}
            }
        return result
    except Exception as e:
        print(f"ERROR: Unexpected error when calling nodriver scraper")
        print(f"  Error type: {type(e).__name__}")
        print(f"  Error message: {e}")
        print(f"  Parameters: pagination={pagination}, coords=({north_lat}, {east_long}, {south_lat}, {west_long})")
        return {
            "listResults": [],
            "mapResults": [],
            "cat1": {"searchResults": {"totalResultCount": 0}}
        }

def nodriver_scraper_full(north_lat, east_long, south_lat, west_long, zoom=17, search_term="San Francisco, CA", pagination=1):
    """
    Fetch all Zillow sold listings from all pages for the given tile, merging all dicts.
    Uses nodriver instead of pyzill API.
    Paginates until accumulated page_count >= house_count (all results fetched).
    """
    results_list = []
    accumulated_pc = 0
    max_pages = 50  # Safety limit to prevent infinite loops
    while pagination <= max_pages:
        print(f"Calling with pagination value {pagination}")
        results_sold = nodriver_scraper_wrapper(
            north_lat, east_long, south_lat, west_long,
            zoom=zoom, search_term=search_term, pagination=pagination
        )
        results_list.append(results_sold)
        pc = page_count(results_sold)
        hc = house_count(results_sold)
        accumulated_pc += pc
        print(f"  Page {pagination}: page_count={pc}, house_count={hc}, accumulated={accumulated_pc}")
        
        # Break conditions:
        # 1. We've accumulated enough results to match the house count
        # 2. No more results (hc == 0 and pc == 0)
        if accumulated_pc >= hc or (hc == 0 and pc == 0):
            break
        pagination += 1
    
    if pagination > max_pages:
        print(f"  ⚠️  Reached max pages limit ({max_pages}), stopping pagination")

    merged = {}
    for d in results_list:
        if not merged:
            merged = d.copy()
            merged["listResults"] = d.get("listResults", []).copy()
            # mapResults typically contains all results, use from first page
            merged["mapResults"] = d.get("mapResults", []).copy()
        else:
            merged["listResults"].extend(d.get("listResults", []))
            # Note: mapResults usually contains all properties, so we don't extend it
            # But if needed, we could merge unique mapResults too
    
    final_pc = page_count(merged)
    final_hc = house_count(merged)
    print(f"  Final merged: page_count={final_pc}, house_count={final_hc}")
    return merged

def nodriver_scraper_master(ne_lat, ne_long, sw_lat, sw_long, zoom=17, search_term="San Francisco, CA",
                            pagination=1, full=True, save=True, filename=None, results_folder=None,
                            indent=2, check_empty_flag=True, empty_filename="empty_tiles.json"):
    """Master scraping helper using nodriver - same interface as pyzill_scraper_master."""
    
    # Pre-check: is this tile already recorded as empty?
    folder = pyzill_files.get_results_folder(results_folder)
    empty_path = os.path.join(folder, empty_filename)
    if check_empty_flag and os.path.exists(empty_path):
        try:
            with open(empty_path, "r") as f:
                empty_list = json.load(f) or []
        except Exception:
            empty_list = []
        if any((entry.get("ne_lat") == ne_lat and entry.get("sw_long") == sw_long) for entry in empty_list):
            print(f"Skipping scrape: coords ({ne_lat}, {sw_long}) recorded as empty in {empty_path}")
            return {
                "results": None,
                "path": None,
                "house_count": 0,
                "page_count": 0,
                "empty_recorded": True,
                "skipped": True,
            }

    # Perform scraping using nodriver
    if full:
        results = nodriver_scraper_full(ne_lat, ne_long, sw_lat, sw_long, zoom=zoom, search_term=search_term, pagination=pagination)
    else:
        results = nodriver_scraper_wrapper(ne_lat, ne_long, sw_lat, sw_long, zoom=zoom, search_term=search_term, pagination=pagination)

    # Deduplicate results
    results = dedupe_results(results)

    hc = house_count(results)
    pc = page_count(results)

    # Validate: we should have collected at least as many items as on the map
    if pc < hc:
        raise ValueError(
            f"Data integrity error: page_count ({pc}) < house_count ({hc}) for tile ({ne_lat}, {sw_long}). "
            "This may indicate incomplete scrape results."
        )

    # Post-check: always attempt to record empty tiles
    empty_recorded = False
    try:
        empty_recorded = check_empty(results, ne_lat, sw_long, filename=empty_filename, results_folder=results_folder)
    except Exception:
        empty_recorded = False

    sold_path = None
    # Only save non-empty results
    if save and hc > 0:
        sold_path = pyzill_files.save_results(results, ne_lat, sw_long, filename=filename, results_folder=results_folder, indent=indent)

    return {
        "results": results if hc > 0 else None,
        "path": sold_path,
        "house_count": hc,
        "page_count": pc,
        "empty_recorded": empty_recorded,
        "skipped": False,
    }

# Main execution
print("Generating zillow links")
zillow_link_generator.main()
print("Zillow links generated")

print("Loading zillow links")
with open("zillow_links.json", "r") as f:
    zillow_links = json.load(f)
print(f"Zillow links loaded: {len(zillow_links)}")
print("Total number of tiles: ", len(zillow_links))

indexes = [i for i in range(len(zillow_links))]
indexes = [100, 200]  # Test with specific indexes
print(zillow_links[100]["link"])

print("\n" + "="*70)
print("Using NODRIVER scraper (bypasses CAPTCHA/blocking)")
print("="*70 + "\n")

# Import the browser cleanup function
from nodriver_scraper import close_shared_browser
import nodriver as uc

try:
    for i in indexes:
        print(f"Scraping index {i}")
        link = zillow_links[i]
        coords = link["coordinates"]
        north = coords["north"]
        east = coords["east"]
        south = coords["south"]
        west = coords["west"]
        print("Scraping with nodriver (no CAPTCHA!)...")
        res = nodriver_scraper_master(north, east, south, west, check_empty_flag=False)
        sold_path = res["path"]
        print(f"Number of houses returned: {res['house_count']}")
        if res["skipped"]:
            print(f"Previously empty tile detected, skipping scrape")
        elif res["empty_recorded"]:
            print(f"Empty tile detected and recorded")
        else:
            print(f"Successfully scraped: {res['house_count']} houses found")
        print()  # Blank line between tiles
finally:
    # Close the shared browser at the end
    print("\nClosing browser...")
    uc.loop().run_until_complete(close_shared_browser())
    print("Browser closed.")

print("="*70)
print("Scraping complete!")
print("="*70)

