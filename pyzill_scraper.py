# use singapore full mask
import os
from dotenv import load_dotenv
import pyzill
import json
import pyzill_files
import sleep_utils

load_dotenv()

# From tile
north = 37.7235590480963
east = -122.4137702007554
south = 37.71694904488391
west = -122.42598845170323

ne_lat  = north
ne_long = east
sw_lat  = south
sw_long = west

pagination = 1

def pyzill_scraper(north_lat, east_long, south_lat, west_long, zoom=17, search_term="San Francisco, CA", pagination=1):
    """
    Fetch a single page of Zillow sold listings for the given parameters and page.
    Includes a Gaussian-distributed sleep before the request to avoid rate limiting.
    """
    sleep_utils.gaussian_sleep()
    return pyzill.sold(
        pagination,
        search_value=search_term,
        min_beds=None, max_beds=None,
        min_bathrooms=None, max_bathrooms=None,
        min_price=None, max_price=None,
        ne_lat=north_lat, ne_long=east_long, sw_lat=south_lat, sw_long=west_long,
        zoom_value=zoom
    )

def pyzill_scraper_full(north_lat, east_long, south_lat, west_long, zoom=17, search_term="San Francisco, CA", pagination=1):
    """
    Fetch all Zillow sold listings from all pages for the given tile, merging all dicts.
    Paginates until accumulated page_count >= house_count (all results fetched).
    """
    results_list = []
    accumulated_pc = 0
    while True:
        print(f"Calling with pagination value {pagination}")
        results_sold = pyzill_scraper(
            north_lat, east_long, south_lat, west_long,
            zoom=zoom, search_term=search_term, pagination=pagination
        )
        results_list.append(results_sold)
        pc = page_count(results_sold)
        hc = house_count(results_sold)
        accumulated_pc += pc
        print(f"  Page {pagination}: page_count={pc}, house_count={hc}, accumulated={accumulated_pc}")
        if accumulated_pc >= hc:
            # We've accumulated enough results to match the house count
            break
        pagination += 1

    merged = {}
    for d in results_list:
        if not merged:
            merged = d.copy()
            merged["listResults"] = d.get("listResults", []).copy()
        else:
            merged["listResults"].extend(d.get("listResults", []))
    
    final_pc = page_count(merged)
    final_hc = house_count(merged)
    print(f"  Final merged: page_count={final_pc}, house_count={final_hc}")
    return merged



def page_count(results_sold):
    """
    listResults contains the paginated sidebar list.
    """
    return len(results_sold.get("listResults", []))


def house_count(results_sold):
    """
    Count houses from mapResults, which contains all properties visible on the map.
    This matches what the browser displays. listResults only contains the paginated sidebar list.
    """
    return len(results_sold.get("mapResults", []))


def dedupe_results(results):
    """Remove duplicate entries from listResults and mapResults based on zpid (property ID).
    
    Each array is deduped independently (removing duplicates within that array).
    Both arrays can share the same zpids — that's expected and correct.
    
    Returns the deduplicated results dict.
    """
    # Dedupe listResults independently
    seen_list_zpids = set()
    deduped_list = []
    for item in results.get("listResults", []):
        zpid = item.get("zpid")
        if zpid and zpid not in seen_list_zpids:
            seen_list_zpids.add(zpid)
            deduped_list.append(item)
    
    # Dedupe mapResults independently (separate zpid tracking)
    seen_map_zpids = set()
    deduped_map = []
    for item in results.get("mapResults", []):
        zpid = item.get("zpid")
        if zpid and zpid not in seen_map_zpids:
            seen_map_zpids.add(zpid)
            deduped_map.append(item)
    
    results["listResults"] = deduped_list
    results["mapResults"] = deduped_map
    return results

def file_save(results_sold, filename):
    """Deprecated"""
    raise RuntimeError(
        "pyzill_scraper.file_save is deprecated and disabled. "
        "Use pyzill_files.save_results(results, ne_lat, sw_long, filename=..., results_folder=...) instead."
    )


def check_empty(results_sold, ne_lat, sw_long, filename="empty_tiles.json", results_folder=None):
    """If a tile has zero houses, record its coords to a JSON list file.

    - Only stores minimal entries: `{ "ne_lat": ..., "sw_long": ... }`.
    - Avoids duplicates based on coords.
    - `results_folder` falls back to env/default via `pyzill_files`.

    Returns True if a new entry was written, False otherwise.
    """
    try:
        count = house_count(results_sold)
    except Exception:
        # Malformed results => don't treat as empty
        return False

    if count != 0:
        return False

    entry = {"ne_lat": ne_lat, "sw_long": sw_long}

    folder = pyzill_files.ensure_results_dir(results_folder)
    path = os.path.join(folder, filename)

    data = []
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f) or []
        except Exception:
            data = []

    # Check duplicate based on coords
    if any(d.get("ne_lat") == entry["ne_lat"] and d.get("sw_long") == entry["sw_long"] for d in data):
        print(f"Empty tile already recorded in {path}")
        return False

    data.append(entry)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Empty tile recorded to {path}: {entry}")
    return True


def pyzill_scraper_master(ne_lat, ne_long, sw_lat, sw_long, zoom=17, search_term="San Francisco, CA",
                          pagination=1, full=True, save=True, filename=None, results_folder=None,
                          indent=2, check_empty_flag=True, empty_filename="empty_tiles.json"):
    """Master scraping helper with pre/post empty-tile checks.

    Auto checks empty (pre and post), Auto saves
    
    Behavior changes:
    - Before scraping, checks `empty_filename` in the results folder; if the coords exist, scraping is skipped and
      the function returns with `skipped=True`.
    - After scraping, always calls `check_empty` to record empty tiles.
    - If a tile is empty (house_count == 0), it will NOT be saved via `pyzill_files.save_results`.

    Returns a dict with keys: `results` (or None if skipped/empty), `path` (saved path or None),
    `house_count`, `page_count`, `empty_recorded` (True if it was recorded as empty), `skipped`.
    """

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

    # Perform scraping
    if full:
        results = pyzill_scraper_full(ne_lat, ne_long, sw_lat, sw_long, zoom=zoom, search_term=search_term, pagination=pagination)
    else:
        results = pyzill_scraper(ne_lat, ne_long, sw_lat, sw_long, zoom=zoom, search_term=search_term, pagination=pagination)

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


if __name__ == "__main__":
    print("Individual Scraping Test")
    results_sold = pyzill_scraper_full(ne_lat, ne_long, sw_lat, sw_long)

    sold_filename = pyzill_files.save_results(results_sold, ne_lat, sw_long, filename="jsondata_sold.json")

    page_count_result = page_count(results_sold)
    print(f"Number of houses in a random page: {page_count_result}")
    house_count_result = house_count(results_sold)
    print(f"Number of houses returned: {house_count_result}")