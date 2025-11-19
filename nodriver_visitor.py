"""
nodriver-based visitor for Zillow links.
Loads zillow_links.json and visits each URL directly using nodriver browser automation.
Supports pagination to visit all pages for each link.
Scales up to scrape all tiles with randomized order and immediate saving.
"""
import nodriver as uc
import json
import os
import urllib.parse
import re
import random
import copy
from dotenv import load_dotenv
import sleep_utils
from pyzill_scraper import dedupe_results

load_dotenv()
BROWSER_PATH = os.getenv("BROWSER_PATH", None)

# Results folder for saving tile data
RESULTS_FOLDER = "nodriver_results"
VISITED_TILES_FILE = os.path.join(RESULTS_FOLDER, "visited_tiles.json")

# Performance tuning: switch between high/low throughput modes
ISOvernight = False  # Set to True when sleeping (slower/safer), False when awake (faster/monitored)

# Low throughput settings (for overnight scraping - slower and safer when you can't monitor)
if ISOvernight:
    INITIAL_PAGE_LOAD_WAIT = 3.0
    TIME_ON_PAGE = 10.0
    BROWSER_INIT_WAIT = 3.0
    API_RESULT_WAIT = 2.0
    GAUSSIAN_SLEEP_MEAN = 4.0
    GAUSSIAN_SLEEP_STD = 1.5
    GAUSSIAN_SLEEP_MIN = 2.0
    GAUSSIAN_SLEEP_MAX = 10.0
else:
    # High throughput settings (when awake and monitoring - faster)
    INITIAL_PAGE_LOAD_WAIT = 2.0
    TIME_ON_PAGE = 3.0
    BROWSER_INIT_WAIT = 2.0
    API_RESULT_WAIT = 1.5
    GAUSSIAN_SLEEP_MEAN = 1.5
    GAUSSIAN_SLEEP_STD = 0.5
    GAUSSIAN_SLEEP_MIN = 0.5
    GAUSSIAN_SLEEP_MAX = 3.0

def ensure_results_dir():
    """Create the results folder if it doesn't exist."""
    if not os.path.exists(RESULTS_FOLDER):
        os.makedirs(RESULTS_FOLDER, exist_ok=True)
    return RESULTS_FOLDER

def load_visited_tiles():
    """Load the set of visited tile indexes from file."""
    ensure_results_dir()
    if os.path.exists(VISITED_TILES_FILE):
        try:
            with open(VISITED_TILES_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("visited_indexes", []))
        except Exception as e:
            print(f"Warning: Could not load visited tiles: {e}")
            return set()
    return set()

def save_visited_tile(index):
    """Add a tile index to the visited tiles file."""
    ensure_results_dir()
    visited = load_visited_tiles()
    visited.add(index)
    
    data = {
        "visited_indexes": sorted(list(visited)),
        "total_visited": len(visited)
    }
    
    try:
        with open(VISITED_TILES_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save visited tile: {e}")

def generate_tile_filename_by_coords(ne_lat, sw_long):
    """Generate filename from coordinates (same as pyzill_files)."""
    # Match pyzill_files format exactly
    lat_parts = str(ne_lat).split('.')
    lat_str = f"{lat_parts[0]}_{lat_parts[1]}"
    long_str = str(sw_long).replace('.', '_')
    filename = f"tile_{lat_str}_long_{long_str}.json"
    return filename

def save_tile_results(merged_results, ne_lat, sw_long, indent=2):
    """Save merged results for a tile to a coordinate-based filename."""
    ensure_results_dir()
    filename = generate_tile_filename_by_coords(ne_lat, sw_long)
    fullpath = os.path.join(RESULTS_FOLDER, filename)
    
    try:
        with open(fullpath, "w") as f:
            json.dump(merged_results, f, indent=indent)
        return fullpath
    except Exception as e:
        print(f"Error saving tile results: {e}")
        return None

# Global browser instance to reuse across calls
_shared_browser = None
_browser_initialized = False

async def get_shared_browser():
    """Get or create a shared browser instance."""
    global _shared_browser, _browser_initialized
    
    if _shared_browser is None or not _browser_initialized:
        print("Creating new browser instance...")
        browser_args = []
        try:
            if BROWSER_PATH:
                print(f"Starting browser with path: {BROWSER_PATH}")
                _shared_browser = await uc.start(
                    headless=False,  # Keep visible for debugging, set True for production
                    browser_executable_path=BROWSER_PATH,
                    browser_args=browser_args
                )
            else:
                print("Starting browser without custom path...")
                _shared_browser = await uc.start(
                    headless=False,
                    browser_args=browser_args
                )
            print("Browser started successfully")
            
            tab = _shared_browser.tabs[0]
            
            # First, navigate to Zillow homepage to establish session
            print("Establishing session with Zillow...")
            await tab.get("https://www.zillow.com")
            print("Navigated to Zillow, waiting for page load...")
            await tab.wait(BROWSER_INIT_WAIT)  # Wait for page to load
            
            # Check for CAPTCHA
            print("Checking for CAPTCHA...")
            page_content = await tab.get_content()
            if any(indicator in page_content.lower() for indicator in ["press and hold", "px-captcha", "access denied"]):
                if ISOvernight:
                    print("⚠️  CAPTCHA detected on homepage (overnight mode - will skip and retry later)")
                    print("   Consider solving CAPTCHA manually or restarting when awake")
                    raise Exception("CAPTCHA detected during overnight run - stopping to avoid hanging")
                else:
                    print("⚠️  CAPTCHA detected on homepage. Please solve it manually...")
                    # Wait for CAPTCHA to be solved (only in non-overnight mode)
                    captcha_check_interval = 2.0  # Check every 2 seconds for CAPTCHA resolution
                    max_captcha_wait = 300  # Max 5 minutes wait
                    waited = 0
                    while any(indicator in (await tab.get_content()).lower() for indicator in ["press and hold", "px-captcha", "access denied"]):
                        if waited >= max_captcha_wait:
                            print("⚠️  CAPTCHA wait timeout - stopping")
                            raise Exception("CAPTCHA not solved within timeout")
                        await tab.wait(captcha_check_interval)
                        waited += captcha_check_interval
                    print("✅ CAPTCHA solved, continuing...")
            else:
                print("No CAPTCHA detected")
            
            _browser_initialized = True
            print("Browser initialized successfully")
        except Exception as e:
            print(f"ERROR: Failed to create browser: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            raise
    else:
        print("Reusing existing browser instance")
    
    return _shared_browser

async def close_shared_browser():
    """Close the shared browser instance."""
    global _shared_browser, _browser_initialized
    if _shared_browser is not None:
        try:
            await _shared_browser.stop()
        except:
            pass
        _shared_browser = None
        _browser_initialized = False

def modify_url_pagination(url, page_number):
    """
    Modify the pagination number in a Zillow URL.
    
    Args:
        url: The Zillow URL
        page_number: The page number to set (1-indexed)
    
    Returns:
        Modified URL with new pagination
    """
    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)
    
    # Get the searchQueryState parameter
    search_query_state = query_params.get("searchQueryState", [None])[0]
    
    if not search_query_state:
        # If no searchQueryState, return original URL
        return url
    
    # URL decode and parse the JSON
    decoded_state = urllib.parse.unquote(search_query_state)
    state_dict = json.loads(decoded_state)
    
    # Modify pagination
    if "pagination" not in state_dict:
        state_dict["pagination"] = {}
    state_dict["pagination"]["currentPage"] = page_number
    
    # Re-encode the JSON compactly (no spaces, same as Zillow uses)
    # Use separators to ensure compact JSON
    json_str = json.dumps(state_dict, separators=(',', ':'))
    
    # URL encode the JSON string
    encoded_state = urllib.parse.quote(json_str, safe='')
    query_params["searchQueryState"] = [encoded_state]
    
    # Rebuild URL - use urlencode with doseq=True to handle list values
    new_query = urllib.parse.urlencode(query_params, doseq=True)
    new_url = urllib.parse.urlunparse((
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        parsed_url.params,
        new_query,
        parsed_url.fragment
    ))
    
    return new_url

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


def extract_search_state_from_url(url):
    """
    Extract searchQueryState from a Zillow URL.
    
    Returns:
        dict with searchQueryState, or None if not found
    """
    try:
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        search_query_state = query_params.get("searchQueryState", [None])[0]
        
        if not search_query_state:
            return None
        
        decoded_state = urllib.parse.unquote(search_query_state)
        state_dict = json.loads(decoded_state)
        
        return state_dict
    except Exception as e:
        print(f"  Warning: Could not extract search state from URL: {e}")
        return None

async def extract_search_results_from_page(tab, url):
    """
    Extract search results data by making an API call (same as scraper does).
    Extracts searchQueryState from URL and makes API call to get results.
    
    Args:
        tab: Browser tab
        url: The Zillow URL being visited
    
    Returns:
        dict with listResults and mapResults, or empty dict if not found
    """
    try:
        # Extract searchQueryState from URL
        search_query_state = extract_search_state_from_url(url)
        
        if not search_query_state:
            print(f"  Warning: Could not extract searchQueryState from URL")
            return {
                "listResults": [],
                "mapResults": []
            }
        
        # Build the API request payload (same structure as nodriver_scraper)
        inputData = {
            "searchQueryState": search_query_state,
            "wants": {
                "cat1": ["listResults", "mapResults"],
                "cat2": ["total"],
            },
            "requestId": 10,
            "isDebugRequest": False,
        }
        
        # Make API call through browser using JavaScript fetch
        input_data_str = json.dumps(inputData)
        await tab.evaluate(f"window.__zillow_input_data = {input_data_str}")
        
        # Make the fetch call
        js_code = """
        (async function() {
            try {
                const inputData = window.__zillow_input_data;
                const response = await fetch('https://www.zillow.com/async-create-search-page-state', {
                    method: 'PUT',
                    headers: {
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Origin': 'https://www.zillow.com',
                        'Referer': 'https://www.zillow.com/'
                    },
                    body: JSON.stringify(inputData)
                });
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                const data = await response.json();
                window.__zillow_api_result = data;
                return data;
            } catch (error) {
                console.error('Fetch error:', error);
                window.__zillow_api_result = { error: error.message };
                return { error: error.message };
            }
        })()
        """
        
        # Execute and wait for promise
        await tab.evaluate(js_code, await_promise=True)
        
        # Wait a moment for the result to be set
        await tab.wait(API_RESULT_WAIT)
        
        # Retrieve the result
        result_str = await tab.evaluate("JSON.stringify(window.__zillow_api_result || null)")
        
        # Parse the JSON string
        if result_str and result_str != "null":
            try:
                result = json.loads(result_str)
            except (json.JSONDecodeError, TypeError) as e:
                print(f"  Warning: Failed to parse JSON response: {e}")
                return {
                    "listResults": [],
                    "mapResults": []
                }
        else:
            return {
                "listResults": [],
                "mapResults": []
            }
        
        # Handle result extraction (same logic as nodriver_scraper)
        if isinstance(result, dict) and "error" in result:
            print(f"  Warning: API call failed: {result.get('error')}")
            return {
                "listResults": [],
                "mapResults": []
            }
        
        # Handle list response
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict) and "cat1" in item:
                    return item.get("cat1", {}).get("searchResults", {})
                if isinstance(item, dict):
                    for value in item.values():
                        if isinstance(value, dict) and "cat1" in value:
                            return value.get("cat1", {}).get("searchResults", {})
        
        # Handle dict response
        if isinstance(result, dict) and "cat1" in result:
            return result.get("cat1", {}).get("searchResults", {})
        
        print(f"  Warning: Unexpected response type: {type(result)}")
        return {
            "listResults": [],
            "mapResults": []
        }
        
    except Exception as e:
        print(f"  Warning: Could not extract search results: {e}")
        import traceback
        traceback.print_exc()
        return {
            "listResults": [],
            "mapResults": []
        }

async def visit_zillow_link(link_url, index=None, page_number=1):
    """
    Visit a Zillow link URL and check if it loads successfully.
    
    Args:
        link_url: The Zillow URL to visit
        index: Optional index number for logging
        page_number: Page number to visit (default: 1)
    
    Returns:
        dict with success status and page info
    """
    try:
        browser = await get_shared_browser()
        tab = browser.tabs[0]
        
        index_str = f" [{index}]" if index is not None else ""
        page_str = f" (page {page_number})" if page_number > 1 else ""
        
        # Store original URL - we'll navigate to base URL and use API calls for pagination
        original_url = link_url
        
        # For page 1, navigate to the URL. For other pages, navigate to base URL
        # and we'll use API calls with modified pagination
        if page_number == 1:
            print(f"Visiting link{index_str}{page_str}: {link_url[:100]}...")
            await tab.get(link_url)
        else:
            # For pagination, navigate to base URL (page 1) and modify API call instead
            print(f"Visiting link{index_str}{page_str} (using API call for page {page_number})...")
            await tab.get(original_url)
        
        # Wait for page to load initially
        await tab.wait(INITIAL_PAGE_LOAD_WAIT)
        
        # Spend time on the page after it loads (configurable for throughput)
        if TIME_ON_PAGE > 0:
            print(f"  Waiting {TIME_ON_PAGE} seconds on page{index_str}{page_str}...")
            await tab.wait(TIME_ON_PAGE)
        
        # Get page content to check if it loaded successfully
        page_content = await tab.get_content()
        page_url = tab.url
        
        # Check for various error conditions
        content_lower = page_content.lower()
        has_captcha = any(indicator in content_lower for indicator in ["press and hold", "px-captcha", "access denied"])
        
        # Check for listings first - if listings exist, it's probably not an error page
        has_listings = any(indicator in content_lower for indicator in ["listresults", "mapresults", "property-card", "zestimate"])
        
        # More specific error detection - only flag actual error pages
        # Check for error indicators in specific contexts (titles, headings, error messages)
        has_error = False
        if not has_listings:  # Only check for errors if no listings found
            # Check for actual error page indicators
            error_indicators = [
                "404", 
                "page not found",
                "access denied",
                "blocked",
                "forbidden",
                "server error",
                "something went wrong"
            ]
            
            # Check if error indicators appear in specific contexts
            if any(indicator in content_lower for indicator in error_indicators):
                # Check if it's in a title or heading (more likely to be an actual error)
                title_match = re.search(r'<title[^>]*>(.*?)</title>', content_lower, re.DOTALL)
                if title_match and any(indicator in title_match.group(1) for indicator in error_indicators):
                    has_error = True
                # Check for error in h1 tags
                h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', content_lower, re.DOTALL)
                if h1_match and any(indicator in h1_match.group(1) for indicator in error_indicators):
                    has_error = True
                # Check for 404 specifically anywhere (usually means error)
                if "404" in content_lower:
                    has_error = True
        
        success = not has_captcha and not has_error and has_listings
        
        # Extract search results to get counts (try even if success is False, in case detection was wrong)
        search_results = {}
        pc = 0
        hc = 0
        if success or has_listings:  # Try to extract if we have listings, even if other checks failed
            # For pagination, modify the URL's searchQueryState for the API call
            api_url = modify_url_pagination(original_url, page_number) if page_number > 1 else original_url
            search_results = await extract_search_results_from_page(tab, api_url)
            pc = page_count(search_results)
            hc = house_count(search_results)
            
            # If we successfully extracted results with counts, override success
            if pc > 0 or hc > 0:
                success = True
                has_error = False  # If we got results, it's not an error page
        
        result = {
            "success": success,
            "url": link_url,
            "loaded_url": page_url,
            "page_number": page_number,
            "has_captcha": has_captcha,
            "has_error": has_error,
            "has_listings": has_listings,
            "content_length": len(page_content),
            "page_count": pc,
            "house_count": hc,
            "search_results": search_results
        }
        
        if success:
            print(f"✅ Link{index_str}{page_str} loaded successfully (page_count={pc}, house_count={hc})")
        elif has_captcha:
            print(f"⚠️  Link{index_str}{page_str} has CAPTCHA")
        elif has_error:
            print(f"❌ Link{index_str}{page_str} has error")
        else:
            print(f"⚠️  Link{index_str}{page_str} loaded but no listings detected")
        
        # Add a small delay between visits (configurable for throughput)
        sleep_utils.gaussian_sleep(
            mean=GAUSSIAN_SLEEP_MEAN,
            std_dev=GAUSSIAN_SLEEP_STD,
            min_sleep=GAUSSIAN_SLEEP_MIN,
            max_sleep=GAUSSIAN_SLEEP_MAX
        )
        
        return result
        
    except Exception as e:
        print(f"❌ Error visiting link{index_str if index is not None else ''}: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "url": link_url,
            "page_number": page_number,
            "error": str(e)
        }

async def visit_zillow_link_with_pagination(base_link_url, index=None, max_pages=50):
    """
    Visit a Zillow link and paginate through all available pages.
    Uses house_count vs accumulated page_count to determine when to stop.
    Merges all search results from all pages.
    
    Args:
        base_link_url: The base Zillow URL (page 1)
        index: Optional index number for logging
        max_pages: Maximum number of pages to visit (safety limit)
    
    Returns:
        dict with:
        - all_page_results: list of results for each page visited
        - merged_results: merged search results from all pages
        - total_houses: total number of unique houses collected
    """
    all_page_results = []
    page_number = 1
    accumulated_pc = 0
    hc = 0  # house_count from first page
    search_results_list = []  # Collect all search_results from each page
    
    while page_number <= max_pages:
        result = await visit_zillow_link(base_link_url, index=index, page_number=page_number)
        all_page_results.append(result)
        
        # Stop if there was an error or no listings
        if not result.get("success") or not result.get("has_listings"):
            break
        
        # Get counts from this page
        pc = result.get("page_count", 0)
        current_hc = result.get("house_count", 0)
        
        # On first page, get house_count (total properties on map)
        if page_number == 1:
            hc = current_hc
        
        # Collect search results from this page
        search_results = result.get("search_results", {})
        if search_results:
            search_results_list.append(search_results)
        
        accumulated_pc += pc
        
        print(f"  Page {page_number}: page_count={pc}, house_count={current_hc}, accumulated={accumulated_pc}")
        
        # Break conditions:
        # 1. We've accumulated enough results to match the house count
        # 2. No more results (hc == 0 and pc == 0)
        # 3. No new results on this page (pc == 0 but we've already started)
        if accumulated_pc >= hc or (hc == 0 and pc == 0) or (page_number > 1 and pc == 0):
            if accumulated_pc >= hc:
                print(f"  Finished pagination: accumulated_pc ({accumulated_pc}) >= house_count ({hc})")
            elif pc == 0:
                print(f"  Finished pagination: no more results (page_count=0)")
            break
        
        page_number += 1
    
    if page_number > max_pages:
        print(f"  ⚠️  Reached max pages limit ({max_pages}) for link [{index}]")
    
    # Merge all search results from all pages
    merged_results = {}
    if search_results_list:
        for i, search_results in enumerate(search_results_list):
            if not merged_results:
                # First page: deep copy everything
                merged_results = copy.deepcopy(search_results)
            else:
                # Subsequent pages: extend listResults (mapResults usually same across pages)
                new_list_results = copy.deepcopy(search_results.get("listResults", []))
                merged_results["listResults"].extend(new_list_results)
                # Note: mapResults typically contains all results, so we don't extend it
        
        # Deduplicate the merged results
        merged_results = dedupe_results(merged_results)
    
    # Count total unique houses (from mapResults, which contains all properties)
    total_houses = house_count(merged_results) if merged_results else 0
    
    print(f"  📊 Collected {total_houses} unique houses (target was {hc})")
    
    return {
        "all_page_results": all_page_results,
        "merged_results": merged_results,
        "total_houses": total_houses,
        "house_count": hc,
        "accumulated_page_count": accumulated_pc
    }

async def visit_all_links(zillow_links, indexes, paginate=True, save_immediately=True):
    """
    Async function to visit all specified links, optionally paginating through all pages.
    Saves results immediately after each tile is processed.
    
    Args:
        zillow_links: List of link data from zillow_links.json
        indexes: List of indexes to visit (should be randomized)
        paginate: If True, paginate through all pages for each link
        save_immediately: If True, save results immediately after each tile
    """
    # Initialize browser
    print("Initializing browser...")
    await get_shared_browser()
    print("Browser ready!\n")
    
    # Load visited tiles to skip already processed ones
    visited_tiles = load_visited_tiles()
    print(f"Found {len(visited_tiles)} already visited tiles\n")
    
    # Visit each link
    successful = 0
    failed = 0
    total_pages_visited = 0
    total_houses_collected = 0
    skipped = 0
    tiles_processed = 0
    start_time = None
    import time
    start_time = time.time()
    
    try:
        for idx, i in enumerate(indexes, 1):
            # Skip if already visited
            if i in visited_tiles:
                print(f"⏭️  Index {i}: Already visited, skipping")
                skipped += 1
                continue
            
            link_data = zillow_links[i]
            link_url = link_data.get("link")
            coordinates = link_data.get("coordinates", {})
            
            if not link_url:
                print(f"⚠️  Index {i}: No link found, skipping")
                continue
            
            print(f"\n{'='*70}")
            print(f"Processing tile {idx}/{len(indexes)} (index {i})")
            print(f"{'='*70}")
            
            # Show progress stats
            elapsed = time.time() - start_time if start_time else 0
            avg_time_per_tile = elapsed / tiles_processed if tiles_processed > 0 else 0
            remaining_tiles = len(indexes) - idx + 1
            estimated_remaining = avg_time_per_tile * remaining_tiles if avg_time_per_tile > 0 else 0
            print(f"📊 Progress: {tiles_processed} tiles completed | {total_houses_collected} houses collected")
            if tiles_processed > 0:
                print(f"⏱️  Elapsed: {elapsed/60:.1f} min | Avg: {avg_time_per_tile:.1f}s/tile | Est. remaining: {estimated_remaining/60:.1f} min")
            
            try:
                if paginate:
                    # Visit all pages for this link
                    pagination_result = await visit_zillow_link_with_pagination(link_url, index=i)
                    merged_results = pagination_result.get("merged_results", {})
                    total_houses = pagination_result.get("total_houses", 0)
                    page_results = pagination_result.get("all_page_results", [])
                    
                    # Count pages visited
                    for page_result in page_results:
                        if page_result.get("success"):
                            successful += 1
                            total_pages_visited += 1
                        else:
                            failed += 1
                    
                    # Save immediately if we have results
                    save_successful = False
                    saved_path = None
                    if save_immediately:
                        if total_houses > 0:
                            ne_lat = coordinates.get("north")
                            sw_long = coordinates.get("west")
                            if ne_lat and sw_long:
                                saved_path = save_tile_results(merged_results, ne_lat, sw_long)
                                if saved_path:
                                    print(f"  💾 Saved {total_houses} houses to {saved_path}")
                                    total_houses_collected += total_houses
                                    save_successful = True
                                else:
                                    print(f"  ⚠️  Failed to save results")
                            else:
                                print(f"  ⚠️  Could not save: missing coordinates")
                        else:
                            # Even if no houses, save empty result to mark tile as processed
                            ne_lat = coordinates.get("north")
                            sw_long = coordinates.get("west")
                            if ne_lat and sw_long:
                                saved_path = save_tile_results(merged_results, ne_lat, sw_long)
                                if saved_path:
                                    print(f"  💾 Saved empty result (0 houses) to {saved_path}")
                                    save_successful = True
                    
                    # Mark as visited only if save was successful (or if we want to skip empty tiles)
                    if save_successful or total_houses == 0:
                        save_visited_tile(i)
                        tiles_processed += 1
                        print(f"  ✅ Tile {i} completed: {total_houses} houses, {len(page_results)} pages")
                        if saved_path:
                            print(f"  📁 Saved to: {os.path.basename(saved_path)}")
                    else:
                        print(f"  ⚠️  Tile {i} NOT marked as visited due to save failure - will retry on next run")
                    
                else:
                    # Visit only the first page
                    result = await visit_zillow_link(link_url, index=i)
                    
                    if result.get("success"):
                        successful += 1
                        total_pages_visited += 1
                        hc = result.get("house_count", 0)
                        total_houses_collected += hc
                    else:
                        failed += 1
                    
                    # Save immediately if we have results
                    save_successful = False
                    saved_path = None
                    if save_immediately:
                        hc = result.get("house_count", 0)
                        if hc > 0:
                            ne_lat = coordinates.get("north")
                            sw_long = coordinates.get("west")
                            if ne_lat and sw_long:
                                search_results = result.get("search_results", {})
                                saved_path = save_tile_results(search_results, ne_lat, sw_long)
                                if saved_path:
                                    print(f"  💾 Saved {hc} houses to {saved_path}")
                                    save_successful = True
                        else:
                            # Even if no houses, save empty result to mark tile as processed
                            ne_lat = coordinates.get("north")
                            sw_long = coordinates.get("west")
                            if ne_lat and sw_long:
                                search_results = result.get("search_results", {})
                                saved_path = save_tile_results(search_results, ne_lat, sw_long)
                                if saved_path:
                                    print(f"  💾 Saved empty result (0 houses) to {saved_path}")
                                    save_successful = True
                    
                    # Mark as visited only if save was successful
                    if save_successful or hc == 0:
                        save_visited_tile(i)
                        tiles_processed += 1
                        print(f"  ✅ Tile {i} completed: {hc} houses")
                        if saved_path:
                            print(f"  📁 Saved to: {os.path.basename(saved_path)}")
                    else:
                        print(f"  ⚠️  Tile {i} NOT marked as visited due to save failure - will retry on next run")
                
            except KeyboardInterrupt:
                print(f"\n  ⚠️  Interrupted while processing tile {i}")
                print(f"  Tile {i} will be retried on next run")
                raise  # Re-raise to allow cleanup
            except Exception as e:
                print(f"  ❌ Error processing tile {i}: {e}")
                import traceback
                traceback.print_exc()
                failed += 1
                # Don't mark as visited - allow retry on next run
                # This way transient errors can be recovered
            
            print()  # Blank line between tiles
        
    finally:
        # Close browser
        print("\nClosing browser...")
        await close_shared_browser()
        print("Browser closed.")
    
    elapsed_total = time.time() - start_time if start_time else 0
    return successful, failed, total_pages_visited, total_houses_collected, skipped, tiles_processed, elapsed_total

def main():
    """Main function to load zillow_links.json and visit each link."""
    print("="*70)
    print("Zillow Link Visitor using NODRIVER - Scaled Up")
    print("="*70 + "\n")
    
    # Load zillow_links.json
    print("Loading zillow_links.json...")
    try:
        with open("zillow_links.json", "r") as f:
            zillow_links = json.load(f)
        print(f"Loaded {len(zillow_links)} links\n")
    except FileNotFoundError:
        print("❌ Error: zillow_links.json not found!")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Error: Failed to parse zillow_links.json: {e}")
        return
    
    # Load visited tiles to determine what's left to process
    visited_tiles = load_visited_tiles()
    all_indexes = list(range(len(zillow_links)))
    remaining_indexes = [i for i in all_indexes if i not in visited_tiles]
    
    print(f"Total tiles: {len(all_indexes)}")
    print(f"Already visited: {len(visited_tiles)}")
    print(f"Remaining to process: {len(remaining_indexes)}\n")
    
    if len(remaining_indexes) == 0:
        print("✅ All tiles have been visited!")
        return
    
    # Randomize the order of remaining tiles to avoid detection
    random.shuffle(remaining_indexes)
    print(f"🔀 Randomized order: Processing {len(remaining_indexes)} tiles in random order\n")
    
    # Performance settings summary
    mode = "LOW THROUGHPUT (overnight/sleeping)" if ISOvernight else "HIGH THROUGHPUT (awake/monitoring)"
    print(f"⚡ Performance Mode: {mode}")
    print(f"   Initial page load wait: {INITIAL_PAGE_LOAD_WAIT}s")
    print(f"   Time on page: {TIME_ON_PAGE}s")
    print(f"   Sleep between requests: {GAUSSIAN_SLEEP_MEAN}s (mean, range: {GAUSSIAN_SLEEP_MIN}-{GAUSSIAN_SLEEP_MAX}s)")
    print(f"   Estimated time per tile: ~{INITIAL_PAGE_LOAD_WAIT + TIME_ON_PAGE + GAUSSIAN_SLEEP_MEAN:.1f}s")
    print()
    
    # Set paginate=True to visit all pages for each link
    paginate = True
    
    # Visit all links using async function
    loop = uc.loop()
    successful, failed, total_pages, total_houses, skipped, tiles_processed, elapsed_total = loop.run_until_complete(
        visit_all_links(zillow_links, remaining_indexes, paginate=paginate, save_immediately=True)
    )
    
    # Print summary
    print("\n" + "="*70)
    print("VISITING SUMMARY")
    print("="*70)
    print(f"Tiles processed: {tiles_processed}")
    print(f"Tiles skipped (already visited): {skipped}")
    print(f"Total pages visited: {total_pages}")
    print(f"Successful pages: {successful}")
    print(f"Failed pages: {failed}")
    print(f"Total houses collected: {total_houses}")
    print(f"Success rate: {successful/(successful+failed)*100:.1f}%" if (successful+failed) > 0 else "N/A")
    print(f"\n⏱️  Total time: {elapsed_total/60:.1f} minutes ({elapsed_total/3600:.2f} hours)")
    if tiles_processed > 0:
        print(f"⚡ Average time per tile: {elapsed_total/tiles_processed:.1f} seconds")
        print(f"📈 Throughput: {tiles_processed/(elapsed_total/3600):.1f} tiles/hour")
    
    # Verify files were created
    ensure_results_dir()
    result_files = [f for f in os.listdir(RESULTS_FOLDER) if f.startswith("tile_") and f.endswith(".json")]
    print(f"\n📁 Verification:")
    print(f"   Result files created: {len(result_files)}")
    print(f"   Results saved to: {RESULTS_FOLDER}/")
    print(f"   Visited tiles tracked in: {VISITED_TILES_FILE}")
    
    if len(result_files) > 0:
        print(f"\n   Sample files:")
        for f in sorted(result_files)[:5]:  # Show first 5 files
            filepath = os.path.join(RESULTS_FOLDER, f)
            size_kb = os.path.getsize(filepath) / 1024
            print(f"      - {f} ({size_kb:.1f} KB)")
        if len(result_files) > 5:
            print(f"      ... and {len(result_files) - 5} more files")
    
    print("="*70)

if __name__ == "__main__":
    main()

