"""
nodriver-based scraper for Zillow that bypasses CAPTCHA/blocking.
Uses browser automation to make API calls through the browser session.
"""
import nodriver as uc
import json
import os
from dotenv import load_dotenv
import sleep_utils

load_dotenv()
BROWSER_PATH = os.getenv("BROWSER_PATH", None)

# Global browser instance to reuse across calls
_shared_browser = None
_browser_initialized = False

async def get_shared_browser():
    """Get or create a shared browser instance."""
    global _shared_browser, _browser_initialized
    
    if _shared_browser is None or not _browser_initialized:
        browser_args = []
        if BROWSER_PATH:
            _shared_browser = await uc.start(
                headless=False,  # Keep visible for debugging, set True for production
                browser_executable_path=BROWSER_PATH,
                browser_args=browser_args
            )
        else:
            _shared_browser = await uc.start(
                headless=False,
                browser_args=browser_args
            )
        
        tab = _shared_browser.tabs[0]
        
        # First, navigate to Zillow homepage to establish session
        print("Establishing session with Zillow...")
        await tab.get("https://www.zillow.com")
        await tab.wait(3)  # Wait for page to load
        
        # Check for CAPTCHA
        page_content = await tab.get_content()
        if any(indicator in page_content.lower() for indicator in ["press and hold", "px-captcha", "access denied"]):
            print("⚠️  CAPTCHA detected on homepage. Please solve it manually...")
            # Wait for CAPTCHA to be solved
            while any(indicator in (await tab.get_content()).lower() for indicator in ["press and hold", "px-captcha", "access denied"]):
                await tab.wait(2)
            print("✅ CAPTCHA solved, continuing...")
        
        _browser_initialized = True
    
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

async def scrape_zillow_sold_nodriver(north_lat, east_long, south_lat, west_long, 
                                       zoom=17, search_term="San Francisco, CA", pagination=1):
    """
    Scrape Zillow sold listings using nodriver browser.
    Makes API call through browser session to avoid blocking.
    Uses a shared browser instance to avoid opening/closing repeatedly.
    
    Returns the same data structure as pyzill.sold()
    """
    try:
        # Get shared browser instance
        browser = await get_shared_browser()
        tab = browser.tabs[0]
        
        # Prepare the API request payload (same as pyzill uses)
        filter_state = {
            "sortSelection": {"value": "globalrelevanceex"},
            "isNewConstruction": {"value": False},
            "isForSaleForeclosure": {"value": False},
            "isForSaleByOwner": {"value": False},
            "isForSaleByAgent": {"value": False},
            "isForRent": {"value": False},
            "isComingSoon": {"value": False},
            "isAuction": {"value": False},
            "isAllHomes": {"value": True},
            "isRecentlySold": {"value": True},
        }
        
        inputData = {
            "searchQueryState": {
                "isMapVisible": True,
                "isListVisible": True,
                "mapBounds": {
                    "north": north_lat,
                    "east": east_long,
                    "south": south_lat,
                    "west": west_long,
                },
                "filterState": filter_state,
                "mapZoom": zoom,
                "pagination": {
                    "currentPage": pagination,
                },
            },
            "wants": {
                "cat1": ["listResults", "mapResults"],
                "cat2": ["total"],
            },
            "requestId": 10,
            "isDebugRequest": False,
        }
        
        if search_term:
            inputData["searchQueryState"]["usersSearchTerm"] = search_term
        
        # Make API call through browser using JavaScript fetch
        print(f"Making API call for coordinates: ({north_lat}, {east_long}, {south_lat}, {west_long})")
        
        # Wait a bit for page to be fully ready
        await tab.wait(2)
        
        # Use JavaScript to make the API call with browser's cookies/session
        # Store the input data as a JSON string in the page first
        input_data_str = json.dumps(inputData)
        await tab.evaluate(f"window.__zillow_input_data = {input_data_str}")
        
        # Now make the fetch call
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
        await tab.wait(2)
        
        # Retrieve the result from window object using JSON.stringify to avoid serialization issues
        result_str = await tab.evaluate("JSON.stringify(window.__zillow_api_result || null)")
        
        # Parse the JSON string
        if result_str and result_str != "null" and result_str:
            try:
                result = json.loads(result_str)
            except (json.JSONDecodeError, TypeError) as e:
                print(f"⚠️  Failed to parse JSON response: {e}")
                result = None
        else:
            result = None
        
        # Handle result extraction
        if result is None:
            print(f"⚠️  API call returned null")
            return {
                "listResults": [],
                "mapResults": [],
                "totalResultCount": 0
            }
        
        # Check for errors
        if isinstance(result, dict) and "error" in result:
            print(f"⚠️  API call failed: {result.get('error')}")
            return {
                "listResults": [],
                "mapResults": [],
                "totalResultCount": 0
            }
        
        # Handle list response (might be CDP format)
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict) and "cat1" in item:
                    return item.get("cat1", {}).get("searchResults", {})
                # Check nested dicts
                if isinstance(item, dict):
                    for value in item.values():
                        if isinstance(value, dict) and "cat1" in value:
                            return value.get("cat1", {}).get("searchResults", {})
            print(f"⚠️  Could not find cat1 in list response (length {len(result)})")
            return {
                "listResults": [],
                "mapResults": [],
                "totalResultCount": 0
            }
        
        # Handle dict response
        if isinstance(result, dict) and "cat1" in result:
            return result.get("cat1", {}).get("searchResults", {})
        
        print(f"⚠️  Unexpected response type: {type(result)}")
        return {
            "listResults": [],
            "mapResults": [],
            "totalResultCount": 0
        }
            
    except Exception as e:
        print(f"❌ Error in nodriver scraper: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "listResults": [],
            "mapResults": [],
            "totalResultCount": 0
        }
    # Note: Don't close browser here - it's shared and will be closed at the end

def scrape_zillow_sold_sync(north_lat, east_long, south_lat, west_long, 
                            zoom=17, search_term="San Francisco, CA", pagination=1):
    """
    Synchronous wrapper for the async nodriver scraper.
    This can be used as a drop-in replacement for pyzill.sold()
    """
    return uc.loop().run_until_complete(
        scrape_zillow_sold_nodriver(north_lat, east_long, south_lat, west_long, 
                                    zoom, search_term, pagination)
    )

if __name__ == "__main__":
    # Test the scraper
    print("Testing nodriver scraper...")
    sleep_utils.gaussian_sleep()
    
    result = scrape_zillow_sold_sync(
        north_lat=37.8,
        east_long=-122.4,
        south_lat=37.7,
        west_long=-122.5,
        zoom=15,
        search_term="San Francisco, CA",
        pagination=1
    )
    
    house_count = len(result.get("mapResults", []))
    list_count = len(result.get("listResults", []))
    
    print(f"\n✅ Scraping complete!")
    print(f"   mapResults: {house_count} properties")
    print(f"   listResults: {list_count} properties")
    
    if house_count > 0:
        print(f"\n✅ Success! Got {house_count} properties without CAPTCHA!")

