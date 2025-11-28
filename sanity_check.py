"""
Sanity check script to test Zillow access and diagnose blocking issues.
Tests both direct API calls and basic connectivity.
"""
import json
from curl_cffi import requests
import pyzill

print("=" * 70)
print("ZILLOW ACCESS SANITY CHECK")
print("=" * 70)

# Test 1: Basic HTTP connectivity to Zillow
print("\n[TEST 1] Testing basic HTTP connectivity to Zillow...")
try:
    response = requests.get("https://www.zillow.com", timeout=10, impersonate="chrome124")
    print(f"  ✅ Status Code: {response.status_code}")
    if response.status_code == 200:
        print("  ✅ Can reach Zillow homepage")
    elif response.status_code == 403:
        print("  🚫 BLOCKED: Got 403 Forbidden")
        if "x-px-blocked" in response.headers:
            print("  🚫 PerimeterX blocking detected (x-px-blocked header)")
    else:
        print(f"  ⚠️  Unexpected status: {response.status_code}")
except Exception as e:
    print(f"  ❌ Error: {type(e).__name__}: {e}")

# Test 2: Test pyzill API call with simple parameters
print("\n[TEST 2] Testing pyzill.sold() API call...")
try:
    # Use a small, simple area for testing
    results = pyzill.sold(
        pagination=1,
        search_value="San Francisco, CA",
        min_beds=None,
        max_beds=None,
        min_bathrooms=None,
        max_bathrooms=None,
        min_price=None,
        max_price=None,
        ne_lat=37.8,
        ne_long=-122.4,
        sw_lat=37.7,
        sw_long=-122.5,
        zoom_value=15
    )
    
    if results and isinstance(results, dict):
        house_count = len(results.get("mapResults", []))
        list_count = len(results.get("listResults", []))
        print(f"  ✅ SUCCESS: API call worked!")
        print(f"     - mapResults: {house_count} properties")
        print(f"     - listResults: {list_count} properties")
        if house_count > 0:
            print("  ✅ Got actual data - not blocked!")
        else:
            print("  ⚠️  Got empty results (might be empty area or still blocked)")
    else:
        print(f"  ⚠️  Got unexpected result type: {type(results)}")
        
except json.JSONDecodeError as e:
    print(f"  🚫 JSON ERROR: {e}")
    print("     This means Zillow returned HTML (CAPTCHA/block page) instead of JSON")
    print("     → You are being blocked by PerimeterX")
except Exception as e:
    print(f"  ❌ ERROR: {type(e).__name__}: {e}")

# Test 3: Test the actual API endpoint directly
print("\n[TEST 3] Testing Zillow API endpoint directly...")
try:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en",
        "Content-Type": "application/json",
        "origin": "https://www.zillow.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    
    inputData = {
        "searchQueryState": {
            "isMapVisible": True,
            "isListVisible": True,
            "mapBounds": {
                "north": 37.8,
                "east": -122.4,
                "south": 37.7,
                "west": -122.5,
            },
            "filterState": {
                "sortSelection": {"value": "globalrelevanceex"},
                "isRecentlySold": {"value": True},
                "isAllHomes": {"value": True},
            },
            "mapZoom": 15,
            "pagination": {"currentPage": 1},
            "usersSearchTerm": "San Francisco, CA",
        },
        "wants": {
            "cat1": ["listResults", "mapResults"],
            "cat2": ["total"],
        },
        "requestId": 10,
        "isDebugRequest": False,
    }
    
    response = requests.put(
        url="https://www.zillow.com/async-create-search-page-state",
        json=inputData,
        headers=headers,
        impersonate="chrome124",
        timeout=30
    )
    
    print(f"  Status Code: {response.status_code}")
    
    if response.status_code == 200:
        try:
            data = response.json()
            print("  ✅ Got valid JSON response!")
            if "cat1" in data:
                print("  ✅ API endpoint is working")
            else:
                print("  ⚠️  Response structure unexpected")
        except json.JSONDecodeError:
            print("  🚫 Got HTML instead of JSON (blocked)")
            if "px-captcha" in response.text.lower() or "perimeterx" in response.text.lower():
                print("  🚫 PerimeterX CAPTCHA detected")
    elif response.status_code == 403:
        print("  🚫 BLOCKED: 403 Forbidden")
        if "x-px-blocked" in response.headers:
            print("  🚫 PerimeterX blocking confirmed")
    else:
        print(f"  ⚠️  Unexpected status: {response.status_code}")
        
except Exception as e:
    print(f"  ❌ ERROR: {type(e).__name__}: {e}")

# Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print("\nIf you see 🚫 blocking messages:")
print("  → Zillow's PerimeterX is blocking your requests")
print("  → Solutions:")
print("     1. Use browser automation (nodriver/selenium)")
print("     2. Use residential proxies")
print("     3. Wait and try again later")
print("\nIf you see ✅ success messages:")
print("  → Your access is working!")
print("  → You can proceed with scraping")
print("=" * 70)

