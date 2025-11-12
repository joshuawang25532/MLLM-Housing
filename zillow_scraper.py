import json
import urllib.parse
import math
from typing import Dict, List, Tuple
import random

BASE_URL = "https://www.zillow.com/san-francisco-ca/sold/?category=RECENT_SEARCH&searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A37.81609909306968%2C%22south%22%3A37.703729038459144%2C%22east%22%3A-122.34046069506836%2C%22west%22%3A-122.54817096118164%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22usersSearchTerm%22%3A%22San%20Francisco%2C%20CA%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A20330%2C%22regionType%22%3A6%7D%5D%7D"


def extract_search_state(url: str) -> dict:
    """
    Extract the complete searchQueryState from a Zillow URL.
    
    Returns the full searchQueryState dictionary which includes:
    - mapBounds: {north, south, east, west}
    - filterState: various filter options
    - isMapVisible, isListVisible
    - mapZoom
    - usersSearchTerm
    - regionSelection
    - etc.
    """
    # Parse the URL to get query parameters
    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)
    
    # Get the searchQueryState parameter
    search_query_state = query_params.get("searchQueryState", [None])[0]
    
    if not search_query_state:
        raise ValueError("searchQueryState not found in URL")
    
    # URL decode and parse the JSON
    decoded_state = urllib.parse.unquote(search_query_state)
    state_dict = json.loads(decoded_state)
    
    return state_dict

def km_per_deg(lat_deg: float) -> Tuple[float, float]:
    """Return (km per degree longitude, km per degree latitude) at latitude."""
    lat_rad = math.radians(lat_deg)
    return 111.320 * math.cos(lat_rad), 110.574

def compute_subtiles(
    search_bounds: Dict[str, float],
    ref_tile: Dict[str, float],
    safety_scale: float = 1.0
) -> List[Tuple[float, float, float, float]]:
    """
    Given:
      search_bounds: {"north":..., "south":..., "east":..., "west":...}
      ref_tile:      {"north":..., "south":..., "east":..., "west":...}  # a known-good (<500) tile
    Returns:
      List of (west, east, south, north) tuples covering search_bounds.
    Each tile's real-world width/height (km) <= ref_tile's (scaled by safety_scale).
    """
    # Unpack search space
    N = float(search_bounds["north"])
    S = float(search_bounds["south"])
    E = float(search_bounds["east"])
    W = float(search_bounds["west"])
    midlat = 0.5 * (S + N)
    km_lon, km_lat = km_per_deg(midlat)

    # Size of search space (km)
    width_km  = abs(E - W) * km_lon
    height_km = abs(N - S) * km_lat

    # Reference tile dimensions (km) measured at its own mid-latitude
    rN = float(ref_tile["north"])
    rS = float(ref_tile["south"])
    rE = float(ref_tile["east"])
    rW = float(ref_tile["west"])
    r_midlat = 0.5 * (rS + rN)
    r_km_lon, r_km_lat = km_per_deg(r_midlat)
    ref_w_km = abs(rE - rW) * r_km_lon
    ref_h_km = abs(rN - rS) * r_km_lat

    # Optional global shrink
    ref_w_km *= safety_scale
    ref_h_km *= safety_scale
    if ref_w_km <= 0 or ref_h_km <= 0:
        raise ValueError("Reference tile has non-positive size.")

    # Rows/cols so each subdivision <= ref tile in BOTH dimensions (km)
    cols = max(1, math.ceil(width_km  / ref_w_km))
    rows = max(1, math.ceil(height_km / ref_h_km))

    # Convert back to degree steps for uniform grid slicing
    dlon = (E - W) / cols
    dlat = (N - S) / rows

    tiles: List[Tuple[float, float, float, float]] = []
    for i in range(rows):
        for j in range(cols):
            w = W + j * dlon
            e = W + (j + 1) * dlon
            s = S + i * dlat
            n = S + (i + 1) * dlat
            tiles.append((float(min(w, e)), float(max(w, e)),
                          float(min(s, n)), float(max(s, n))))
    return tiles






def build_url(
    base_path: str,
    search_state: dict,
    category: str = "RECENT_SEARCH",
    north: float = None,
    south: float = None,
    east: float = None,
    west: float = None
) -> str:
    """
    Build a Zillow URL from a search state dictionary, with optional bounds override.
    
    Args:
        base_path: The base path (e.g., "/san-francisco-ca/sold/")
        search_state: The searchQueryState dictionary
        category: The category parameter (default: "RECENT_SEARCH")
        north, south, east, west: Optional float values to override mapBounds
    
    Returns:
        A complete Zillow URL
    """
    # If any NSEW is provided, override mapBounds in the search state copy
    if any(val is not None for val in [north, south, east, west]):
        # Don't mutate user provided state
        search_state = dict(search_state)
        bounds = dict(search_state.get("mapBounds") or {})
        if north is not None: bounds["north"] = north
        if south is not None: bounds["south"] = south
        if east  is not None: bounds["east"]  = east
        if west  is not None: bounds["west"]  = west
        search_state["mapBounds"] = bounds

    # Convert the search state to JSON and URL encode it
    state_json = json.dumps(search_state, separators=(',', ':'))
    encoded_state = urllib.parse.quote(state_json)
    
    # Build the full URL
    url = f"https://www.zillow.com{base_path}?category={category}&searchQueryState={encoded_state}"
    
    return url

if __name__ == "__main__":
    search_state = extract_search_state(BASE_URL)
    
    print("=" * 70)
    print("COMPLETE SEARCH STATE")
    print("=" * 70)
    print(json.dumps(search_state, indent=2))
    
    print("\n" + "=" * 70)
    print("OTHER PARAMETERS")
    print("=" * 70)
    print(f"  Map Zoom: {search_state.get('mapZoom')}")
    print(f"  Search Term: {search_state.get('usersSearchTerm')}")
    print(f"  Map Visible: {search_state.get('isMapVisible')}")
    print(f"  List Visible: {search_state.get('isListVisible')}")
    
    # Test rebuilding the URL
    print("\n" + "=" * 70)
    print("URL REBUILD TEST")
    print("=" * 70)
    rebuilt_url = build_url("/san-francisco-ca/sold/", search_state)
    print("Rebuilt URL matches original:", rebuilt_url == BASE_URL)

    search_state["mapZoom"] = 17

    search_space = {
        "north": search_state.get("mapBounds", {}).get("north"),
        "south": search_state.get("mapBounds", {}).get("south"),
        "east": search_state.get("mapBounds", {}).get("east"),
        "west": search_state.get("mapBounds", {}).get("west"),
    }

    # Hopefully smaller than 500 items
    ref_tile = {
        "west":  -122.42879176774504,
        "east":  -122.41580987611296,
        "south":  37.79044676337045,
        "north":  37.797466660899765,
    }

    tiles = compute_subtiles(search_space, ref_tile)
    print(f"Generated {len(tiles)} tiles")

    for tile in tiles:
        print(tile)
    
    sample_links = []
    for i, tile in enumerate(tiles):
        link = build_url("/san-francisco-ca/sold/", search_state, north=tile[3], south=tile[2], east=tile[1], west=tile[0])
        sample_links.append(link)

    random_link = random.choice(sample_links)
    print("\nRandom sample link:")
    print(random_link)

