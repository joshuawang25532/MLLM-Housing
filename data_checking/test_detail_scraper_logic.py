"""
Test script to validate nodriver_detail_scraper logic without requiring nodriver.
Tests file operations, data extraction, and tracking logic.
"""
import json
import os
from pathlib import Path
import tempfile
import shutil

# Add root directory to path to allow importing modules
import sys
root_dir = str(Path(__file__).parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from nodriver_detail_scraper_concurrent import (
    ensure_houses_dir,
    load_visited_houses,
    save_visited_house,
    extract_all_detail_urls,
    get_next_nullzpid_counter,
    save_house_data,
    RESULTS_FOLDER,
    HOUSES_FOLDER,
    VISITED_HOUSES_FILE
)

def test_basic_functions():
    """Test basic file operations."""
    print("Testing basic functions...")
    
    # Test ensure_houses_dir
    ensure_houses_dir()
    assert os.path.exists(HOUSES_FOLDER), "Houses folder should exist"
    print("✅ ensure_houses_dir works")
    
    # Test load_visited_houses (should work even if file doesn't exist)
    zpids, urls = load_visited_houses()
    assert isinstance(zpids, set), "zpids should be a set"
    assert isinstance(urls, set), "urls should be a set"
    print("✅ load_visited_houses works")
    
    # Test save_visited_house
    save_visited_house("12345", "https://test.com/house1")
    zpids, urls = load_visited_houses()
    assert "12345" in zpids, "zpid should be saved"
    assert "https://test.com/house1" in urls, "URL should be saved"
    print("✅ save_visited_house works")
    
    # Test get_next_nullzpid_counter
    counter = get_next_nullzpid_counter()
    assert isinstance(counter, int), "counter should be int"
    assert counter >= 1, "counter should be >= 1"
    print("✅ get_next_nullzpid_counter works")

def test_extract_urls():
    """Test URL extraction from tile files."""
    print("\nTesting URL extraction...")
    
    if not os.path.exists(RESULTS_FOLDER):
        print("⚠️  nodriver_results folder doesn't exist, skipping URL extraction test")
        return
    
    houses = extract_all_detail_urls()
    print(f"✅ Extracted {len(houses)} houses from tile files")
    
    if houses:
        # Check structure
        house = houses[0]
        assert "zpid" in house, "House should have zpid"
        assert "detailUrl" in house, "House should have detailUrl"
        assert "source" in house, "House should have source"
        assert "tile_file" in house, "House should have tile_file"
        print(f"✅ House structure is correct: {house.get('zpid')}, {house.get('detailUrl')[:50]}...")

def test_save_house_data():
    """Test saving house data."""
    print("\nTesting save_house_data...")
    
    # Create test data
    test_data = {
        "basic_info": {"zpid": "99999", "address": "123 Test St"},
        "metadata": {"zpid": "99999"}
    }
    
    # Test with zpid
    path = save_house_data(test_data, "99999")
    assert path is not None, "Should save successfully"
    assert os.path.exists(path), "File should exist"
    assert path.endswith("99999.json"), "Filename should be zpid.json"
    print("✅ save_house_data with zpid works")
    
    # Test without zpid (nullzpid)
    test_data_no_zpid = {
        "basic_info": {"address": "456 Test St"},
        "metadata": {}
    }
    path2 = save_house_data(test_data_no_zpid, None)
    assert path2 is not None, "Should save successfully"
    assert os.path.exists(path2), "File should exist"
    assert "nullzpid_" in path2, "Filename should contain nullzpid"
    print("✅ save_house_data without zpid works")
    
    # Clean up test files
    if os.path.exists(path):
        os.remove(path)
    if os.path.exists(path2):
        os.remove(path2)

def test_sync_logic():
    """Test sync logic between files and visited_houses.json."""
    print("\nTesting sync logic...")
    
    # Create a test file manually
    test_file = os.path.join(HOUSES_FOLDER, "88888.json")
    with open(test_file, "w") as f:
        json.dump({"test": "data"}, f)
    
    # Reload visited houses (should sync)
    zpids, urls = load_visited_houses()
    
    # Check if zpid was synced
    assert "88888" in zpids, "ZPID from file should be synced"
    print("✅ Sync logic works - file zpid was added to visited list")
    
    # Clean up
    if os.path.exists(test_file):
        os.remove(test_file)

def main():
    """Run all tests."""
    print("="*70)
    print("Testing nodriver_detail_scraper Logic")
    print("="*70 + "\n")
    
    try:
        test_basic_functions()
        test_extract_urls()
        test_save_house_data()
        test_sync_logic()
        
        print("\n" + "="*70)
        print("✅ All tests passed!")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

