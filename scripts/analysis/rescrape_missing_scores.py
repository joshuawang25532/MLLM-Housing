"""
Rescrape houses with missing scores.
Identifies houses where all scores are None and rescrapes them.
"""
import json
import os
import asyncio
import random
import time
from pathlib import Path
from dotenv import load_dotenv
# Lazy imports to avoid requiring nodriver at import time
# import nodriver_detail
# import nodriver_parser
from utils.data_validator import HouseValidator

load_dotenv()

# Configuration
HOUSES_FOLDER = "data/raw_houses"
VISITED_HOUSES_FILE = os.path.join(HOUSES_FOLDER, "visited_houses.json")

# Performance tuning: switch between high/low throughput modes
ISOvernight = False  # Set to True when sleeping (slower/safer), False when awake (faster/monitored)

# Low throughput settings (for overnight scraping - slower and safer when you can't monitor)
if ISOvernight:
    GAUSSIAN_SLEEP_MEAN = 4.0
    GAUSSIAN_SLEEP_STD = 1.5
    GAUSSIAN_SLEEP_MIN = 2.0
    GAUSSIAN_SLEEP_MAX = 10.0
else:
    # High throughput settings (when awake and monitoring - faster)
    GAUSSIAN_SLEEP_MEAN = 2.5
    GAUSSIAN_SLEEP_STD = 0.8
    GAUSSIAN_SLEEP_MIN = 1.0
    GAUSSIAN_SLEEP_MAX = 5.0


def find_houses_with_missing_scores(issues_json_file=None):
    """
    Find all houses with missing scores (all scores are None).
    
    Args:
        issues_json_file: Optional path to issues.json file from validator.
                         If provided, will filter houses with missing scores from that file.
                         If None, will scan all house files.
    """
    houses_to_rescrape = []
    
    if issues_json_file and Path(issues_json_file).exists():
        # Read from issues.json file
        print(f"Reading houses from {issues_json_file}...")
        try:
            with open(issues_json_file, 'r', encoding='utf-8') as f:
                issues_data = json.load(f)
            
            houses_with_issues = issues_data.get("houses_with_issues", [])
            print(f"Found {len(houses_with_issues)} houses with issues in JSON file")
            
            # Filter for houses with missing scores error
            missing_scores_error_prefix = "All scores are None - __NEXT_DATA__ loaded but scores were not scraped from HTML"
            
            for house in houses_with_issues:
                errors = house.get("errors", [])
                # Check if this house has the missing scores error (checking if any error starts with the prefix)
                has_missing_scores = any(
                    error.startswith(missing_scores_error_prefix) 
                    for error in errors
                )
                
                if has_missing_scores:
                    zpid = house.get("zpid")
                    url = house.get("url")
                    filename = house.get("filename")
                    
                    if zpid and url and filename:
                        filepath = Path(HOUSES_FOLDER) / filename
                        houses_to_rescrape.append({
                            "zpid": str(zpid),
                            "url": url,
                            "filepath": str(filepath)
                        })
            
            print(f"✅ Found {len(houses_to_rescrape)} houses with missing scores from issues.json")
            return houses_to_rescrape
            
        except Exception as e:
            print(f"⚠️  Error reading {issues_json_file}: {e}")
            print("Falling back to scanning all house files...")
    
    # Fallback: scan all house files (original behavior)
    validator = HouseValidator(houses_dir=HOUSES_FOLDER)
    houses_dir = Path(HOUSES_FOLDER)
    
    json_files = list(houses_dir.glob("*.json"))
    print(f"Scanning {len(json_files)} house files for missing scores...")
    
    for filepath in json_files:
        # Skip visited_houses.json
        if filepath.name == "visited_houses.json":
            continue
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if this house has missing scores
            if "scores" in data and "basic_info" in data:
                scores = data.get("scores", {})
                score_fields = ["walkScore", "transitScore", "bikeScore"]
                
                # Check if all scores are None
                all_scores_none = all(
                    scores.get(field) is None 
                    for field in score_fields 
                    if field in scores
                )
                
                if all_scores_none and (len(scores) == 0 or all(v is None for v in scores.values())):
                    # Get ZPID and URL
                    zpid = data.get("basic_info", {}).get("zpid")
                    url = data.get("metadata", {}).get("url") or data.get("metadata", {}).get("scrapedUrl")
                    
                    # If no URL in metadata, construct it from ZPID
                    if not url and zpid:
                        url = f"https://www.zillow.com/homedetails/{zpid}_zpid/"
                    
                    if zpid and url:
                        houses_to_rescrape.append({
                            "zpid": str(zpid),
                            "url": url,
                            "filepath": str(filepath)
                        })
        
        except Exception as e:
            print(f"⚠️  Error reading {filepath.name}: {e}")
            continue
    
    return houses_to_rescrape


def load_visited_houses():
    """Load visited houses to avoid re-scraping."""
    visited_zpids = set()
    
    if os.path.exists(VISITED_HOUSES_FILE):
        try:
            with open(VISITED_HOUSES_FILE, "r") as f:
                data = json.load(f)
                visited_zpids = set(data.get("visited_zpids", []))
        except Exception as e:
            print(f"Warning: Could not load visited houses: {e}")
    
    return visited_zpids


async def rescrape_house(zpid, url, filepath):
    """Rescrape a single house and update its file."""
    # Lazy import
    import utils.detail_scraper as nodriver_detail
    import utils.html_parser as nodriver_parser
    
    print(f"\n{'='*70}")
    print(f"Rescraping ZPID: {zpid}")
    print(f"URL: {url}")
    print(f"File: {Path(filepath).name}")
    print(f"{'='*70}")
    
    try:
        # Scrape the detail page
        scrape_start = time.time()
        raw_data = await nodriver_detail.scrape_detail_page(url, parse_data=False)
        scrape_time = time.time() - scrape_start
        
        print(f"  ⏱️  Scraping took {scrape_time:.1f}s")
        
        if "error" in raw_data:
            print(f"  ❌ Error scraping: {raw_data['error']}")
            
            # Check if this is a CAPTCHA
            if raw_data.get("captcha", False):
                print(f"  🚫 CAPTCHA detected - rotating browser...")
                try:
                    await nodriver_detail.rotate_browser_on_captcha()
                    # Retry once after rotation
                    print(f"  🔄 Retrying house after browser rotation...")
                    raw_data = await nodriver_detail.scrape_detail_page(url, parse_data=False)
                    scrape_time = time.time() - scrape_start
                    print(f"  ⏱️  Retry scraping took {scrape_time:.1f}s")
                    
                    if "error" in raw_data:
                        print(f"  ❌ Retry also failed: {raw_data['error']}")
                        return False
                except Exception as e:
                    print(f"  ❌ Browser rotation failed: {e}")
                    return False
            else:
                return False
        
        # Check for required data
        if "next_data" not in raw_data:
            print(f"  ❌ Missing next_data in raw_data")
            return False
        
        # Parse the data
        print("  → Parsing data...")
        parsed_data = nodriver_parser.parse_from_next_data(
            raw_data["next_data"],
            scores_html=raw_data.get("scores_html", []),
            url=url,
            scraped_url=raw_data.get("scraped_url", url)
        )
        
        if "error" in parsed_data:
            print(f"  ❌ Parsing error: {parsed_data['error']}")
            return False
        
        # Check if scores were successfully extracted
        scores = parsed_data.get("scores", {})
        score_fields = ["walkScore", "transitScore", "bikeScore"]
        has_scores = any(
            scores.get(field) is not None 
            for field in score_fields
        )
        
        if not has_scores:
            print(f"  ⚠️  Warning: Still no scores after rescraping")
            print(f"     Scores: {scores}")
        else:
            print(f"  ✅ Successfully extracted scores:")
            for field in score_fields:
                value = scores.get(field)
                if value is not None:
                    print(f"     {field}: {value}")
        
        # Update the house file
        print(f"  → Updating house file...")
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(parsed_data, f, indent=2)
            print(f"  ✅ House file updated: {Path(filepath).name}")
            return True
        except Exception as e:
            print(f"  ❌ Error updating house file: {e}")
            return False
        
    except Exception as e:
        print(f"  ❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main(issues_json_file=None):
    """Main entry point."""
    # Lazy import
    import nodriver_detail
    
    print("="*70)
    print("RESCRAPE HOUSES WITH MISSING SCORES")
    print("="*70 + "\n")
    
    # Find houses with missing scores
    print("Step 1: Finding houses with missing scores...")
    houses_to_rescrape = find_houses_with_missing_scores(issues_json_file=issues_json_file)
    
    if not houses_to_rescrape:
        print("✅ No houses found with missing scores!")
        return
    
    print(f"✅ Found {len(houses_to_rescrape)} houses with missing scores\n")
    
    # Load visited houses (for reference, but we'll rescrape anyway)
    visited_zpids = load_visited_houses()
    print(f"Already visited: {len(visited_zpids)} zpids\n")
    
    # Randomize order
    random.shuffle(houses_to_rescrape)
    print(f"🔀 Randomized order: Processing {len(houses_to_rescrape)} houses\n")
    
    # Performance settings summary
    mode = "LOW THROUGHPUT (overnight/sleeping)" if ISOvernight else "HIGH THROUGHPUT (awake/monitoring)"
    print(f"⚡ Performance Mode: {mode}")
    print(f"   Sleep between requests: {GAUSSIAN_SLEEP_MEAN}s (mean, range: {GAUSSIAN_SLEEP_MIN}-{GAUSSIAN_SLEEP_MAX}s)")
    print()
    
    # Initialize browser
    print("Initializing browser...")
    browser = await nodriver_detail.get_shared_browser()
    print("✅ Browser ready\n")
    
    # Statistics
    houses_processed = 0
    houses_successful = 0
    houses_failed = 0
    start_time = time.time()
    
    # Randomized browser restart interval (45-55 houses)
    next_restart_at = random.randint(45, 55)
    
    try:
        for i, house in enumerate(houses_to_rescrape, 1):
            zpid = house["zpid"]
            url = house["url"]
            filepath = house["filepath"]
            
            print(f"\n[{i}/{len(houses_to_rescrape)}] Processing house {zpid}...")
            
            # Restart browser at randomized intervals
            if houses_processed > 0 and houses_processed >= next_restart_at:
                print(f"\n🔄 Restarting browser after {houses_processed} houses (next restart at {next_restart_at})...")
                
                close_wait = random.gauss(1.5, 0.5)
                close_wait = max(0.5, min(close_wait, 3.0))
                print(f"  ⏳ Waiting {close_wait:.2f}s before closing browser...")
                await asyncio.sleep(close_wait)
                
                await nodriver_detail.close_shared_browser()
                
                reopen_wait = random.gauss(2.0, 0.8)
                reopen_wait = max(1.0, min(reopen_wait, 5.0))
                print(f"  ⏳ Waiting {reopen_wait:.2f}s before reopening browser...")
                await asyncio.sleep(reopen_wait)
                
                browser = await nodriver_detail.get_shared_browser()
                print("✅ Browser restarted\n")
                
                # Set next restart interval
                next_restart_at = houses_processed + random.randint(45, 55)
            
            # Rescrape the house
            success = await rescrape_house(zpid, url, filepath)
            
            houses_processed += 1
            if success:
                houses_successful += 1
            else:
                houses_failed += 1
            
            # Gaussian sleep between requests (except for the last one)
            if i < len(houses_to_rescrape):
                sleep_time = random.gauss(GAUSSIAN_SLEEP_MEAN, GAUSSIAN_SLEEP_STD)
                sleep_time = max(GAUSSIAN_SLEEP_MIN, min(sleep_time, GAUSSIAN_SLEEP_MAX))
                print(f"  ⏳ Waiting {sleep_time:.2f}s before next house...")
                await asyncio.sleep(sleep_time)
        
        # Final summary
        total_time = time.time() - start_time
        print("\n" + "="*70)
        print("RESCRAPING SUMMARY")
        print("="*70)
        print(f"Total houses processed: {houses_processed}")
        print(f"✅ Successful: {houses_successful}")
        print(f"❌ Failed: {houses_failed}")
        print(f"⏱️  Total time: {total_time/60:.1f} minutes")
        if houses_processed > 0:
            print(f"⏱️  Average time per house: {total_time/houses_processed:.1f}s")
        print("="*70)
        
    finally:
        print("\nClosing browser...")
        await nodriver_detail.close_shared_browser()
        print("Browser closed.")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Rescrape houses with missing scores")
    parser.add_argument(
        "--issues-json",
        type=str,
        help="Path to issues.json file from validator (e.g., issues.json). "
             "If provided, will filter houses with missing scores from that file. "
             "If not provided, will scan all house files."
    )
    
    args = parser.parse_args()
    
    import nodriver as uc
    loop = uc.loop()
    loop.run_until_complete(main(issues_json_file=args.issues_json))

