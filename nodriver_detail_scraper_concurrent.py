"""
Concurrent-safe version of nodriver_detail_scraper.py
Supports running multiple instances simultaneously without interference.

Key changes:
1. File locking for visited_houses.json
2. Atomic file operations for house data files
3. Process-safe canary files
4. Each process maintains its own browser instance (no shared globals)

Large-scale scraper for Zillow property detail pages.
Iterates over all houses in nodriver_results tile files and scrapes their detail pages.
Saves parsed results to nodriver_houses folder, tracking visited houses.
"""
import json
import os
import random
import time
import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
# Lazy imports to avoid requiring nodriver at import time
# import nodriver_detail
# import nodriver_parser

# Import file locking module (platform-specific)
_has_file_locking = False
_fcntl = None
_msvcrt = None

try:
    if sys.platform == 'win32':
        import msvcrt
        _msvcrt = msvcrt
        _has_file_locking = True
    else:
        import fcntl
        _fcntl = fcntl
        _has_file_locking = True
except ImportError as e:
    _has_file_locking = False
    print(f"Warning: File locking not available ({e}). Concurrent execution may have race conditions.")

load_dotenv()

# Configuration
RESULTS_FOLDER = "nodriver_results"
HOUSES_FOLDER = "nodriver_houses"
VISITED_HOUSES_FILE = os.path.join(HOUSES_FOLDER, "visited_houses.json")
VISITED_HOUSES_LOCK_FILE = os.path.join(HOUSES_FOLDER, "visited_houses.lock")

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

# Process ID for unique identification
PROCESS_ID = os.getpid()


class FileLock:
    """Cross-platform file locking context manager."""
    def __init__(self, lock_file_path, timeout=30):
        self.lock_file_path = lock_file_path
        self.timeout = timeout
        self.lock_file = None
        
    def __enter__(self):
        if not _has_file_locking:
            # No file locking available - return a no-op context manager
            return self
        
        # Ensure lock file directory exists
        lock_dir = os.path.dirname(self.lock_file_path)
        if lock_dir and lock_dir != '.':
            os.makedirs(lock_dir, exist_ok=True)
        
        # Check if lock file exists and contains a PID
        if os.path.exists(self.lock_file_path):
            try:
                with open(self.lock_file_path, 'r') as f:
                    lock_pids = [line.strip() for line in f.readlines() if line.strip().isdigit()]
                    if lock_pids:
                        last_pid = int(lock_pids[-1])
                        # Check if the process is still running
                        try:
                            os.kill(last_pid, 0)  # Signal 0 just checks if process exists
                        except OSError:
                            # Process doesn't exist - remove stale lock file
                            print(f"⚠️  Removing stale lock file (process {last_pid} no longer exists)")
                            try:
                                os.remove(self.lock_file_path)
                            except:
                                pass
            except:
                pass
        
        # Try to acquire lock with timeout
        start_time = time.time()
        attempts = 0
        while True:
            attempts += 1
            try:
                # Open lock file in append mode to avoid truncating if it exists
                self.lock_file = open(self.lock_file_path, 'a+')
                
                if sys.platform == 'win32':
                    _msvcrt.locking(self.lock_file.fileno(), _msvcrt.LK_NBLCK, 1)
                else:
                    _fcntl.flock(self.lock_file.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
                # Lock acquired
                self.lock_file.seek(0, 2)  # Seek to end
                self.lock_file.write(f"{PROCESS_ID}\n")
                self.lock_file.flush()
                if attempts > 1:
                    print(f"✅ Acquired lock after {attempts} attempts (waited {time.time() - start_time:.1f}s)")
                return self
            except (IOError, OSError) as e:
                # Close file if we opened it but failed to lock
                if self.lock_file:
                    try:
                        self.lock_file.close()
                    except:
                        pass
                    self.lock_file = None
                
                # Lock is held by another process
                elapsed = time.time() - start_time
                if elapsed > self.timeout:
                    # Check what process might be holding the lock
                    lock_info = ""
                    if os.path.exists(self.lock_file_path):
                        try:
                            with open(self.lock_file_path, 'r') as f:
                                lock_pids = [line.strip() for line in f.readlines() if line.strip().isdigit()]
                                if lock_pids:
                                    lock_info = f" (lock file contains PIDs: {', '.join(lock_pids)})"
                        except:
                            pass
                    raise TimeoutError(
                        f"Could not acquire lock on {self.lock_file_path} after {self.timeout}s{lock_info}. "
                        f"Another process may be running. Try removing the lock file if no other process is active."
                    )
                if attempts == 1:
                    print(f"⏳ Waiting for lock (held by another process)...")
                time.sleep(0.1)
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_file and _has_file_locking:
            try:
                if sys.platform == 'win32':
                    _msvcrt.locking(self.lock_file.fileno(), _msvcrt.LK_UNLCK, 1)
                else:
                    _fcntl.flock(self.lock_file.fileno(), _fcntl.LOCK_UN)
            except:
                pass
            self.lock_file.close()
            self.lock_file = None


def ensure_houses_dir():
    """Create the houses folder if it doesn't exist."""
    if not os.path.exists(HOUSES_FOLDER):
        os.makedirs(HOUSES_FOLDER, exist_ok=True)
    return HOUSES_FOLDER


def load_visited_houses():
    """Load the set of visited house zpid/detailUrls from file with locking."""
    ensure_houses_dir()
    visited_zpids = set()
    visited_urls = set()
    
    # Use file lock to prevent concurrent access
    with FileLock(VISITED_HOUSES_LOCK_FILE):
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
            data = {
                "visited_zpids": sorted(list(visited_zpids)),
                "visited_urls": sorted(list(visited_urls)),
                "total_visited": len(visited_zpids)
            }
            try:
                # Atomic write: write to temp file, then rename
                temp_file = VISITED_HOUSES_FILE + ".tmp"
                with open(temp_file, "w") as f:
                    json.dump(data, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())  # Ensure data is written to disk
                os.replace(temp_file, VISITED_HOUSES_FILE)  # Atomic rename
            except Exception as e:
                print(f"Warning: Could not save synced visited houses: {e}")
    
    return visited_zpids, visited_urls


def save_visited_house(zpid, detail_url, visited_zpids=None, visited_urls=None):
    """Add a house zpid and URL to the visited houses file with locking.
    
    Args:
        zpid: The zpid to mark as visited (can be None)
        detail_url: The detail URL to mark as visited
        visited_zpids: Optional set to update (avoids reloading)
        visited_urls: Optional set to update (avoids reloading)
    """
    ensure_houses_dir()
    
    # Use file lock to prevent concurrent access
    with FileLock(VISITED_HOUSES_LOCK_FILE):
        # Load current state
        if visited_zpids is None or visited_urls is None:
            if os.path.exists(VISITED_HOUSES_FILE):
                try:
                    with open(VISITED_HOUSES_FILE, "r") as f:
                        data = json.load(f)
                        visited_zpids = set(data.get("visited_zpids", []))
                        visited_urls = set(data.get("visited_urls", []))
                except Exception as e:
                    print(f"Warning: Could not load visited houses: {e}")
                    visited_zpids = set()
                    visited_urls = set()
            else:
                visited_zpids = set()
                visited_urls = set()
        
        # Add new entries
        if zpid:
            visited_zpids.add(str(zpid))
        if detail_url:
            visited_urls.add(detail_url)
        
        # Save with atomic write
        data = {
            "visited_zpids": sorted(list(visited_zpids)),
            "visited_urls": sorted(list(visited_urls)),
            "total_visited": len(visited_zpids)
        }
        
        try:
            temp_file = VISITED_HOUSES_FILE + ".tmp"
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, VISITED_HOUSES_FILE)
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
        
        if not isinstance(all_houses, list):
            print(f"❌ Invalid format: expected list, got {type(all_houses)}")
            return []
        
        valid_houses = []
        for house in all_houses:
            if not isinstance(house, dict):
                continue
            
            zpid = house.get("zpid")
            detail_url = house.get("detailUrl")
            
            if detail_url:
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
    """Get the next available nullzpid counter by checking existing files (with locking)."""
    ensure_houses_dir()
    
    # Use file lock to prevent race conditions
    with FileLock(VISITED_HOUSES_LOCK_FILE):
        existing_files = list(Path(HOUSES_FOLDER).glob("nullzpid_*.json"))
        
        if not existing_files:
            return 1
        
        counters = []
        for file in existing_files:
            try:
                num_str = file.stem.split("_")[1]
                counters.append(int(num_str))
            except (ValueError, IndexError):
                continue
        
        if not counters:
            return 1
        
        return max(counters) + 1


def save_house_data(parsed_data, zpid):
    """Save parsed house data to nodriver_houses folder with atomic operations."""
    ensure_houses_dir()
    
    if not zpid:
        zpid = parsed_data.get("metadata", {}).get("zpid") or parsed_data.get("basic_info", {}).get("zpid")
    
    if zpid:
        zpid = str(zpid)
    
    # Generate filename
    if zpid:
        filename = f"{zpid}.json"
    else:
        null_counter = get_next_nullzpid_counter()
        filename = f"nullzpid_{null_counter}.json"
        print(f"  ⚠️  No ZPID found, using filename: {filename}")
    
    filepath = os.path.join(HOUSES_FOLDER, filename)
    
    # Check if file already exists (with lock to prevent race condition)
    with FileLock(VISITED_HOUSES_LOCK_FILE):
        if os.path.exists(filepath):
            print(f"  ⚠️  File already exists: {filename}, skipping save")
            return None
        
        # Atomic write: write to temp file, then rename
        try:
            temp_filepath = filepath + ".tmp"
            with open(temp_filepath, "w") as f:
                json.dump(parsed_data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_filepath, filepath)
            return filepath
        except Exception as e:
            print(f"  ⚠️  Error saving house data: {e}")
            import traceback
            traceback.print_exc()
            # Clean up temp file if it exists
            temp_filepath = filepath + ".tmp"
            if os.path.exists(temp_filepath):
                try:
                    os.remove(temp_filepath)
                except:
                    pass
            return None


async def scrape_all_houses():
    """Scrape all houses from all_house_urls.json (concurrent-safe version)."""
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
    
    # Load visited houses (with locking)
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
    
    # Initialize browser (each process gets its own instance)
    print("Initializing browser...")
    browser = await nodriver_detail.get_shared_browser()
    print("✅ Browser ready\n")
    
    # Statistics
    houses_processed = 0
    houses_saved = 0
    houses_failed = 0
    houses_skipped = 0
    start_time = time.time()
    
    # Randomized browser restart interval (45-55 houses)
    next_restart_at = random.randint(45, 55)
    
    try:
        for i, house in enumerate(remaining_houses, 1):
            zpid = house.get("zpid")
            detail_url = house.get("detailUrl")
            
            # Double-check if already visited (another process might have scraped it)
            # Note: load_visited_houses() already handles locking internally
            current_zpids, current_urls = load_visited_houses()
            if (zpid and zpid in current_zpids) or (detail_url in current_urls):
                print(f"  ⚠️  House already visited by another process, skipping")
                houses_skipped += 1
                continue
            
            # Restart browser at randomized intervals to prevent memory leaks and detection
            if houses_processed > 0 and houses_processed >= next_restart_at:
                print(f"\n🔄 Restarting browser after {houses_processed} houses (next restart at {next_restart_at})...")
                
                # Gaussian wait before closing
                close_wait = random.gauss(1.5, 0.5)  # mean=1.5s, std=0.5s
                close_wait = max(0.5, min(close_wait, 3.0))  # Clamp between 0.5-3.0s
                print(f"  ⏳ Waiting {close_wait:.2f}s before closing browser...")
                await asyncio.sleep(close_wait)
                
                await nodriver_detail.close_shared_browser()
                
                # Gaussian wait before reopening
                reopen_wait = random.gauss(2.0, 0.8)  # mean=2.0s, std=0.8s
                reopen_wait = max(1.0, min(reopen_wait, 5.0))  # Clamp between 1.0-5.0s
                print(f"  ⏳ Waiting {reopen_wait:.2f}s before reopening browser...")
                await asyncio.sleep(reopen_wait)
                
                browser = await nodriver_detail.get_shared_browser()
                
                # Set next restart interval (randomized 45-55 houses from current count)
                next_restart_at = houses_processed + random.randint(45, 55)
                print(f"✅ Browser restarted (next restart at {next_restart_at} houses)\n")
            
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
                    
                    # Check if this is a CAPTCHA (timeout after 15s)
                    if raw_data.get("captcha", False):
                        print(f"  🚫 CAPTCHA detected - rotating browser...")
                        try:
                            await nodriver_detail.rotate_browser_on_captcha()
                            # Retry this house once after rotation
                            print(f"  🔄 Retrying house after browser rotation...")
                            raw_data = await nodriver_detail.scrape_detail_page(detail_url, parse_data=False)
                            scrape_time = time.time() - scrape_start
                            print(f"  ⏱️  Retry scraping took {scrape_time:.1f}s")
                            
                            # If retry also fails, mark as failed
                            if "error" in raw_data:
                                print(f"  ❌ Retry also failed: {raw_data['error']}")
                                houses_failed += 1
                                continue
                        except Exception as e:
                            print(f"  ❌ Browser rotation failed: {e}")
                            houses_failed += 1
                            continue
                    else:
                        # Non-CAPTCHA error, just mark as failed
                        houses_failed += 1
                        continue
                
                # Check for required data (next_data is critical, scores_html is optional)
                if "next_data" not in raw_data:
                    print(f"  ❌ Missing next_data in raw_data")
                    houses_failed += 1
                    continue
                
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

