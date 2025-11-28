"""
Standalone script to download photos from existing house JSON files.
Scans all houses in nodriver_houses folder and downloads their photos.
"""
import json
import os
import time
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from urllib.parse import urlparse

# Configuration
HOUSES_FOLDER = "nodriver_houses"
PHOTOS_FOLDER = "nodriver_photos"

# Photo download settings
PHOTO_RESOLUTION_PREFERENCE = "highest"  # Options: "highest", "original", "1536", "1344", "960", "768", "576", "384", "192"
PHOTO_FORMAT_PREFERENCE = "jpeg"  # Options: "jpeg" or "webp"
MAX_CONCURRENT_DOWNLOADS = 5  # Number of photos to download concurrently


def ensure_photos_dir():
    """Create the main photos folder if it doesn't exist."""
    if not os.path.exists(PHOTOS_FOLDER):
        os.makedirs(PHOTOS_FOLDER, exist_ok=True)
    return PHOTOS_FOLDER


def get_house_zpid_from_file(filepath):
    """Extract zpid from a house JSON file."""
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
        
        # Try to get zpid from metadata or basic_info
        zpid = data.get("metadata", {}).get("zpid") or data.get("basic_info", {}).get("zpid")
        
        if zpid:
            return str(zpid)
        
        # If not found, try to extract from filename
        filename = os.path.basename(filepath)
        if filename.endswith(".json"):
            stem = filename[:-5]  # Remove .json
            if stem.isdigit():
                return stem
        
        return None
    except Exception as e:
        print(f"  ⚠️  Error reading {filepath}: {e}")
        return None


def extract_photo_urls(photos_data):
    """Extract photo URLs from photos data based on configuration preferences.
    
    Returns:
        list of dicts with 'url', 'index', 'caption', 'subjectType' keys
    """
    if not photos_data:
        return []
    
    photo_urls = []
    
    # Get responsive photos (these have multiple resolutions)
    responsive_photos = photos_data.get("responsivePhotos", [])
    original_photos = photos_data.get("originalPhotos", [])
    
    # Determine which resolution to use
    target_width = None
    if isinstance(PHOTO_RESOLUTION_PREFERENCE, str) and PHOTO_RESOLUTION_PREFERENCE.isdigit():
        target_width = int(PHOTO_RESOLUTION_PREFERENCE)
    elif PHOTO_RESOLUTION_PREFERENCE == "highest":
        target_width = 1536  # Highest common resolution
    elif PHOTO_RESOLUTION_PREFERENCE == "original":
        # Use originalPhotos instead
        pass
    else:
        target_width = 1536  # Default to highest
    
    # Extract URLs from responsive photos
    for idx, photo in enumerate(responsive_photos):
        if PHOTO_RESOLUTION_PREFERENCE == "original":
            continue  # Skip responsive photos if we want originals
        
        resolutions = photo.get("resolutions", {})
        format_key = PHOTO_FORMAT_PREFERENCE.lower()
        
        if format_key not in resolutions:
            # Fallback to jpeg if preferred format not available
            format_key = "jpeg" if "jpeg" in resolutions else None
        
        if format_key and format_key in resolutions:
            format_resolutions = resolutions[format_key]
            
            # Find the best matching resolution
            selected_url = None
            if target_width:
                # Find exact match or closest larger resolution
                best_match = None
                best_diff = float('inf')
                for res in format_resolutions:
                    width = res.get("width", 0)
                    diff = abs(width - target_width)
                    if diff < best_diff:
                        best_diff = diff
                        best_match = res
                    # Prefer exact match
                    if width == target_width:
                        selected_url = res.get("url")
                        break
                
                if not selected_url and best_match:
                    selected_url = best_match.get("url")
            else:
                # Use the first available
                if format_resolutions:
                    selected_url = format_resolutions[0].get("url")
            
            if selected_url:
                photo_urls.append({
                    "url": selected_url,
                    "index": idx,
                    "caption": photo.get("caption", ""),
                    "subjectType": photo.get("subjectType")
                })
    
    # Extract URLs from original photos if requested
    if PHOTO_RESOLUTION_PREFERENCE == "original":
        for idx, photo in enumerate(original_photos):
            resolutions = photo.get("resolutions", {})
            format_key = PHOTO_FORMAT_PREFERENCE.lower()
            
            if format_key not in resolutions:
                format_key = "jpeg" if "jpeg" in resolutions else None
            
            if format_key and format_key in resolutions:
                format_resolutions = resolutions[format_key]
                # Get the highest resolution from originals
                if format_resolutions:
                    # Sort by width descending and take the first
                    sorted_res = sorted(format_resolutions, key=lambda x: x.get("width", 0), reverse=True)
                    selected_url = sorted_res[0].get("url")
                    
                    if selected_url:
                        photo_urls.append({
                            "url": selected_url,
                            "index": idx,
                            "caption": photo.get("caption", ""),
                            "subjectType": None  # Originals don't have subjectType
                        })
    
    return photo_urls


async def download_photo(session, url, filepath):
    """Download a single photo."""
    try:
        async with session.get(url) as response:
            if response.status == 200:
                async with aiofiles.open(filepath, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
                return True, None
            else:
                return False, f"HTTP {response.status}"
    except Exception as e:
        return False, str(e)


async def download_photos_for_house(zpid, photos_data, house_dir):
    """Download all photos for a house.
    
    Returns:
        tuple: (success_count, failed_count)
    """
    if not photos_data:
        return 0, 0
    
    # Extract photo URLs
    photo_urls = extract_photo_urls(photos_data)
    
    if not photo_urls:
        return 0, 0
    
    print(f"  📸 Found {len(photo_urls)} photos to download")
    
    # Create download tasks
    download_tasks = []
    
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        
        async def download_with_semaphore(url_info):
            async with semaphore:
                url = url_info["url"]
                index = url_info["index"]
                
                # Extract filename from URL
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path)
                
                # Create a safe filename (index_filename)
                safe_filename = f"{index:03d}_{filename}"
                filepath = os.path.join(house_dir, safe_filename)
                
                # Skip if already downloaded
                if os.path.exists(filepath):
                    return True, None
                
                success, error = await download_photo(session, url, filepath)
                return success, error
        
        # Create all download tasks
        for url_info in photo_urls:
            task = download_with_semaphore(url_info)
            download_tasks.append(task)
        
        # Execute downloads with progress tracking
        success_count = 0
        failed_count = 0
        
        for i, task in enumerate(asyncio.as_completed(download_tasks), 1):
            try:
                success, error = await task
                if success:
                    success_count += 1
                    if i % 5 == 0 or i == len(photo_urls):
                        print(f"  📥 Downloaded {i}/{len(photo_urls)} photos...")
                else:
                    failed_count += 1
                    if error:
                        print(f"  ⚠️  Failed to download photo {i}: {error}")
            except Exception as e:
                failed_count += 1
                print(f"  ⚠️  Error downloading photo {i}: {e}")
    
    return success_count, failed_count


def process_house_file(filepath):
    """Process a single house JSON file.
    
    Returns:
        tuple: (zpid, photos_data, house_dir) if successful, None if failed
    """
    zpid = get_house_zpid_from_file(filepath)
    
    if not zpid:
        print(f"⚠️  Skipping {os.path.basename(filepath)}: Could not extract ZPID")
        return None
    
    try:
        with open(filepath, "r") as f:
            house_data = json.load(f)
    except Exception as e:
        print(f"⚠️  Error reading {filepath}: {e}")
        return None
    
    # Extract photos data
    photos_data = house_data.get("photos")
    
    if not photos_data:
        print(f"⚠️  Skipping {zpid}: No photos data found")
        return None
    
    # Create house directory
    ensure_photos_dir()
    house_dir = os.path.join(PHOTOS_FOLDER, zpid)
    os.makedirs(house_dir, exist_ok=True)
    
    # Save photos JSON to file
    photos_json_path = os.path.join(house_dir, "photos.json")
    try:
        with open(photos_json_path, "w") as f:
            json.dump(photos_data, f, indent=2)
        print(f"✅ Saved photos.json for {zpid}")
    except Exception as e:
        print(f"⚠️  Error saving photos.json for {zpid}: {e}")
        return None
    
    return zpid, photos_data, house_dir


async def main():
    """Main function to process all houses and download photos."""
    print("="*70)
    print("Photo Downloader for Existing Houses")
    print("="*70 + "\n")
    
    # Get all house JSON files
    houses_path = Path(HOUSES_FOLDER)
    if not houses_path.exists():
        print(f"❌ Houses folder not found: {HOUSES_FOLDER}")
        return
    
    house_files = list(houses_path.glob("*.json"))
    # Exclude visited_houses.json
    house_files = [f for f in house_files if f.name != "visited_houses.json"]
    
    if not house_files:
        print(f"❌ No house JSON files found in {HOUSES_FOLDER}")
        return
    
    print(f"Found {len(house_files)} house files to process\n")
    
    # Configuration summary
    print(f"📸 Photo Download Configuration:")
    print(f"   Resolution preference: {PHOTO_RESOLUTION_PREFERENCE}")
    print(f"   Format preference: {PHOTO_FORMAT_PREFERENCE}")
    print(f"   Max concurrent downloads: {MAX_CONCURRENT_DOWNLOADS}")
    print(f"   Photos folder: {PHOTOS_FOLDER}")
    print()
    
    # Statistics
    houses_processed = 0
    houses_skipped = 0
    total_photos_downloaded = 0
    total_photos_failed = 0
    
    # Process each house
    for i, filepath in enumerate(house_files, 1):
        filename = filepath.name
        print(f"\n[{i}/{len(house_files)}] Processing {filename}...")
        
        result = process_house_file(filepath)
        
        if result is None:
            houses_skipped += 1
            continue
        
        zpid, photos_data, house_dir = result
        houses_processed += 1
        
        # Download photos
        print(f"  Downloading photos for ZPID: {zpid}")
        photo_start = time.time()
        success_count, failed_count = await download_photos_for_house(zpid, photos_data, house_dir)
        photo_time = time.time() - photo_start
        
        total_photos_downloaded += success_count
        total_photos_failed += failed_count
        
        if success_count > 0:
            print(f"  ✅ Downloaded {success_count} photos ({photo_time:.1f}s)")
        if failed_count > 0:
            print(f"  ⚠️  Failed to download {failed_count} photos")
        if success_count == 0 and failed_count == 0:
            print(f"  ℹ️  No photos to download")
    
    # Final summary
    print("\n" + "="*70)
    print("FINAL SUMMARY")
    print("="*70)
    print(f"Houses processed: {houses_processed}")
    print(f"Houses skipped: {houses_skipped}")
    print(f"Total photos downloaded: {total_photos_downloaded}")
    print(f"Total photos failed: {total_photos_failed}")
    print("="*70)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

