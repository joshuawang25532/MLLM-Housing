"""
nodriver-based visitor for Zillow links.
Loads zillow_links.json and visits each URL directly using nodriver browser automation.
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
            await tab.wait(3)  # Wait for page to load
            
            # Check for CAPTCHA
            print("Checking for CAPTCHA...")
            page_content = await tab.get_content()
            if any(indicator in page_content.lower() for indicator in ["press and hold", "px-captcha", "access denied"]):
                print("⚠️  CAPTCHA detected on homepage. Please solve it manually...")
                # Wait for CAPTCHA to be solved
                while any(indicator in (await tab.get_content()).lower() for indicator in ["press and hold", "px-captcha", "access denied"]):
                    await tab.wait(2)
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

async def visit_zillow_link(link_url, index=None):
    """
    Visit a Zillow link URL and check if it loads successfully.
    
    Args:
        link_url: The Zillow URL to visit
        index: Optional index number for logging
    
    Returns:
        dict with success status and page info
    """
    try:
        browser = await get_shared_browser()
        tab = browser.tabs[0]
        
        index_str = f" [{index}]" if index is not None else ""
        print(f"Visiting link{index_str}: {link_url[:80]}...")
        
        # Navigate to the URL
        await tab.get(link_url)
        
        # Wait for page to load initially
        await tab.wait(3)
        
        # Spend 10 seconds on the page after it loads
        print(f"  Waiting 10 seconds on page{index_str}...")
        await tab.wait(10)
        
        # Get page content to check if it loaded successfully
        page_content = await tab.get_content()
        page_url = tab.url
        
        # Check for various error conditions
        content_lower = page_content.lower()
        has_captcha = any(indicator in content_lower for indicator in ["press and hold", "px-captcha", "access denied"])
        has_error = any(indicator in content_lower for indicator in ["error", "not found", "404", "blocked"])
        has_listings = any(indicator in content_lower for indicator in ["listresults", "mapresults", "property-card", "zestimate"])
        
        success = not has_captcha and not has_error and has_listings
        
        result = {
            "success": success,
            "url": link_url,
            "loaded_url": page_url,
            "has_captcha": has_captcha,
            "has_error": has_error,
            "has_listings": has_listings,
            "content_length": len(page_content)
        }
        
        if success:
            print(f"✅ Link{index_str} loaded successfully")
        elif has_captcha:
            print(f"⚠️  Link{index_str} has CAPTCHA")
        elif has_error:
            print(f"❌ Link{index_str} has error")
        else:
            print(f"⚠️  Link{index_str} loaded but no listings detected")
        
        # Add a small delay between visits
        sleep_utils.gaussian_sleep()
        
        return result
        
    except Exception as e:
        print(f"❌ Error visiting link{index_str if index is not None else ''}: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "url": link_url,
            "error": str(e)
        }

async def visit_all_links(zillow_links, indexes):
    """
    Async function to visit all specified links.
    """
    # Initialize browser
    print("Initializing browser...")
    await get_shared_browser()
    print("Browser ready!\n")
    
    # Visit each link
    results = []
    successful = 0
    failed = 0
    
    try:
        for i in indexes:
            link_data = zillow_links[i]
            link_url = link_data.get("link")
            
            if not link_url:
                print(f"⚠️  Index {i}: No link found, skipping")
                continue
            
            result = await visit_zillow_link(link_url, index=i)
            result["index"] = i
            result["coordinates"] = link_data.get("coordinates")
            results.append(result)
            
            if result.get("success"):
                successful += 1
            else:
                failed += 1
            
            print()  # Blank line between visits
        
    finally:
        # Close browser
        print("\nClosing browser...")
        await close_shared_browser()
        print("Browser closed.")
    
    return results, successful, failed

def main():
    """Main function to load zillow_links.json and visit each link."""
    print("="*70)
    print("Zillow Link Visitor using NODRIVER")
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
    
    # You can modify this to visit specific indexes or all links
    # indexes = [i for i in range(len(zillow_links))]  # Visit all links
    indexes = [80, 100, 200]  # Visit specific indexes
    
    # Visit all links using async function
    loop = uc.loop()
    results, successful, failed = loop.run_until_complete(visit_all_links(zillow_links, indexes))
    
    # Print summary
    print("\n" + "="*70)
    print("VISITING SUMMARY")
    print("="*70)
    print(f"Total links visited: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success rate: {successful/len(results)*100:.1f}%" if results else "N/A")
    print("="*70)
    
    # Save results to file
    results_file = "nodriver_visit_results.json"
    print(f"\nSaving results to {results_file}...")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {results_file}")

if __name__ == "__main__":
    main()

