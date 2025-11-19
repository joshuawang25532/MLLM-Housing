"""
nodriver-based scraper for Zillow property detail pages.
Scrapes individual property detail pages and returns raw data.
Use nodriver_parser.py to parse the raw data into clean JSON.
"""
import nodriver as uc
import json
import os
from dotenv import load_dotenv
import nodriver_parser

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
                    headless=False,
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
            
            # Navigate to Zillow homepage to establish session
            print("Establishing session with Zillow...")
            await tab.get("https://www.zillow.com")
            print("Navigated to Zillow, waiting for page load...")
            await tab.wait(3)
            
            # Quick CAPTCHA check (non-blocking, just warn)
            try:
                page_content = await tab.get_content()
                if any(indicator in page_content.lower() for indicator in ["press and hold", "px-captcha", "access denied"]):
                    print("⚠️  WARNING: CAPTCHA detected! The script may hang. Please solve it manually.")
                    print("   You can continue - the script will try to proceed anyway.")
                else:
                    print("✅ No CAPTCHA detected")
            except:
                pass  # Don't fail if we can't check
            
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


async def scrape_detail_page(detail_url, parse_data=False):
    """
    Scrape a Zillow property detail page and return raw data.
    
    Args:
        detail_url: The Zillow property detail URL
        parse_data: If True, parse the data using nodriver_parser. If False, return raw data.
    
    Returns:
        If parse_data=True: dict with clean, labeled property information
        If parse_data=False: dict with raw scraped data (next_data, scores_html, url)
    """
    try:
        browser = await get_shared_browser()
        tab = browser.tabs[0]
        
        print(f"Visiting detail page: {detail_url}")
        
        # Navigate to the detail URL
        print("  → Navigating to page...")
        await tab.get(detail_url)
        
        # Wait for page to load (with timeout awareness)
        print(f"  → Waiting {5}s for page to load...")
        await tab.wait(5)
        
        page_url = tab.url
        print(f"  → Page loaded: {page_url}")
        
        # Extract the full __NEXT_DATA__
        print("Extracting property data...")
        next_data_code = """
        (function() {
            try {
                const nextDataScript = document.getElementById('__NEXT_DATA__');
                if (nextDataScript) {
                    return nextDataScript.textContent;
                }
                return null;
            } catch (e) {
                return null;
            }
        })()
        """
        
        next_data_str = await tab.evaluate(next_data_code, await_promise=False)
        if not next_data_str:
            return {"error": "Could not extract __NEXT_DATA__"}
        
        next_data = json.loads(next_data_str)
        
        # Extract scores from visible HTML (non-blocking - don't fail if this fails)
        scores_html = []
        try:
            print("Extracting scores from HTML...")
            scores_code = """
            (function() {
                try {
                    const results = [];
                    const scoreSelectors = [
                        '[class*="score"]',
                        '[class*="Score"]',
                        '[data-testid*="score"]'
                    ];
                    
                    scoreSelectors.forEach(selector => {
                        try {
                            const elements = document.querySelectorAll(selector);
                            elements.forEach(el => {
                                try {
                                    const text = el.textContent.trim();
                                    if (text && (text.includes('Score') || text.includes('score'))) {
                                        results.push(text);
                                    }
                                } catch (e) {
                                    // Skip this element if text extraction fails
                                }
                            });
                        } catch (e) {
                            // Skip this selector if querySelectorAll fails
                        }
                    });
                    
                    return JSON.stringify(results);
                } catch (e) {
                    return JSON.stringify([]);
                }
            })()
            """
            
            scores_html_str = await tab.evaluate(scores_code, await_promise=False)
            if scores_html_str:
                try:
                    scores_html = json.loads(scores_html_str)
                    if scores_html:
                        print(f"  ✅ Extracted {len(scores_html)} score elements from HTML")
                    else:
                        print(f"  ⚠️  No score elements found in HTML (this is OK)")
                except json.JSONDecodeError as e:
                    print(f"  ⚠️  Failed to parse scores HTML JSON: {e}")
                    scores_html = []
        except Exception as e:
            # HTML extraction failed, but don't fail the whole scrape
            print(f"  ⚠️  HTML score extraction failed (non-critical): {type(e).__name__}: {e}")
            scores_html = []
        
        # If parsing is requested, use the parser
        if parse_data:
            print("Parsing data...")
            clean_data = nodriver_parser.parse_from_next_data(
                next_data, 
                scores_html=scores_html, 
                url=detail_url, 
                scraped_url=page_url
            )
            if "error" not in clean_data:
                print(f"✅ Successfully extracted property data for ZPID: {clean_data.get('metadata', {}).get('zpid')}")
            return clean_data
        else:
            # Return raw data for custom processing
            return {
                "next_data": next_data,
                "scores_html": scores_html,
                "url": detail_url,
                "scraped_url": page_url
            }
        
    except Exception as e:
        print(f"❌ Error scraping detail page: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "url": detail_url
        }

async def main():
    """Test scraping a detail page."""
    test_url = "https://www.zillow.com/homedetails/574-26th-Ave-APT-3-San-Francisco-CA-94121/96032188_zpid/"
    
    print("="*70)
    print("Zillow Detail Page Scraper - Raw Data Output")
    print("="*70 + "\n")
    
    try:
        result = await scrape_detail_page(test_url, parse_data=False)
        
        if "error" in result:
            print(f"\n❌ Error: {result['error']}")
            return
        
        print("\n" + "="*70)
        print("RAW SCRAPED DATA")
        print("="*70)
        print(f"\n✅ URL: {result.get('url')}")
        print(f"✅ Scraped URL: {result.get('scraped_url')}")
        print(f"✅ __NEXT_DATA__ extracted: {result.get('next_data') is not None}")
        print(f"✅ Scores HTML extracted: {len(result.get('scores_html', []))} items")
        
        if result.get('scores_html'):
            print("\n📊 Sample scores HTML:")
            for i, score_text in enumerate(result['scores_html'][:3], 1):
                print(f"  {i}. {score_text[:100]}...")
        
        # Save raw results to file
        output_file = "nodriver_detail_test_raw.json"
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n✅ Raw data saved to {output_file}")
        
        # Optionally parse it
        print("\n" + "="*70)
        print("PARSING RAW DATA")
        print("="*70)
        try:
            import nodriver_parser
            parsed = nodriver_parser.parse_from_next_data(
                result['next_data'],
                scores_html=result.get('scores_html', []),
                url=result.get('url'),
                scraped_url=result.get('scraped_url')
            )
            
            if "error" not in parsed:
                print(f"\n✅ Parsed successfully!")
                print(f"📍 Address: {parsed['basic_info']['address']}, {parsed['basic_info']['city']}, {parsed['basic_info']['state']} {parsed['basic_info']['zipcode']}")
                print(f"🏠 {parsed['basic_info']['bedrooms']} bed, {parsed['basic_info']['bathrooms']} bath, {parsed['basic_info']['livingArea']} sqft")
                print(f"💰 Price: ${parsed['financial']['price']:,}")
                print(f"📊 Walk Score: {parsed['scores'].get('walkScore', 'N/A')}")
                print(f"🚇 Transit Score: {parsed['scores'].get('transitScore', 'N/A')}")
                print(f"🚴 Bike Score: {parsed['scores'].get('bikeScore', 'N/A')}")
                print(f"🏫 Schools: {len(parsed['schools'])} schools found")
                
                # Save parsed results
                parsed_file = "nodriver_detail_test_parsed.json"
                with open(parsed_file, "w") as f:
                    json.dump(parsed, f, indent=2)
                print(f"\n✅ Parsed JSON saved to {parsed_file}")
            else:
                print(f"\n⚠️  Parsing error: {parsed.get('error')}")
        except Exception as e:
            print(f"\n⚠️  Could not parse: {e}")
        
    finally:
        print("\nClosing browser...")
        await close_shared_browser()
        print("Browser closed.")

if __name__ == "__main__":
    loop = uc.loop()
    loop.run_until_complete(main())

