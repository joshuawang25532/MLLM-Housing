"""
nodriver browser script to open Zillow.com
This uses nodriver (already installed) which can bypass PerimeterX blocking.
nodriver is often better at avoiding detection than Selenium.
"""
import nodriver as uc
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
BROWSER_PATH = os.getenv("BROWSER_PATH", None)  # Optional: path to Chrome/Chromium

async def open_zillow_nodriver(headless=False, wait_time=5):
    """
    Open Zillow.com using nodriver (undetected-chromedriver).
    
    Args:
        headless: If True, run browser in headless mode (no GUI)
        wait_time: Seconds to wait for page to load
    """
    print("Starting Chrome browser with nodriver...")
    
    try:
        # Configure browser options
        browser_args = []
        if headless:
            browser_args.append("--headless")
        
        # Start browser
        if BROWSER_PATH:
            browser = await uc.start(
                headless=headless,
                browser_executable_path=BROWSER_PATH,
                browser_args=browser_args
            )
        else:
            browser = await uc.start(
                headless=headless,
                browser_args=browser_args
            )
        
        # Get the main tab
        tab = browser.tabs[0]
        
        print("Navigating to Zillow.com...")
        await tab.get("https://www.zillow.com")
        
        # Wait for page to load
        print(f"Waiting {wait_time} seconds for page to load...")
        await tab.wait(wait_time)
        
        # Get page info
        page_title = tab.title
        current_url = tab.url
        
        print(f"\n✅ Page loaded successfully!")
        print(f"   Title: {page_title}")
        print(f"   URL: {current_url}")
        
        # Check for CAPTCHA and wait until solved
        async def check_captcha_present():
            """Check if CAPTCHA is present on the page"""
            try:
                page_content = await tab.get_content()
                page_source = page_content.lower() if page_content else ""
                # Check for various CAPTCHA indicators
                captcha_indicators = [
                    "press and hold",
                    "px-captcha",
                    "perimeterx",
                    "access denied",
                    "verify you are human",
                    "challenge-platform",
                    "px-captcha-container"
                ]
                return any(indicator in page_source for indicator in captcha_indicators)
            except:
                return False
        
        # Initial check
        if await check_captcha_present():
            print("\n🚫 CAPTCHA DETECTED!")
            print("   Please solve the CAPTCHA manually in the browser window.")
            print("   The script will wait until you solve it...")
            print("   (Checking every 2 seconds)\n")
            
            # Wait loop - check every 2 seconds if CAPTCHA is still present
            check_interval = 2
            max_wait_time = 600  # 10 minutes max wait
            waited_time = 0
            
            while await check_captcha_present() and waited_time < max_wait_time:
                await tab.wait(check_interval)
                waited_time += check_interval
                # Don't refresh - just check current page state
                # Refreshing would interrupt CAPTCHA solving
                
                if waited_time % 10 == 0:  # Print status every 10 seconds
                    print(f"   Still waiting... ({waited_time}s elapsed)")
            
            if await check_captcha_present():
                print(f"\n⚠️  CAPTCHA still present after {waited_time} seconds")
                print("   You may need more time, or the CAPTCHA may have changed.")
            else:
                print(f"\n✅ CAPTCHA appears to be solved! (took {waited_time} seconds)")
        else:
            print("\n✅ No CAPTCHA detected - page appears to be accessible")
        
        # Get page info
        page_title = tab.title
        current_url = tab.url
        
        print(f"\n📄 Page Status:")
        print(f"   Title: {page_title}")
        print(f"   URL: {current_url}")
        
        # Take a screenshot
        screenshot_path = "zillow_screenshot_nodriver.png"
        await tab.save_screenshot(screenshot_path)
        print(f"\n📸 Screenshot saved to: {screenshot_path}")
        
        # Keep browser open for inspection
        if not headless:
            print("\n✅ Browser will stay open for 30 seconds for inspection...")
            print("   Close the browser window or press Ctrl+C to exit early")
            await tab.wait(30)
        else:
            print("\nBrowser running in headless mode - closing in 5 seconds...")
            await tab.wait(5)
        
        return browser
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        # Clean up
        try:
            await browser.quit()
            print("\n✅ Browser closed")
        except:
            pass

def main():
    """Main entry point"""
    print("=" * 70)
    print("NODRIVER ZILLOW BROWSER TEST")
    print("=" * 70)
    print("\nOpening Zillow.com in Chrome browser...")
    print("(Set headless=True to run without GUI)\n")
    
    # Run with GUI visible (set headless=True to hide browser)
    uc.loop().run_until_complete(open_zillow_nodriver(headless=False, wait_time=5))
    
    print("\n" + "=" * 70)
    print("Test complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()

