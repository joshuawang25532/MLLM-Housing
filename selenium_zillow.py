"""
Selenium browser script to open Zillow.com
This uses a real browser which can bypass PerimeterX blocking.

INSTALLATION:
    pip install selenium
    # Also need ChromeDriver - Selenium 4+ manages this automatically
    # Or install manually: brew install chromedriver (on Mac)

ALTERNATIVE:
    If you prefer nodriver (already installed), see nodriver_zillow.py
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def open_zillow(headless=False, wait_time=5):
    """
    Open Zillow.com in a Chrome browser using Selenium.
    
    Args:
        headless: If True, run browser in headless mode (no GUI)
        wait_time: Seconds to wait for page to load
    """
    # Configure Chrome options
    chrome_options = Options()
    
    if headless:
        chrome_options.add_argument("--headless")
    
    # Add options to make browser less detectable
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Optional: Set window size
    chrome_options.add_argument("--window-size=1920,1080")
    
    print("Starting Chrome browser...")
    try:
        # Initialize the driver
        # Selenium 4+ automatically manages the driver
        driver = webdriver.Chrome(options=chrome_options)
        
        # Execute script to remove webdriver property
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print("Navigating to Zillow.com...")
        driver.get("https://www.zillow.com")
        
        # Wait for page to load
        print(f"Waiting {wait_time} seconds for page to load...")
        time.sleep(wait_time)
        
        # Check if page loaded successfully
        page_title = driver.title
        current_url = driver.current_url
        
        print(f"\n✅ Page loaded successfully!")
        print(f"   Title: {page_title}")
        print(f"   URL: {current_url}")
        
        # Check for CAPTCHA and wait until solved
        def check_captcha_present():
            """Check if CAPTCHA is present on the page"""
            try:
                page_source = driver.page_source.lower()
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
        if check_captcha_present():
            print("\n🚫 CAPTCHA DETECTED!")
            print("   Please solve the CAPTCHA manually in the browser window.")
            print("   The script will wait until you solve it...")
            print("   (Checking every 2 seconds)\n")
            
            # Wait loop - check every 2 seconds if CAPTCHA is still present
            check_interval = 2
            max_wait_time = 600  # 10 minutes max wait
            waited_time = 0
            
            while check_captcha_present() and waited_time < max_wait_time:
                time.sleep(check_interval)
                waited_time += check_interval
                # Don't refresh - just check current page state
                # Refreshing would interrupt CAPTCHA solving
                
                if waited_time % 10 == 0:  # Print status every 10 seconds
                    print(f"   Still waiting... ({waited_time}s elapsed)")
            
            if check_captcha_present():
                print(f"\n⚠️  CAPTCHA still present after {waited_time} seconds")
                print("   You may need more time, or the CAPTCHA may have changed.")
            else:
                print(f"\n✅ CAPTCHA appears to be solved! (took {waited_time} seconds)")
        else:
            print("\n✅ No CAPTCHA detected - page appears to be accessible")
        
        # Check if page loaded successfully
        page_title = driver.title
        current_url = driver.current_url
        
        print(f"\n📄 Page Status:")
        print(f"   Title: {page_title}")
        print(f"   URL: {current_url}")
        
        # Take a screenshot for verification
        screenshot_path = "zillow_screenshot.png"
        driver.save_screenshot(screenshot_path)
        print(f"\n📸 Screenshot saved to: {screenshot_path}")
        
        # Keep browser open for inspection
        if not headless:
            print("\n✅ Browser will stay open for 30 seconds for inspection...")
            print("   Close the browser window or press Ctrl+C to exit early")
            time.sleep(30)
        else:
            print("\nBrowser running in headless mode - closing in 5 seconds...")
            time.sleep(5)
        
        return driver
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        # Clean up
        try:
            driver.quit()
            print("\n✅ Browser closed")
        except:
            pass

if __name__ == "__main__":
    print("=" * 70)
    print("SELENIUM ZILLOW BROWSER TEST")
    print("=" * 70)
    print("\nOpening Zillow.com in Chrome browser...")
    print("(Set headless=True to run without GUI)\n")
    
    # Run with GUI visible (set headless=True to hide browser)
    driver = open_zillow(headless=False, wait_time=5)
    
    print("\n" + "=" * 70)
    print("Test complete!")
    print("=" * 70)

