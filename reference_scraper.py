import os
import nodriver
import asyncio
from dotenv import load_dotenv

load_dotenv()
TILE_LINK = os.getenv("TILE_LINK")
BROWSER_PATH = os.getenv("BROWSER_PATH")


ZILLOW_BASE_URL = "https://www.zillow.com/" 


async def main():

    browser = await nodriver.start()
    page = await browser.get('https://www.nowsecure.nl')

    await page.save_screenshot()
    await page.get_content()
    await page.scroll_down(150)
    elems = await page.select_all('*[src]')

    for elem in elems:
        await elem.flash()

    page2 = await browser.get('https://twitter.com', new_tab=True)
    page3 = await browser.get('https://github.com/ultrafunkamsterdam/nodriver', new_window=True)

    for p in (page, page2, page3):
       await p.bring_to_front()
       await p.scroll_down(200)
       await p   # wait for events to be processed
       await p.reload()
       if p != page3:
           await p.close()

if __name__ == '__main__':

    # since asyncio.run never worked (for me)
    uc.loop().run_until_complete(main())




async def fetch_zillow_tile(tile_url: str, base_url: str):
    if not tile_url:
        print("❌ TILE_LINK is not set in your .env file.")
        return

    # 1. Start a stealthy, asynchronous browser instance
    # headless=False shows the browser GUI, which is helpful for debugging Zillow.
    # Change to headless=True for production.
    browser = await uc.start(
        headless=False,
        # Optional: Add user-agent here if you want to override the default stealth one
        # browser_args=['--user-agent="<YOUR USER AGENT>"']
        browser_executable_path=BROWSER_PATH,
    )
    
    # Use the first tab created
    tab = browser.tabs[0]

    try:
        # 2. **CRITICAL WARM-UP:** Navigate to the main Zillow page first.
        # This executes their anti-bot JavaScript and sets the required cookies/tokens.
        print(f"1. Warming up session on: {base_url}")
        await tab.get(base_url, timeout=30)
        
        # You may want a small wait here to ensure all JS is done
        await tab.wait(5)
        print(f"   Warm-up successful. Cookies established.")

        # 3. Request the specific tile URL within the established session
        print(f"2. Requesting tile from: {tile_url}")
        # nodriver handles all cookies and headers from the established session
        response = await tab.get(tile_url)

        # 4. Check status and process the content
        if response and response.status == 200:
            # For a tile (which is an image), you want the binary content, not text
            tile_content = await response.body()
            
            # --- Save the tile content to a file ---
            file_name = "zillow_tile.png"
            with open(file_name, "wb") as f:
                f.write(tile_content)
            
            print(f"✅ Success! Tile content saved to {file_name} ({len(tile_content)} bytes).")
            
        elif response:
            print(f"❌ Tile request failed with Status Code: {response.status}")
        else:
            print("❌ Tile request failed. No response received.")


    except Exception as e:
        print(f"🚨 An error occurred during fetching: {e}")
        
    finally:
        # 5. Clean up and close the browser instance
        await browser.quit()

# --- Run the asynchronous script ---
if __name__ == "__main__":
    uc.loop().run_until_complete(fetch_zillow_tile(TILE_LINK, ZILLOW_BASE_URL))