print("Remember to use Singapore full mask")
import os
import pyzill
import json
import pyzill_scraper
import zillow_link_generator
import pyzill_files

from dotenv import load_dotenv
load_dotenv()

print("Generating zillow links")
zillow_link_generator.main()
print("Zillow links generated")

print("Loading zillow links")
with open("zillow_links.json", "r") as f:
    zillow_links = json.load(f)
print(f"Zillow links loaded: {len(zillow_links)}")

print("Scraping Zillow #80")
first_link = zillow_links[80]
print(first_link)
coords = first_link["coordinates"]

north = coords["north"]
east = coords["east"]
south = coords["south"]
west = coords["west"]

print("Scraping...")
res = pyzill_scraper.pyzill_scraper_master(north, east, south, west)
sold_path = res["path"]
print(f"Number of houses returned: {res['house_count']}")




print("Testing empty tile handling")
first_link = zillow_links[1]

coords = first_link["coordinates"]

north = coords["north"]
east = coords["east"]
south = coords["south"]
west = coords["west"]

print("Scraping...")
res = pyzill_scraper.pyzill_scraper_master(north, east, south, west)

if res['skipped']:
    print(f"Previously empty tile detected, skipping scrape")
elif res['empty_recorded']:
    print(f"Empty tile detected and recorded")
else:
    print(f"Successfully scraped: {res['house_count']} houses found")




# Plan:
# - Use random delays between HTTP requests during scraping to better mimic human browsing behavior.
#   - This could involve using `random.uniform` or `random.randint` to pick wait durations.
# - After scraping each tile, check the number of houses/entries returned.
# - If a tile has 0 houses, consider it as "water" (i.e., empty or of no interest).
# - Maintain a file (e.g., "empty_tiles.json" or "water_tiles.json") where you append or store the coordinates/indexes of these zero-house tiles for future avoidance.
# - This log can help prevent redundant scraping of useless/water tiles.
