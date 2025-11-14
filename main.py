print("Remember to use Singapore full mask")
import os
import pyzill
import json
import pyzill_scraper
import zillow_link_generator
import pyzill_files

from dotenv import load_dotenv
load_dotenv()
results_folder = os.getenv("SAVE_FOLDER") or "pyzill_results"


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
results_sold = pyzill_scraper.pyzill_scraper(north, east, south, west)

if not os.path.exists(results_folder):
    os.makedirs(results_folder)

sold_filename = os.path.join(results_folder, pyzill_files.generate_tile_filename_by_coords(north, west))
with open(sold_filename, "w") as f:
    json.dump(results_sold, f)
    print(f"Results saved to {sold_filename}")

house_count_result = pyzill_scraper.house_count(results_sold)
print(f"Number of houses returned: {house_count_result}")
print("Has a small discrepancy but it is very small so we arent worrying about it")

# Can use random choices to make it seem more user like
# the random waits, etc
# create a file to save the tiles that have 0 houses (water)
