# use singapore full mask
import os
from dotenv import load_dotenv
import pyzill
import json

load_dotenv()
results_folder = os.getenv("SAVE_FOLDER")

ne_lat = 38.602951833355434
ne_long = -87.22283859375
sw_lat = 23.42674607019482
sw_long = -112.93084640625

# From tile
north = 37.7235590480963
east = -122.4137702007554
south = 37.71694904488391
west = -122.42598845170323

ne_lat  = north
ne_long = east
sw_lat  = south
sw_long = west

pagination = 1

def pyzill_scraper(north_lat, east_long, south_lat, west_long, zoom=17, search_term="San Francisco, CA"):
    results_sold = pyzill.sold(pagination, 
                search_value=search_term,
                min_beds=None,max_beds=None,
                min_bathrooms=None,max_bathrooms=None,
                min_price=None,max_price=None,
                ne_lat=north_lat,ne_long=east_long,sw_lat=south_lat,sw_long=west_long,
                zoom_value=zoom)
    return results_sold


def house_count(results_sold):
    """
    Count houses from mapResults, which contains all properties visible on the map.
    This matches what the browser displays. listResults only contains the paginated sidebar list.
    """
    return len(results_sold.get("mapResults", []))


if __name__ == "__main__":
    results_sold = pyzill_scraper(ne_lat, ne_long, sw_lat, sw_long)

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    sold_filename = os.path.join(results_folder, "jsondata_sold.json")
    with open(sold_filename, "w") as f:
        json.dump(results_sold, f)
        print(f"Results saved to {sold_filename}")

    house_count_result = house_count(results_sold)
    print(f"Number of houses returned: {house_count_result}")