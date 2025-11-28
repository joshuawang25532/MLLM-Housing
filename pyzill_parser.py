import os
import json
import pprint
from dotenv import load_dotenv

load_dotenv()
results_folder = os.getenv("SAVE_FOLDER")

sold_filename = os.path.join(results_folder, "jsondata_sold.json")

with open(sold_filename, "r") as f:
    results_sold = json.load(f)

pprint.pprint(results_sold, indent=4)

