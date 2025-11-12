import os
import dotenv
import pandas as pd

dotenv.load_dotenv()

path = os.getenv("HOUSING_DATA_PATH")

print("Loading dataset from:", path)

df = pd.read_csv(path + "/HouseTS.csv")

print(df.head())

dmv_multi_data_dir = os.path.join(path, "DMV_Multi_Data", "DMV_Multi_Data")
if os.path.isdir(dmv_multi_data_dir):
    photo_zipcodes = [name for name in os.listdir(dmv_multi_data_dir) if os.path.isdir(os.path.join(dmv_multi_data_dir, name))]
else:
    photo_zipcodes = []

print("Photo zipcodes:", sorted(photo_zipcodes))
print(len(photo_zipcodes))



# Delete test_year.txt for zipcode 20851, if it exists
test_year_path = os.path.join(dmv_multi_data_dir, "20851", "test_year.txt")
if os.path.isfile(test_year_path):
    os.remove(test_year_path)
    print("Deleted test_year.txt for zipcode 20851.")
else:
    print("test_year.txt not found for zipcode 20851. Probably already deleted")

