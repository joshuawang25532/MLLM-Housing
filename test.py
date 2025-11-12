import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

# print(os.getenv("HOUSING_DATA_PATH"))

recommended_filter_dir = "recommended-filter"
mls_set = set()

for fname in os.listdir(recommended_filter_dir):
    if fname.lower().endswith(".csv"):
        fpath = os.path.join(recommended_filter_dir, fname)
        # The header is on the first line, but actual data starts on the fourth line.
        # It's okay if a row has no data in the MLS# column; those will be dropped anyway.
        try:
            df = pd.read_csv(fpath, skiprows=[1,2])
        except pd.errors.EmptyDataError:
            continue  # Skip empty files
        if "MLS#" not in df.columns:
            continue
        mls_vals = df["MLS#"].dropna().astype(str).str.strip()
        mls_set.update(val for val in mls_vals if val)

print(len(mls_set))
