import kagglehub
import os
from dotenv import load_dotenv

path = kagglehub.dataset_download("shengkunwang/housets-dataset")

print("Dataset downloaded and stored in ", path)

env_file = ".env"

if os.path.exists(env_file):
    with open(env_file, "r") as f:
        lines = f.readlines()
    lines = [line for line in lines if not line.startswith("HOUSING_DATA_PATH=")]
else:
    lines = []

lines.append(f'HOUSING_DATA_PATH="{path}"\n')

with open(env_file, "w") as f:
    f.writelines(lines)

print("HOUSING_DATA_PATH added to .env file")