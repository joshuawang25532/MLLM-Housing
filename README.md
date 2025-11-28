# MLLM-Housing

## Project Overview
MLLM-Housing is a machine learning project designed to predict housing prices by integrating traditional structured data with abstract quality assessments (e.g., beauty, cleanliness, spaciousness) derived from Multimodal Large Language Models (MLLMs).

The project utilizes a robust scraping pipeline to collect real estate data (Zillow), processes it through a comprehensive cleaning and imputation workflow, and employs XGBoost for price prediction. Unique to this project is the use of Gemini-encoded descriptions to capture qualitative features from property text.

## Core Functions

### 1. Advanced Data Collection (Scraping)
The project implements a custom, driverless scraping solution using `nodriver` to bypass standard bot detection mechanisms.
- **Concurrent Scraping**: Supports running multiple scraper instances simultaneously to maximize throughput.
- **Resilience**: Implements retry logic, rate limiting (Gaussian-distributed delays), and error handling for network issues.
- **Data Integrity**: Validates scraped data against expected schemas to ensure quality.

### 2. Data Processing Pipeline
A unified pipeline orchestrates the transformation of raw JSON data into a model-ready CSV dataset.
- **Validation**: Checks structure and content of scraped files using `utils/data_validator.py`.
- **Preprocessing**: Cleans raw fields, handles missing values, and normalizes data.
- **One-Hot Encoding**: Converts categorical variables for machine learning compatibility.
- **Imputation**: Uses KNN imputation to fill missing numerical data.
- **Feature Engineering**: Merges structured data with LLM-derived features (`gemini_encoded_descriptions.csv`).

### 3. Predictive Modeling
- **XGBoost + LLM Price Predictor**: The core model for price prediction, tuned via RandomizedSearchCV.
- **Evaluation**: Comprehensive metrics including RMSE, MAE, RMSLE, and R².

## Collaboration Scheme Summary
The project features a **Concurrent Scraping Architecture** designed for scalability:
- **File Locking**: Uses platform-specific locking (fcntl/msvcrt) to safely manage shared resources like `visited_houses.json` across multiple processes.
- **Atomic Operations**: Ensures data consistency during file writes.
- **Process Isolation**: Each scraper instance maintains its own browser context and "canary" files to prevent conflicts.
- **Distributed Work**: Multiple scraper scripts can run in parallel on the same machine or across a network (sharing a filesystem) to accelerate data collection.

## Environment Setup

1. **Prerequisites**: Ensure you have [Anaconda](https://www.anaconda.com/) or Miniconda installed.

2. **Create Environment**:
   ```bash
   conda env create -f environment.yml
   ```

3. **Activate Environment**:
   ```bash
   conda activate MLLMHousing
   ```

## Quick-Start Commands

To run the model, run the command:
```bash
jupyter notebook model/xgboost.ipynb
```
Alternatively, start from the beginning. This system is designed to be run end-to-end, starting with data collection.

### 1. Data Collection (Scraping)
The data collection process has 3 steps:

**Step 1: Generate Search Links**
Generate grid-based search URLs to cover the target area.
```bash
python -m scripts.scraping.generate_links
```
*Output: `data/zillow_links.json`*

**Step 2: Extract Property URLs**
Extract unique property URLs from the search grid. This step processes the generated links to identify all available properties.
```bash
python -m scripts.scraping.extract_urls
```
*Output: `data/raw_tiles/all_house_urls.json`*

**Step 3: Scrape Property Details**
Visit each property URL to scrape detailed listing data. This script supports concurrent execution.
```bash
python -m scripts.scraping.scrape_listings
```
*Output: Raw JSON files are saved to `data/raw_houses/`.*

### 2. Run the Data Processing Pipeline
Once data is collected, run the full pipeline to validate, clean, encode, and impute the data into a final CSV.
```bash
python scripts/pipeline/run_pipeline.py
```
*Options:*
- `--skip-validation`: Skip the initial data validation step.
- `--skip-imputation`: Skip the KNN imputation step.
- `--source`: Specify source directory (default: `data/raw_houses`).

### 3. Train and Evaluate Model
Open the Jupyter Notebook to train the XGBoost model on the processed dataset:
```bash
jupyter notebook model/xgboost.ipynb
```
*Note: Ensure you update file paths in the notebook to point to `../data/final_dataset.csv`.*

## Directory Structure

```
MLLM-Housing/
├── data/                       # All data files (Input/Output)
│   ├── raw_houses/             # Raw JSON files containing scraped property details
│   ├── raw_tiles/              # Intermediate search result data (auto-generated)
│   ├── preprocessed_houses/    # Intermediate filtered & cleaned JSONs
│   ├── encoded_houses/         # Intermediate one-hot encoded JSONs
│   ├── imputed_houses/         # Intermediate KNN-imputed JSONs
│   ├── final_dataset.csv       # Final merged and processed dataset for training
│   └── gemini_encoded_descriptions.csv # Pre-computed features from LLM analysis
│
├── model/                      # Model training and artifacts
│   ├── artifacts/              # Saved .pkl models and feature names
│   └── xgboost.ipynb           # Model training and evaluation notebook
│
├── scripts/                    # Executable scripts
│   ├── pipeline/               # Data processing pipeline scripts
│   │   ├── run_pipeline.py     # Main pipeline orchestrator
│   │   ├── preprocess_data.py  # Cleaning and filtering
│   │   ├── encode_features.py  # Categorical encoding
│   │   ├── impute_missing.py   # KNN imputation
│   │   └── json_to_csv.py      # Final conversion
│   ├── scraping/               # Data collection scripts
│   │   ├── generate_links.py   # Step 1: Generate search grid
│   │   ├── extract_urls.py    # Step 2: Extract property URLs
│   │   └── scrape_listings.py  # Step 3: Scrape property details
│   └── analysis/               # Data checking and verification scripts
│
├── utils/                      # Shared utility modules
│   ├── common.py               # Common helpers
│   ├── data_validator.py       # Data validation logic
│   ├── detail_scraper.py       # Core scraping logic (nodriver)
│   └── html_parser.py          # HTML parsing logic
│
├── environment.yml             # Conda environment dependency file
└── README.md                   # Project documentation
```

## Supplementary Documents

### Dataset
- **Source**: Zillow (Scraped via `nodriver`).
- **Processed Data**: `data/final_dataset.csv` (Contains ~4,800 listings with 100+ features).
- **LLM Features**: `data/gemini_encoded_descriptions.csv` (Contains embedding-like features representing qualitative aspects).

### Experiment Logs
- Model performance metrics (MAE, RMSE, R²) are output directly in `model/xgboost.ipynb` after training.
- Scraping progress is logged to the console and tracked via `data/raw_houses/visited_houses.json` (internal state file).
