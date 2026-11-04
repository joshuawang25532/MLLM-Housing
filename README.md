# MLLM-Housing

## Project Overview
MLLM-Housing predicts San Francisco housing sale prices with XGBoost and tests whether qualitative features extracted by an LLM add anything on top of traditional structured listing data.

The project scrapes listing data from Zillow, runs it through a cleaning, encoding and imputation pipeline, and asks Gemini to score each listing's text description on five qualitative dimensions (luxury, renovation, distress, marketing hype, location premium). The model notebook then compares XGBoost with and without those scores on identical splits.

> **Scope note:** the LLM features currently come from listing **text only**. The scraped data includes listing photos, but image-based (multimodal) features are not implemented yet; that is the natural next step and the reason for the project name.

## Core Functions

### 1. Advanced Data Collection (Scraping)
The project implements a browser-automation scraper built on `nodriver` that drives a real Chrome instance, paced with randomized delays to keep request rates low.
- **Concurrent Scraping**: Supports running multiple scraper instances simultaneously to maximize throughput.
- **Resilience**: Implements retry logic, rate limiting (Gaussian-distributed delays), and error handling for network issues.
- **Data Integrity**: Validates scraped data against expected schemas to ensure quality.

### 2. Data Processing Pipeline
A unified pipeline orchestrates the transformation of raw JSON data into a model-ready CSV dataset.
- **Validation**: Checks structure and content of scraped files using `utils/data_validator.py`.
- **Preprocessing**: Cleans raw fields, handles missing values, and normalizes data.
- **One-Hot Encoding**: Converts categorical variables for machine learning compatibility.
- **Imputation**: Uses KNN imputation to fill missing numerical data.
- **Feature Engineering**: Joins structured data with LLM-derived features (`gemini_encoded_descriptions.csv`) on the listing ID (`zpid`).
- **Leakage guard**: The sale price (the target) is excluded from the features KNN imputation uses to find neighbours, and imputation is deterministic run to run. Listing-side price signals (list price, Zestimate, price per sq ft, price history) are dropped. California reassesses a home at its sale price, so a tax assessment from a later year than the sale is treated as missing.

### 3. Predictive Modeling
- **Baseline vs. LLM features**: Two XGBoost models (structured features only, and structured + Gemini scores), each tuned separately with RandomizedSearchCV on the training split.
- **Evaluation**: Paired 5x3 repeated K-fold CV on the training split, then a single held-out test set. Reports log-RMSE plus dollar MAE, MAPE, median APE and R². Results are written to `results.txt`.

**Current results** (4,838 SF sales after removing 15 non-market transfers under $100k; 80/20 split):

| Model | CV RMSE (log) | Test RMSE (log) | Test MAE | Test MAPE | Test median APE | Test R² ($) |
|---|---|---|---|---|---|---|
| XGBoost (baseline) | 0.2544 ± 0.0176 | 0.2619 | $364,344 | 18.5% | 11.4% | 0.641 |
| XGBoost + Gemini | 0.2456 ± 0.0179 | 0.2596 | $355,639 | 18.1% | 11.0% | 0.666 |

The Gemini features give a small but consistent improvement: about 3.5% lower CV log-RMSE, better in all 15 paired CV folds, and about $8.7k lower test MAE. Most of the predictive power still comes from the structured features.

### Limitations
- **Text only:** the LLM features come from listing descriptions; photos are scraped but not yet used.
- **Small effect, one market:** the gain is modest and measured on a single city (San Francisco) over one scrape.
- **Dollar R² is outlier-sensitive:** a few very expensive sales (up to $44M) dominate dollar-scale R², so log-RMSE and median APE are the more reliable summaries.
- **Imputation is fit on all rows:** KNN imputation never sees the sale price, but it does use other rows' features (including test rows) to fill gaps. Moving it into a train-only sklearn `Pipeline` would remove that last bit of overlap.
- **Gemini scores are a snapshot:** they were generated once with `gemini-flash-lite-latest`. That run had Google Search grounding switched on; `model/description.ipynb` now scores from the description text only (no tools, temperature 0), so re-running it may give slightly different scores.

## Collaboration Scheme Summary
The project features a **Concurrent Scraping Architecture** designed for scalability:
- **File Locking**: Uses platform-specific locking (fcntl/msvcrt) to safely manage shared resources like `visited_houses.json` across multiple processes.
- **Atomic Operations**: Ensures data consistency during file writes.
- **Process Isolation**: Each scraper instance maintains its own browser context and "canary" files to prevent conflicts.
- **Distributed Work**: Multiple scraper scripts can run in parallel on the same machine or across a network (sharing a filesystem) to accelerate data collection.

## Environment Setup

Dependencies are pinned in `requirements.txt`; `environment.yml` wraps them for conda. Works on macOS, Linux and Windows.

**With conda:**
```bash
conda env create -f environment.yml
conda activate MLLMHousing
```

**Or with pip** (Python 3.10):
```bash
pip install -r requirements.txt
```

**Run the tests:**
```bash
pytest
```
The tests check that preprocessing drops tax assessments made after the sale, that imputation never uses the sale price and is deterministic, that the final CSV is keyed by `zpid`, and that the committed datasets join correctly and respect the Gemini scoring rubric. They run in GitHub Actions on Ubuntu and Windows.

## Quick-Start Commands

To reproduce the results, open the model notebook and run all cells (roughly 10-15 minutes on a laptop, mostly the two hyperparameter searches). It only needs the two committed tables, `data/final_dataset.csv` and `data/gemini_encoded_descriptions.csv`:
```bash
jupyter notebook model/xgboost.ipynb
```
Alternatively, start from the beginning. Raw and intermediate data (~1 GB of scraped JSON) is not committed, so the full pipeline starts with data collection.

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

## Directory Structure

```
MLLM-Housing/
├── data/                       # All data files (only the two CSVs are committed)
│   ├── raw_houses/             # Raw JSON files containing scraped property details (generated)
│   ├── raw_tiles/              # Intermediate search result data (generated)
│   ├── preprocessed_houses/    # Intermediate filtered & cleaned JSONs (generated)
│   ├── encoded_houses/         # Intermediate one-hot encoded JSONs (generated)
│   ├── imputed_houses/         # Intermediate KNN-imputed JSONs (generated)
│   ├── final_dataset.csv       # Final processed dataset for training, keyed by zpid
│   └── gemini_encoded_descriptions.csv # Gemini scores per listing, keyed by zpid
│
├── model/                      # Modeling notebooks
│   ├── description.ipynb       # Gemini scoring of listing descriptions
│   └── xgboost.ipynb           # Baseline vs. + Gemini model comparison
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
├── tests/                      # pytest suite (imputation, CSV conversion, dataset contracts)
├── .github/workflows/tests.yml # CI: runs pytest on Ubuntu and Windows
├── requirements.txt            # Pinned dependencies
├── environment.yml             # Conda wrapper around requirements.txt
├── results.txt                 # Latest model comparison, written by model/xgboost.ipynb
└── README.md                   # Project documentation
```

## Supplementary Documents

### Dataset
- **Source**: Zillow (Scraped via `nodriver`).
- **Processed Data**: `data/final_dataset.csv` (4,853 listings, ~110 columns, keyed by `zpid`; the model drops 15 sales under $100k).
- **LLM Features**: `data/gemini_encoded_descriptions.csv` (5 scalar scores per listing from `model/description.ipynb`, keyed by `zpid`; empty for the 25 listings with no description).

### Experiment Logs
- Model comparison (baseline vs. + Gemini) is written to `results.txt` by `model/xgboost.ipynb`.
- Scraping progress is logged to the console and tracked via `data/raw_houses/visited_houses.json` (internal state file).
