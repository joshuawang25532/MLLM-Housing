# MLLM-Housing

## Project Overview
MLLM-Housing predicts San Francisco housing sale prices with XGBoost and tests whether qualitative features extracted by an LLM add anything on top of traditional structured listing data.

The project scrapes listing data from Zillow, runs it through a cleaning, encoding and imputation pipeline, and asks Gemini to score each listing's text description on five qualitative dimensions (luxury, renovation, distress, marketing hype, location premium). The model notebook then compares XGBoost with and without those scores on identical splits.

> **Scope note:** the LLM features currently come from listing **text only**. The scraped data includes listing photos, but image-based (multimodal) features are not implemented yet; that is the natural next step and the reason for the project name.

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
- **Feature Engineering**: Joins structured data with LLM-derived features (`gemini_encoded_descriptions.csv`) on the listing ID (`zpid`).
- **Leakage guard**: The sale price (the target) is excluded from the features KNN imputation uses to find neighbours, and imputation is deterministic run to run.

### 3. Predictive Modeling
- **Baseline vs. LLM features**: Two XGBoost models (structured features only, and structured + Gemini scores), each tuned separately with RandomizedSearchCV on the training split.
- **Evaluation**: Paired 5x3 repeated K-fold CV on the training split, then a single held-out test set. Reports log-RMSE plus dollar MAE, MAPE, median APE and R². Results are written to `results.txt`.

**Current results** (4,853 SF listings, 80/20 split):

| Model | CV RMSE (log) | Test RMSE (log) | Test MAE | Test MAPE | Test R² |
|---|---|---|---|---|---|
| XGBoost (baseline) | 0.3792 ± 0.0725 | 0.2606 | $368,342 | 18.9% | 0.732 |
| XGBoost + Gemini | 0.3719 ± 0.0708 | 0.2585 | $356,049 | 18.5% | 0.748 |

The Gemini features give a small but consistent improvement: about 2% lower log-RMSE, better in 14 of 15 paired CV folds. Most of the predictive power still comes from the structured features.

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
├── environment.yml             # Conda environment dependency file
└── README.md                   # Project documentation
```

## Supplementary Documents

### Dataset
- **Source**: Zillow (Scraped via `nodriver`).
- **Processed Data**: `data/final_dataset.csv` (4,853 listings, ~110 columns, keyed by `zpid`).
- **LLM Features**: `data/gemini_encoded_descriptions.csv` (5 scalar scores per listing from `model/description.ipynb`, keyed by `zpid`; empty for the 25 listings with no description).

### Experiment Logs
- Model comparison (baseline vs. + Gemini) is written to `results.txt` by `model/xgboost.ipynb`.
- Scraping progress is logged to the console and tracked via `data/raw_houses/visited_houses.json` (internal state file).
