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
- **Validation**: Checks structure and content of scraped files.
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

### 1. Run the Data Processing Pipeline
To process raw scraped data into the final dataset:
```bash
python run_full_pipeline.py
```
*Options:*
- `--skip-validation`: Skip the initial data validation step.
- `--skip-imputation`: Skip the KNN imputation step.

### 2. Launch the Scraper
To start collecting property details (concurrently safe):
```bash
python nodriver_detail_scraper_concurrent.py
```

### 3. Train and Evaluate Model
Open the Jupyter Notebook to train the XGBoost model:
```bash
jupyter notebook xgboost.ipynb
```
*Follow the cells to load data, merge LLM features, train the model, and view evaluation metrics.*

## Directory Structure

```
MLLM-Housing/
├── data_checking/              # Scripts for validating and verifying data integrity
├── models/                     # Directory for saving trained XGBoost models
├── nodriver_houses/            # Raw JSON files containing scraped property details
├── nodriver_results/           # Raw tile results from initial search
├── houses_dataset.csv          # Final merged and processed dataset for training
├── gemini_encoded_descriptions.csv # Pre-computed features from LLM analysis
├── run_full_pipeline.py        # Main script to orchestrate data processing
├── nodriver_detail_scraper_concurrent.py # Main scraping script
├── house_preprocessing.py      # Data cleaning logic
├── house_imputation.py         # Missing value imputation logic
├── one_hot_encode_houses.py    # Categorical encoding logic
├── xgboost.ipynb               # Model training and evaluation notebook
├── environment.yml             # Conda environment dependency file
└── README.md                   # Project documentation
```

## Supplementary Documents

### Dataset
- **Source**: Zillow (Scraped via `nodriver`).
- **Processed Data**: `houses_dataset.csv` (Contains ~4,800 listings with 100+ features).
- **LLM Features**: `gemini_encoded_descriptions.csv` (Contains embedding-like features representing qualitative aspects).

### Experiment Logs
- Model performance metrics (MAE, RMSE, R²) are output directly in `xgboost.ipynb` after training.
- Scraping progress is logged to the console and tracked via `visited_houses.json` (internal state file).
