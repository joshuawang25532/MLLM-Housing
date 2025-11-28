# ML Pipeline Summary: From Raw Scraping to Model-Ready Data

## Overview
This document describes the complete data preprocessing pipeline from raw scraped house data to a single, model-ready dataset.

## Pipeline Stages

### Stage 1: Raw Data (`nodriver_houses/`)
- **Input**: Raw JSON files scraped from Zillow
- **Format**: Nested JSON structure with sections: `basic_info`, `location`, `financial`, `property_details`, `features`, `schools`, `scores`, `history`, `nearby`, `photos`, `metadata`
- **Status**: Unvalidated, may contain errors or incomplete data

### Stage 2: Validation (`nodriver_validator.py`)
- **Purpose**: Validate all houses and identify issues
- **Output**: 
  - Console report with error/warning statistics
  - `issues.json` (optional) - List of houses with issues and their URLs
- **Key Validations**:
  - JSON structure validity
  - Required sections present (`basic_info`, `location`, `financial`)
  - Required fields present (zpid, address, city, state, zipcode)
  - Data type checks (coordinates, prices, areas)
  - Score completeness (walkScore, transitScore, bikeScore)
- **Usage**: `python3 nodriver_validator.py [--output-issues issues.json]`

### Stage 3: Preprocessing (`house_preprocessing.py`)
- **Input**: `nodriver_houses/` (raw scraped data)
- **Output**: `filtered_houses/` (cleaned, validated, transformed)
- **Key Operations**:
  1. **Filter**: Only keep `SINGLE_FAMILY` and `CONDO` home types
  2. **Validate**: Skip houses with any errors or warnings (from validator)
  3. **Remove Features**:
     - Entire sections: `history`, `nearby`, `photos`, `metadata`
     - Leaking features: `address`, `city`, `state`, `zpid`, `price`, `zestimate`, etc.
     - Noisy features: `architecturalStyle`, `propertyCondition`, `levels`, `stories`, etc.
  4. **Transform**:
     - `yearBuilt` → `house_age` (2025 - yearBuilt)
     - `lotAreaValue` + `lotAreaUnits` → `lotSize_sqft` (normalized to square feet)
     - `appliances` → `standard_appliance_score` (0-6) + luxury flags
     - `schools` array → flat structure (6 features: rating + distance for each level)
  5. **Validate Consistency**:
     - ZPID matches between `basic_info` and `metadata`
     - **Note**: Missing schools/scores are allowed and will be imputed later using KNN
- **Usage**: `python3 house_preprocessing.py`

### Stage 4: One-Hot Encoding (`one_hot_encode_houses.py`)
- **Input**: `filtered_houses/` (preprocessed data)
- **Output**: `houses_onehot_encoded/` (flattened, one-hot encoded)
- **Key Operations**:
  1. **Row Filtering** (Phase 1):
     - Remove rows with null `lastSoldPrice` (target variable)
     - Remove rows with null `latitude` or `longitude`
  2. **Simple Imputation** (Phase 2):
     - `monthlyHoaFee` → 0 if null
     - `hasSpa`, `hasView`, `hasFireplace` → 0 if null
     - `fireplaces`, `garageParkingCapacity` → 0 if null
  3. **Impute with Flagging** (Phase 3):
     - `taxAssessedValue` → median + `taxAssessedValue_is_missing` flag
  4. **Features to Ignore** (Phase 5):
     - `hasAssociation` → 0 if null
     - `numberOfUnitsInCommunity` → 0 if null
  5. **One-Hot Encoding**:
     - `homeType`: All values (SINGLE_FAMILY, CONDO)
     - `zipcode`: All values (~30 zipcodes)
     - `neighborhood`: Top 20 + Other
     - `heating`: Top 5 + Other
     - `flooring`: Top 4 + Other
     - `parkingFeatures`: Top 5 + Other
     - `exteriorFeatures`: Top 3 + Other
  6. **Boolean Normalization**: True → 1, False/None → 0
  7. **Flattening**: Convert nested structure to flat dictionary (all keys at top level)
- **Usage**: `python3 one_hot_encode_houses.py [--source filtered_houses] [--dest houses_onehot_encoded]`

### Stage 5: KNN Imputation (`house_imputation.py`)
- **Input**: `houses_onehot_encoded/` (one-hot encoded, flattened data)
- **Output**: `houses_imputed/` (complete data with no missing Phase 4 features)
- **Key Operations**:
  1. **Load & Filter**: Load houses, filter out removed features
  2. **KNN Imputation** (Phase 4):
     - Impute: `bedrooms`, `bathrooms`, `livingArea`, `lotSize_sqft`, `house_age`
     - Impute: `walkScore`, `transitScore`, `bikeScore`
     - Impute: All 6 school rating/distance features
     - Uses MinMaxScaler for feature scaling
     - Uses weighted KNN (distance-weighted, K=5 by default)
     - Includes one-hot encoded features in distance calculation
  3. **Post-Processing**:
     - Bedrooms: Round to nearest integer, negatives → 0
     - Bathrooms: Round to nearest 0.5 increment, negatives → 0
     - LivingArea: Non-positive → 1
     - LotSize: Negatives → 0
     - Scores: Clip to [0, 100]
     - School ratings: Clip to [1, 10]
  4. **Save Artifacts**: `knn_scaler.pkl` and `knn_metadata.json` for inference
- **Usage**: `python3 house_imputation.py [--source houses_onehot_encoded] [--dest houses_imputed] [--k 5]`

### Stage 6: Single File Conversion (`convert_to_single_file.py`)
- **Input**: `houses_imputed/` (individual JSON files)
- **Output**: `houses_dataset.csv` (single CSV file for ML)
- **Purpose**: Convert all houses into a single file for easy loading into pandas/sklearn
- **Usage**: `python3 convert_to_single_file.py [--source houses_imputed] [--output houses_dataset.csv]`

## Complete Pipeline Flow

```
nodriver_houses/
    ↓ (nodriver_validator.py - optional validation)
    ↓ (house_preprocessing.py)
filtered_houses/
    ↓ (one_hot_encode_houses.py)
houses_onehot_encoded/
    ↓ (house_imputation.py)
houses_imputed/
    ↓ (convert_to_single_file.py)
houses_dataset.csv
```

## Feature Counts (Approximate)

- **Raw features** (nodriver_houses): ~100+ nested features
- **After preprocessing** (filtered_houses): ~50 features (removed leaking/noisy)
- **After one-hot encoding** (houses_onehot_encoded): ~110+ features (flattened, one-hot encoded)
- **After imputation** (houses_imputed): ~110+ features (all Phase 4 features complete)
- **Final dataset** (houses_dataset.csv): ~110+ columns, ~4,850 rows

## Key Decisions

1. **Home Types**: Only `SINGLE_FAMILY` and `CONDO` (removed other types)
2. **Row Removal**: Houses with missing target (`lastSoldPrice`) or location (`latitude`/`longitude`) are removed
3. **Null Handling**: Multi-phase strategy (simple imputation → flagging → KNN)
4. **Scaling**: MinMaxScaler (0-1 range) for KNN imputation
5. **One-Hot Encoding**: Top N values + "Other" for high-cardinality features
6. **Extreme Values**: Not clipped (SF has valid outliers like 15+ bedrooms)

## Output Files

- `filtered_houses/*.json`: Preprocessed houses (SINGLE_FAMILY + CONDO only)
- `houses_onehot_encoded/*.json`: One-hot encoded, flattened houses
- `houses_imputed/*.json`: Complete houses with imputed Phase 4 features
- `houses_imputed/knn_scaler.pkl`: Scaler for inference
- `houses_imputed/knn_metadata.json`: Metadata for inference
- `houses_dataset.csv`: Single CSV file with all houses (ready for ML)

