# Complete Pipeline Documentation: Natural Language Explanation

## Overview
This document provides a comprehensive, natural language explanation of the entire data preprocessing pipeline from raw scraped house data to a model-ready CSV file. Every filtering rule, preprocessing step, transformation, and encoding decision is explained in detail.

---

## Stage 1: Raw Data (`nodriver_houses/`)

**What it is:** Raw JSON files scraped from Zillow, containing all available information about each property.

**Structure:** Nested JSON with sections: `basic_info`, `location`, `financial`, `property_details`, `features`, `schools`, `scores`, `history`, `nearby`, `photos`, `metadata`.

**Status:** Unvalidated, may contain errors, incomplete data, or scraping failures.

---

## Stage 2: Validation (`nodriver_validator.py`)

**Purpose:** Identify houses with structural problems, missing critical data, or scraping failures.

### Validation Rules (Errors - House is Invalid):

1. **File Structure Errors:**
   - File doesn't exist or can't be read
   - File is not valid JSON
   - Root element is not a dictionary

2. **Scraping Failure Indicators:**
   - Contains an "error" field (indicates scraping failure)
   - All scores (walkScore, transitScore, bikeScore) are None when `basic_info` exists (means `__NEXT_DATA__` loaded but HTML score extraction failed)
   - Missing scores section entirely when `basic_info` exists

3. **Missing Critical Sections:**
   - Missing `basic_info` section
   - Missing `location` section
   - Missing `financial` section

4. **Missing Critical Fields:**
   - Missing `basic_info.zpid` (property identifier)
   - Missing `basic_info.address`
   - Missing `basic_info.city`
   - Missing `basic_info.state`
   - Missing `basic_info.zipcode`
   - Missing `location.latitude` OR `location.latitude` is null
   - Missing `location.longitude` OR `location.longitude` is null
   - Missing `financial.lastSoldPrice` OR `financial.lastSoldPrice` is null (this is the target variable!)

5. **Data Type Errors:**
   - `basic_info.zpid` is not an integer
   - `location.latitude` is not numeric
   - `location.longitude` is not numeric
   - `financial.lastSoldPrice` is not numeric

6. **Range Errors:**
   - `location.latitude` is not between -90 and 90
   - `location.longitude` is not between -180 and 180

### Validation Rules (Warnings - House has Issues but is Structurally Valid):

1. **Filename Mismatch:** Filename doesn't match the ZPID in the data
2. **Non-Numeric Values:** `bedrooms`, `bathrooms`, `livingArea` are not numeric
3. **Invalid Values:** `livingArea` is zero or negative
4. **Price Issues:** `lastSoldPrice` or `price` is zero or negative
5. **Score Issues:** Scores are not integers or are outside [0-100] range
6. **Photo Count:** `photoCount` is negative

**Output:** Houses are marked as valid (no errors) or invalid (has errors). Warnings are tracked separately.

---

## Stage 3: Preprocessing (`house_preprocessing.py`)

**Purpose:** Clean the data, remove leaking/noisy features, transform structures, and filter to only SINGLE_FAMILY and CONDO properties.

### Step 1: Filter by Home Type

**Rule:** Only process houses where `basic_info.homeType` is exactly "SINGLE_FAMILY" or "CONDO".

**Why:** These are the only property types we want to model. Other types (e.g., TOWNHOUSE, MULTI_FAMILY) are excluded.

**Implementation:** First categorizes all houses into two lists (single-family and condo), then only processes houses in those lists.

### Step 2: Validation Filter

**Rule:** Only keep houses that pass ALL of the following checks:
1. Passes validator (no errors)
2. Has zero warnings
3. Has non-null `financial.lastSoldPrice` (explicit check) - **CRITICAL: Cannot impute target variable**
4. Has non-null `location.latitude` (explicit check) - **CRITICAL: Cannot impute location**
5. Has non-null `location.longitude` (explicit check) - **CRITICAL: Cannot impute location**
6. ZPID in `basic_info` matches ZPID in `metadata` (if metadata exists)

**Note:** Missing schools or scores are **ALLOWED** and will be imputed later using KNN. We do NOT filter out houses with missing school ratings/distances or missing walk/transit/bike scores.

**Why:** 
- Missing target variable or location makes a house unusable (cannot be imputed reliably)
- Missing schools/scores can be imputed using similar houses (same zipcode, neighborhood, home type, etc.)
- This approach preserves ~3% more data (195 houses) that would otherwise be lost

### Step 3: Remove Entire Sections

**Rule:** Delete these entire sections from every house:
- `history` (price history, tax history)
- `nearby` (nearby homes, neighborhoods, zipcodes, cities)
- `photos` (photo metadata)
- `metadata` (scraping metadata, URLs)

**Why:** These sections contain:
- Leaking information (price history reveals the answer we're trying to predict)
- Noisy/irrelevant data (nearby homes, photos don't help predict price)
- Metadata that's not useful for modeling

### Step 4: Remove Specific Fields (Leaking/Noisy Features)

**Rule:** Remove these specific fields from their respective sections:

**From `basic_info`:**
- `address`, `city`, `state` (location already captured by zipcode/lat/long)
- `zpid` (identifier, not a feature)
- `lotSize` (replaced by normalized `lotSize_sqft`)
- `yearBuilt` (replaced by `house_age`)
- `lotAreaValue` (replaced by normalized `lotSize_sqft`)
- `lotAreaUnits` (used for normalization, then removed)
- `livingAreaUnits` (always "Square Feet", redundant)

**From `location`:**
- `county` (redundant with zipcode)
- `timeZone` (not useful for price prediction)

**From `financial`:**
- `price` (leaking - this is the current listing price, not what we want to predict)
- `zestimate`, `zestimateHighPercent`, `zestimateLowPercent`, `rentZestimate` (Zillow's estimates - leaking information)
- `pricePerSquareFoot` (can be calculated, redundant)
- `hoaFee` (redundant with `monthlyHoaFee`)
- `dateSoldString` (redundant with dateSold, which is also removed)
- `dateSold` (temporal information that could leak)
- `taxAssessedYear` (temporal, not useful)
- `propertyTaxRate` (noisy, varies by jurisdiction)

**From `features`:**
- `associationFee` (redundant with `monthlyHoaFee`)
- `associationFeeIncludes` (too detailed, noisy)
- `hasPetsAllowed` (not relevant for price)
- `hasHomeWarranty` (temporary feature, not permanent)
- `hasLandLease` (rare, noisy)
- `isNewConstruction` (temporal, can leak)

**From `property_details`:**
- `rooms` (array of room details, too complex and often null)
- `interiorFeatures` (list, will be removed entirely later)
- `securityFeatures` (list, will be removed entirely later)
- `cooling` (list, will be removed entirely later)
- `appliances` (list, transformed into score/flags, then removed)
- `architecturalStyle` (too many unique values, noisy)
- `propertyCondition` (subjective, noisy)
- `levels` (redundant with stories)
- `stories` (redundant with levels, noisy)
- `hasCooling` (redundant with cooling list)

### Step 5: Feature Engineering - House Age

**Rule:** Replace `yearBuilt` with `house_age` = 2025 - `yearBuilt`.

**Why:** Age is more interpretable than year built. If `yearBuilt` is null or invalid, `house_age` becomes null.

**Example:** House built in 2000 → `house_age` = 25

### Step 6: Feature Engineering - Lot Size Normalization

**Rule:** Replace `lotAreaValue` and `lotAreaUnits` with `lotSize_sqft`:
- If `homeType` is "CONDO": `lotSize_sqft` = 0 (condos don't have lots)
- If `lotAreaUnits` is "Acres": `lotSize_sqft` = `lotAreaValue` × 43,560
- If `lotAreaUnits` is "Square Feet" or "sqft": `lotSize_sqft` = `lotAreaValue`
- If `lotAreaUnits` is null: Assume square feet
- If `lotAreaValue` is null: `lotSize_sqft` = null

**Why:** Standardize all lot sizes to square feet for consistent modeling.

**Example:** 0.5 acres → 21,780 sqft

### Step 7: Schools Transformation

**Rule:** Transform the `schools` array into 6 flat fields:
- `elementary_school_rating` (1-10, can be null)
- `elementary_school_distance` (miles, can be null)
- `middle_school_rating` (1-10, can be null)
- `middle_school_distance` (miles, can be null)
- `high_school_rating` (1-10, can be null)
- `high_school_distance` (miles, can be null)

**Why:** Arrays are hard to work with in ML. Flat structure is easier. Missing values are allowed and will be imputed later using KNN.

**Implementation:** Finds school by `level` field, extracts `rating` and `distance`. If a school level is missing or has null rating/distance, the corresponding field is set to `None`. This allows the house to pass through preprocessing and be imputed later.

### Step 8: Appliances Transformation

**Rule:** Transform the `appliances` list into:
1. **`standard_appliance_score`** (0-6): Count of standard appliance categories present:
   - Laundry (Washer, Dryer, etc.)
   - Dishwasher
   - Refrigerator
   - Oven/Range
   - Disposal
   - Microwave
   
   Each category found adds 1 to the score (max 6).

2. **Luxury flags** (binary, 0 or 1):
   - `has_wine_refrigerator`
   - `has_double_oven`
   - `has_built_in_refrigerator`
   - `has_warming_drawer`

**Why:** Lists are hard to work with. A score captures standard amenities, flags capture luxury features. If `appliances` is missing or empty, all features default to 0.

**Example:** `["Dishwasher", "Microwave", "Wine Refrigerator"]` → `standard_appliance_score` = 2, `has_wine_refrigerator` = 1

**Important:** Even if `appliances` field is missing, all 5 appliance features are always created (with 0 values).

### Step 9: Save Cleaned Data

**Output:** `filtered_houses/` directory with cleaned JSON files. Each file has:
- Only SINGLE_FAMILY or CONDO properties
- No leaking/noisy features
- Transformed features (house_age, lotSize_sqft, schools, appliances)
- All required fields present and non-null

---

## Stage 4: One-Hot Encoding (`one_hot_encode_houses.py`)

**Purpose:** Convert categorical features into binary columns, handle null values, and flatten the nested structure.

### Step 1: Filter Critical Nulls

**Rule:** Skip houses with:
- Null `financial.lastSoldPrice` (target variable)
- Null `location.latitude` or `location.longitude` (critical location)

**Why:** These are unfixable - we can't train without the target or location.

### Step 2: Remove Features (Again)

**Rule:** Remove these features if they still exist (safety check):
- `property_details.interiorFeatures`
- `property_details.securityFeatures`
- `property_details.cooling`
- `property_details.appliances` (should already be transformed, but remove if present)

**Why:** These were removed in preprocessing, but some might slip through. Lists are not useful for ML.

### Step 3: One-Hot Encode Categorical Features

**Rule:** Convert categorical features into binary columns:

**`neighborhood` (Top 20 + Other):**
- Create columns: `location_neighborhood_{value}` for top 20 neighborhoods
- Create column: `location_neighborhood_Other` for all other neighborhoods
- Each house gets exactly one "1" and the rest are "0"

**`heating` (Top 5 + Other):**
- Create columns: `property_details_heating_{value}` for top 5 heating types
- Create column: `property_details_heating_Other` for all others
- Example: "Central" → `property_details_heating_Central` = 1, all others = 0

**`flooring` (Top 4 + Other):**
- Create columns: `property_details_flooring_{value}` for top 4 flooring types
- Create column: `property_details_flooring_Other` for all others

**`parkingFeatures` (Top 5 + Other):**
- Create columns: `property_details_parkingFeatures_{value}` for top 5 parking features
- Create column: `property_details_parkingFeatures_Other` for all others
- Note: `parkingFeatures` is a list, so we check if any item in the list matches

**`exteriorFeatures` (Top 3 + Other):**
- Create columns: `property_details_exteriorFeatures_{value}` for top 3 exterior features
- Create column: `property_details_exteriorFeatures_Other` for all others
- Note: `exteriorFeatures` is a list, so we check if any item in the list matches

**`homeType` (All values, no Other):**
- Create columns: `basic_info_homeType_{value}` for each unique homeType
- No "Other" column (we only have SINGLE_FAMILY and CONDO)
- Example: "SINGLE_FAMILY" → `basic_info_homeType_SINGLE_FAMILY` = 1, `basic_info_homeType_CONDO` = 0

**`zipcode` (All values, no Other):**
- Create columns: `basic_info_zipcode_{value}` for each unique zipcode
- No "Other" column (all zipcodes are kept)
- Example: "94117" → `basic_info_zipcode_94117` = 1, all other zipcodes = 0

**Why:** Machine learning models need numeric inputs. One-hot encoding converts categories into binary features.

### Step 4: Normalize Boolean Fields

**Rule:** Convert boolean fields to 0/1:
- `property_details.hasView`: True → 1, False/None → 0
- `property_details.hasSpa`: True → 1, False/None → 0
- `property_details.hasFireplace`: True → 1, False/None → 0
- `property_details.hasGarage`: True → 1, False/None → 0
- `property_details.hasHeating`: True → 1, False/None → 0
- `features.hasAssociation`: True → 1, False/None → 0

**Why:** Consistent numeric representation. None/False both mean "not present" = 0.

### Step 5: Handle Null Values (Phase 2 - Simple Imputation)

**Rule:** Fill nulls with 0 for these features:
- `financial.monthlyHoaFee` → 0 (null means no HOA)
- `property_details.fireplaces` → 0 (null means no fireplaces)
- `property_details.garageParkingCapacity` → 0 (null means no garage capacity)
- `features.numberOfUnitsInCommunity` → 0 (null means not applicable)

**Why:** These have logical defaults. Null = absence = 0.

**Note:** `hasSpa`, `hasView`, `hasFireplace` are already handled by boolean normalization (Step 4).

### Step 6: Handle taxAssessedValue (Phase 3 - Impute with Flagging)

**Rule:** For `financial.taxAssessedValue`:
- If null: Create `financial.taxAssessedValue_is_missing` = 1, fill `taxAssessedValue` with median
- If not null: Create `financial.taxAssessedValue_is_missing` = 0, keep original value

**Why:** Missingness itself is informative. Flag tells the model "this value was imputed" while median provides a reasonable estimate.

### Step 7: Ensure Appliance Features Exist

**Rule:** If any appliance features are missing, create them with value 0:
- `property_details.standard_appliance_score` → 0
- `property_details.has_built_in_refrigerator` → 0
- `property_details.has_double_oven` → 0
- `property_details.has_warming_drawer` → 0
- `property_details.has_wine_refrigerator` → 0

**Why:** Some houses might have been processed before the preprocessing fix. This ensures all houses have these features.

### Step 8: Flatten Structure

**Rule:** Convert nested dictionary structure into flat key-value pairs:
- `basic_info.bedrooms` → `basic_info_bedrooms`
- `location.latitude` → `location_latitude`
- `financial.lastSoldPrice` → `financial_lastSoldPrice`
- `property_details.hasGarage` → `property_details_hasGarage`

**Why:** Machine learning libraries expect flat structures (one column per feature).

**Output:** `houses_onehot_encoded/` directory with flattened JSON files. Each file has:
- All categorical features one-hot encoded
- All boolean features normalized to 0/1
- All nulls filled (Phase 2 & 3)
- Flat structure (no nesting)

---

## Stage 5: KNN Imputation (`house_imputation.py`)

**Purpose:** Fill missing values in core numerical features using K-Nearest Neighbors.

### Step 1: Filter Critical Nulls (Safety Check)

**Rule:** Remove rows with:
- Null `financial_lastSoldPrice` (target variable)
- Null `location_latitude` or `location_longitude` (critical location)

**Why:** These should have been filtered earlier, but this is a safety net.

### Step 2: Identify Feature Categories

**Rule:** Categorize features into:

**Phase 4 Features (To Impute):**
- `basic_info_bedrooms`
- `basic_info_bathrooms`
- `basic_info_livingArea`
- `basic_info_lotSize_sqft`
- `basic_info_house_age`
- `scores_walkScore`
- `scores_transitScore`
- `scores_bikeScore`
- `schools_elementary_school_rating`
- `schools_elementary_school_distance`
- `schools_middle_school_rating`
- `schools_middle_school_distance`
- `schools_high_school_rating`
- `schools_high_school_distance`

**Features Used for Distance (Not Imputed):**
- All zipcode one-hot columns (`basic_info_zipcode_*`)
- All neighborhood one-hot columns (`location_neighborhood_*`)
- All homeType one-hot columns (`basic_info_homeType_*`)
- All heating one-hot columns (`property_details_heating_*`)
- All flooring one-hot columns (`property_details_flooring_*`)
- All parkingFeatures one-hot columns (`property_details_parkingFeatures_*`)
- All exteriorFeatures one-hot columns (`property_details_exteriorFeatures_*`)
- Other numeric features: `location_latitude`, `location_longitude`, `financial_lastSoldPrice`, `financial_monthlyHoaFee`, `financial_taxAssessedValue`, `financial_taxAssessedValue_is_missing`, `property_details_fireplaces`, `property_details_garageParkingCapacity`, `property_details_hasFireplace`, `property_details_hasGarage`, `property_details_hasHeating`, `property_details_hasSpa`, `property_details_hasView`, `property_details_standard_appliance_score`, `features_hasAssociation`, `features_numberOfUnitsInCommunity`

**Why:** We use location and other complete features to find similar houses, then impute missing Phase 4 features from those neighbors.

### Step 3: KNN Imputation

**Rule:** For each house with missing Phase 4 features:
1. Scale all features (Phase 4 + distance features) using MinMaxScaler (0-1 range)
2. Find K=5 nearest neighbors using weighted distance (closer neighbors have more influence)
3. Impute missing values as weighted average of neighbors' values
4. Unscale back to original scale

**Why:** KNN uses similar houses to estimate missing values. Scaling ensures all features contribute equally to distance. Weighted distance gives more influence to closer neighbors.

**Parameters:**
- K = 5 neighbors
- Weights = 'distance' (inverse distance weighting)
- Scaler = MinMaxScaler (0-1 range)

### Step 4: Post-Processing (Enforce Logical Constraints)

**Rule:** After imputation, enforce these constraints:

**`bedrooms`:**
- Round to nearest integer
- If negative, set to 0

**`bathrooms`:**
- Round to nearest 0.5 increment (0, 0.5, 1, 1.5, 2, ...)
- If negative, set to 0

**`livingArea`:**
- If non-positive (≤0), set to 1 (minimum valid living area)

**`lotSize_sqft`:**
- If negative, set to 0

**`house_age`:**
- If negative, set to 0

**Scores (walkScore, transitScore, bikeScore):**
- Clip to [0, 100] range

**School Ratings:**
- Clip to [1, 10] range

**School Distances:**
- If negative, set to 0

**Why:** Imputed values might not make logical sense. These constraints ensure valid values (e.g., can't have -2 bedrooms, bathrooms must be in 0.5 increments).

**Note:** We DON'T clip extreme values for bedrooms, bathrooms, livingArea, lotSize_sqft, or house_age because San Francisco has valid outliers (e.g., mansions with 10+ bedrooms).

### Step 5: Save Artifacts

**Rule:** Save:
- `knn_scaler.pkl`: The fitted MinMaxScaler (for inference)
- `knn_metadata.json`: Feature names, KNN parameters, Phase 4 features list

**Why:** Need these for inference on new houses.

**Output:** `houses_imputed/` directory with complete JSON files. Each file has:
- All Phase 4 features filled (no nulls)
- All values within logical constraints
- Same flat structure as before

---

## Stage 6: CSV Conversion (`convert_to_single_file.py`)

**Purpose:** Convert all individual JSON files into a single CSV file.

### Step 1: Load All Houses

**Rule:** Load all JSON files from `houses_imputed/` into a pandas DataFrame.

**Exclusions:** Skip metadata files (`knn_metadata.json`, `issues.json`, `visited_houses.json`).

### Step 2: Final Safety Checks

**Rule:** Remove rows with:
- Null `financial_lastSoldPrice` (target variable)
- Null `location_latitude` or `location_longitude` (critical location)

**Why:** Final safety net - shouldn't happen, but ensures clean output.

### Step 3: Remove Empty Columns

**Rule:** Remove columns that are entirely NaN (no valid values in any row).

**Why:** Some removed features might appear as empty columns.

### Step 4: Sort Columns

**Rule:** Sort columns alphabetically for consistency.

**Output:** `houses_dataset.csv` - Single CSV file with:
- One row per house
- One column per feature
- No missing values in Phase 4 features
- Ready for machine learning

---

## Summary: Complete Pipeline Flow

```
nodriver_houses/ (raw scraped data)
    ↓
[nodriver_validator.py] - Validates structure, catches errors
    ↓
[house_preprocessing.py]
    - Filters: Only SINGLE_FAMILY and CONDO
    - Filters: Only houses with no errors/warnings
    - Filters: Only houses with non-null lastSoldPrice/latitude/longitude (critical fields)
    - Allows: Houses with missing schools/scores (will be imputed later)
    - Removes: Entire sections (history, nearby, photos, metadata)
    - Removes: 38 specific leaking/noisy fields
    - Transforms: yearBuilt → house_age
    - Transforms: lotAreaValue/Units → lotSize_sqft
    - Transforms: schools array → 6 flat fields (nulls allowed)
    - Transforms: appliances list → score + 4 flags
    ↓
filtered_houses/ (cleaned, validated, transformed)
    ↓
[one_hot_encode_houses.py]
    - Filters: Removes houses with null lastSoldPrice/location
    - Encodes: 6 categorical features → one-hot columns
    - Normalizes: 6 boolean fields → 0/1
    - Imputes: Phase 2 & 3 nulls (simple defaults + taxAssessedValue)
    - Flattens: Nested structure → flat key-value pairs
    ↓
houses_onehot_encoded/ (flattened, encoded, partially imputed)
    ↓
[house_imputation.py]
    - Filters: Safety check for null lastSoldPrice/location
    - Imputes: Phase 4 features using KNN (14 features)
    - Post-processes: Enforces logical constraints
    - Saves: Scaler and metadata for inference
    ↓
houses_imputed/ (complete, no missing Phase 4 features)
    ↓
[convert_to_single_file.py]
    - Filters: Final safety check
    - Converts: JSON files → single CSV
    - Cleans: Removes empty columns
    ↓
houses_dataset.csv (model-ready)
```

---

## Feature Counts

- **Raw features** (nodriver_houses): ~100+ nested features
- **After preprocessing** (filtered_houses): ~50 features (removed leaking/noisy)
- **After one-hot encoding** (houses_onehot_encoded): ~110+ features (flattened, one-hot encoded)
- **After imputation** (houses_imputed): ~110+ features (all Phase 4 features complete)
- **Final CSV** (houses_dataset.csv): ~109 features (after removing empty columns)

---

## Key Design Decisions

1. **Why filter to SINGLE_FAMILY and CONDO only?**
   - These are the most common property types
   - Other types have different pricing dynamics
   - Simplifies the model

2. **Why remove so many features?**
   - Some leak information (price history, zestimate)
   - Some are too noisy (architectural style, property condition)
   - Some are redundant (address when we have zipcode/lat/long)

3. **Why one-hot encode instead of label encoding?**
   - One-hot preserves no-ordering assumption (zipcode 94117 isn't "greater than" 94116)
   - Label encoding implies ordering, which is wrong for categories

4. **Why KNN imputation instead of mean/median?**
   - KNN uses similar houses (by location, type, etc.) to estimate missing values
   - Mean/median ignores house similarity
   - More accurate for correlated features (e.g., bedrooms and bathrooms)

5. **Why not clip extreme values?**
   - San Francisco has valid outliers (mansions, penthouses)
   - Clipping would lose important signal
   - Model should learn to handle outliers

---

## Validation Checklist

✅ All filtering rules are implemented  
✅ All field removals are implemented  
✅ All transformations are implemented  
✅ All one-hot encoding rules are implemented  
✅ All null handling is implemented  
✅ All post-processing constraints are implemented  
✅ Safety checks at each stage  
✅ Artifacts saved for inference  

