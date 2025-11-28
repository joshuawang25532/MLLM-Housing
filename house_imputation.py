"""
KNN Imputation for Real Estate Features

Applies K-Nearest Neighbors imputation to handle missing values in core property features.
This script works on the output of one_hot_encode_houses.py (flattened, one-hot encoded data).

Phase 4 Features (Imputed with KNN):
- bedrooms, bathrooms, livingArea, lotSize_sqft, house_age
- walkScore, transitScore, bikeScore
- All 6 school rating/distance features

Strategy:
1. Uses one-hot encoded location features (zipcode, neighborhood) in distance calculation
2. Scales all features using StandardScaler before KNN
3. Applies weighted KNN (closer neighbors = more influence)
4. Post-processes to enforce logical constraints (integer bedrooms, 0.5-increment bathrooms)
"""

import json
import numpy as np
import pandas as pd
import pickle
from pathlib import Path
from sklearn.impute import KNNImputer
from sklearn.preprocessing import MinMaxScaler
from typing import Dict, List, Tuple
import argparse


def get_removed_features() -> List[str]:
    """
    Get list of features that should have been removed in preprocessing.
    These should not exist in one-hot encoded output, but pandas may create
    columns for them if they appear in any file.
    
    Returns:
        List of feature names (flattened format) that should be removed
    """
    # Features removed in preprocessing (in flattened format)
    removed = [
        # basic_info removed features
        'basic_info_address',
        'basic_info_city',
        'basic_info_state',
        'basic_info_zpid',
        'basic_info_lotSize',
        'basic_info_yearBuilt',
        'basic_info_lotAreaValue',
        'basic_info_lotAreaUnits',
        'basic_info_livingAreaUnits',
        # location removed features
        'location_county',
        'location_timeZone',
        # financial removed features
        'financial_price',
        'financial_zestimate',
        'financial_zestimateHighPercent',
        'financial_zestimateLowPercent',
        'financial_rentZestimate',
        'financial_pricePerSquareFoot',
        'financial_hoaFee',
        'financial_dateSoldString',
        'financial_dateSold',
        'financial_taxAssessedYear',
        'financial_propertyTaxRate',
        # features removed features
        'features_associationFee',
        'features_associationFeeIncludes',
        'features_hasPetsAllowed',
        'features_hasHomeWarranty',
        'features_hasLandLease',
        'features_isNewConstruction',
        # property_details removed features
        'property_details_rooms',
        'property_details_interiorFeatures',
        'property_details_securityFeatures',
        'property_details_cooling',
        'property_details_appliances',
        'property_details_architecturalStyle',
        'property_details_propertyCondition',
        'property_details_levels',
        'property_details_stories',
        'property_details_hasCooling',
        # Entire sections removed
        'history',
        'nearby',
        'photos',
        'metadata',
    ]
    return removed


def load_houses_to_dataframe(houses_dir: str) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load all house JSON files into a pandas DataFrame.
    Filters out features that should have been removed in preprocessing.
    
    Args:
        houses_dir: Directory containing flattened/one-hot encoded house JSON files
        
    Returns:
        Tuple of (DataFrame with all houses, list of filenames)
    """
    houses_path = Path(houses_dir)
    json_files = list(houses_path.glob("*.json"))
    
    if not json_files:
        raise ValueError(f"No JSON files found in {houses_dir}")
    
    houses_data = []
    filenames = []
    
    for filepath in sorted(json_files):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            houses_data.append(data)
            filenames.append(filepath.name)
        except Exception as e:
            print(f"⚠️  Error loading {filepath.name}: {e}")
            continue
    
    df = pd.DataFrame(houses_data)
    
    # Filter out removed features (pandas may create columns for them if they appear in any file)
    removed_features = get_removed_features()
    columns_to_drop = [col for col in df.columns if col in removed_features]
    
    if columns_to_drop:
        print(f"⚠️  Found {len(columns_to_drop)} removed features in data, filtering them out:")
        for col in sorted(columns_to_drop):
            print(f"      - {col}")
        df = df.drop(columns=columns_to_drop)
    
    print(f"✅ Loaded {len(df)} houses from {houses_dir}")
    print(f"   Total features: {len(df.columns)}")
    
    return df, filenames


def identify_feature_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Identify which columns belong to each feature category.
    Works on flattened output from one_hot_encode_houses.py.
    
    Args:
        df: DataFrame with all house features (already flattened)
        
    Returns:
        Dictionary mapping category names to column lists
    """
    all_columns = df.columns.tolist()
    
    # Phase 4: Core features to impute (these are the ones with nulls)
    phase4_features = [
        'basic_info_bedrooms',
        'basic_info_bathrooms',
        'basic_info_livingArea',
        'basic_info_lotSize_sqft',
        'basic_info_house_age',
        'scores_walkScore',
        'scores_transitScore',
        'scores_bikeScore',
        'schools_elementary_school_rating',
        'schools_elementary_school_distance',
        'schools_middle_school_rating',
        'schools_middle_school_distance',
        'schools_high_school_rating',
        'schools_high_school_distance',
    ]
    
    # Location features: one-hot encoded zipcode and neighborhood (used for distance, NOT imputed)
    zipcode_features = [col for col in all_columns if col.startswith('basic_info_zipcode_')]
    neighborhood_features = [col for col in all_columns if col.startswith('location_neighborhood_')]
    
    # Other numeric features that should be included in KNN distance calculation
    # (but won't be imputed - they're already complete or handled elsewhere)
    other_numeric_features = [
        'location_latitude',
        'location_longitude',
        'financial_lastSoldPrice',
        'financial_monthlyHoaFee',
        'financial_taxAssessedValue',
        'financial_taxAssessedValue_is_missing',
        'property_details_fireplaces',
        'property_details_garageParkingCapacity',
        'property_details_hasFireplace',
        'property_details_hasGarage',
        'property_details_hasHeating',
        'property_details_hasSpa',
        'property_details_hasView',
        'property_details_standard_appliance_score',
        'features_hasAssociation',
        'features_numberOfUnitsInCommunity',
    ]
    
    # One-hot encoded features (also used for distance, NOT imputed)
    # These are already binary (0/1) from one_hot_encode_houses.py
    one_hot_features = [
        col for col in all_columns 
        if any(col.startswith(prefix) for prefix in [
            'basic_info_homeType_',
            'property_details_heating_',
            'property_details_flooring_',
            'property_details_parkingFeatures_',
            'property_details_exteriorFeatures_',
        ])
    ]
    
    # Filter to only include columns that actually exist in the dataframe
    phase4_features = [col for col in phase4_features if col in all_columns]
    other_numeric_features = [col for col in other_numeric_features if col in all_columns]
    
    return {
        'phase4': phase4_features,
        'zipcode': zipcode_features,
        'neighborhood': neighborhood_features,
        'other_numeric': other_numeric_features,
        'one_hot': one_hot_features,
    }


def analyze_missingness(df: pd.DataFrame, feature_cols: List[str]) -> Dict[str, Tuple[int, float]]:
    """
    Analyze missing data patterns in Phase 4 features.
    
    Args:
        df: DataFrame with house data
        feature_cols: List of column names to analyze
        
    Returns:
        Dictionary mapping feature name to (null_count, null_percentage)
    """
    missingness = {}
    total_rows = len(df)
    
    for col in feature_cols:
        if col in df.columns:
            null_count = df[col].isna().sum()
            null_pct = (null_count / total_rows) * 100
            missingness[col] = (null_count, null_pct)
    
    return missingness


def apply_knn_imputation(
    df: pd.DataFrame,
    features_to_impute: List[str],
    features_for_distance: List[str],
    n_neighbors: int = 5,
    weights: str = 'distance'
) -> Tuple[pd.DataFrame, Dict[str, int], MinMaxScaler, List[str]]:
    """
    Apply KNN imputation to specified features.
    
    Args:
        df: DataFrame with house data
        features_to_impute: Columns to impute (Phase 4 features)
        features_for_distance: All columns to use for distance calculation
        n_neighbors: Number of neighbors for KNN
        weights: 'uniform' or 'distance' (distance gives more weight to closer neighbors)
        
    Returns:
        Tuple of (DataFrame with imputed values, dict of imputation counts, fitted scaler, feature names)
    """
    # Create a copy to avoid modifying original
    df_imputed = df.copy()
    
    # Combine features: those to impute + those for distance calculation only
    all_features_for_knn = list(set(features_to_impute + features_for_distance))
    
    # Extract the feature matrix
    X = df[all_features_for_knn].copy()
    
    print(f"\n📊 KNN Imputation Setup:")
    print(f"   Features to impute: {len(features_to_impute)}")
    print(f"   Features for distance calculation: {len(all_features_for_knn)}")
    print(f"   K (neighbors): {n_neighbors}")
    print(f"   Weighting: {weights}")
    
    # Track which values were imputed
    imputation_counts = {}
    for col in features_to_impute:
        if col in X.columns:
            imputation_counts[col] = X[col].isna().sum()
    
    # Step 1: Scale features using MinMaxScaler
    print("\n🔧 Step 1: Scaling features (MinMaxScaler)...")
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Step 2: Apply KNN imputation
    print("🔧 Step 2: Applying KNN imputation...")
    imputer = KNNImputer(n_neighbors=n_neighbors, weights=weights)
    X_imputed_scaled = imputer.fit_transform(X_scaled)
    
    # Step 3: Inverse transform to original scale
    print("🔧 Step 3: Converting back to original scale...")
    X_imputed = scaler.inverse_transform(X_imputed_scaled)
    
    # Step 4: Update dataframe with imputed values
    # IMPORTANT: Only update the Phase 4 features that were actually imputed
    # Don't replace one-hot encoded features or other features that were fine
    X_imputed_df = pd.DataFrame(X_imputed, columns=all_features_for_knn, index=X.index)
    
    # Only update the features we're imputing (Phase 4 features)
    for col in features_to_impute:
        if col in X_imputed_df.columns:
            df_imputed[col] = X_imputed_df[col]
    
    return df_imputed, imputation_counts, scaler, all_features_for_knn


def print_imputed_value_stats(df: pd.DataFrame, imputation_counts: Dict[str, int]):
    """
    Print statistics of imputed values for each feature.
    
    Args:
        df: DataFrame with imputed values
        imputation_counts: Dictionary of features that were imputed
    """
    print("\n📊 Statistics of imputed values:")
    print("-" * 70)
    
    for feature in sorted(imputation_counts.keys()):
        if imputation_counts[feature] > 0 and feature in df.columns:
            col = df[feature]
            feature_display = feature.replace('basic_info_', '').replace('scores_', '').replace('schools_', '')
            
            print(f"\n   {feature_display}:")
            print(f"      Range: [{col.min():.2f}, {col.max():.2f}]")
            print(f"      Mean: {col.mean():.2f}, Median: {col.median():.2f}")
            print(f"      Std Dev: {col.std():.2f}")


def detect_weird_imputed_values(df: pd.DataFrame, imputation_counts: Dict[str, int]):
    """
    Detect and report potentially problematic imputed values before post-processing.
    Only flags truly invalid values (negatives, out of valid ranges), not extreme outliers.
    
    Args:
        df: DataFrame with imputed values (before post-processing)
        imputation_counts: Dictionary of features that were imputed
    """
    print("\n⚠️  Detecting weird imputed values (before post-processing):")
    
    weird_found = False
    
    # Check bedrooms
    if 'basic_info_bedrooms' in df.columns and 'basic_info_bedrooms' in imputation_counts:
        col = df['basic_info_bedrooms']
        non_integer = col[col != np.round(col)]
        negative = col[col < 0]
        
        if len(non_integer) > 0 or len(negative) > 0:
            weird_found = True
            print(f"\n   Bedrooms:")
            if len(non_integer) > 0:
                print(f"      Non-integer values: {len(non_integer)} (e.g., {non_integer.head(3).tolist()})")
            if len(negative) > 0:
                print(f"      Negative values: {len(negative)} (e.g., {negative.head(3).tolist()})")
    
    # Check bathrooms
    if 'basic_info_bathrooms' in df.columns and 'basic_info_bathrooms' in imputation_counts:
        col = df['basic_info_bathrooms']
        not_half_increment = col[(np.round(col * 2) / 2) != col]
        negative = col[col < 0]
        
        if len(not_half_increment) > 0 or len(negative) > 0:
            weird_found = True
            print(f"\n   Bathrooms:")
            if len(not_half_increment) > 0:
                print(f"      Not in 0.5 increments: {len(not_half_increment)} (e.g., {not_half_increment.head(3).tolist()})")
            if len(negative) > 0:
                print(f"      Negative values: {len(negative)} (e.g., {negative.head(3).tolist()})")
    
    # Check livingArea
    if 'basic_info_livingArea' in df.columns and 'basic_info_livingArea' in imputation_counts:
        col = df['basic_info_livingArea']
        negative_or_zero = col[col <= 0]
        
        if len(negative_or_zero) > 0:
            weird_found = True
            print(f"\n   LivingArea:")
            print(f"      Non-positive values: {len(negative_or_zero)} (e.g., {negative_or_zero.head(3).tolist()})")
    
    # Check lotSize
    if 'basic_info_lotSize_sqft' in df.columns and 'basic_info_lotSize_sqft' in imputation_counts:
        col = df['basic_info_lotSize_sqft']
        negative = col[col < 0]
        
        if len(negative) > 0:
            weird_found = True
            print(f"\n   LotSize:")
            print(f"      Negative values: {len(negative)} (e.g., {negative.head(3).tolist()})")
    
    # Check house_age
    if 'basic_info_house_age' in df.columns and 'basic_info_house_age' in imputation_counts:
        col = df['basic_info_house_age']
        negative = col[col < 0]
        
        if len(negative) > 0:
            weird_found = True
            print(f"\n   House Age:")
            print(f"      Negative values: {len(negative)} (e.g., {negative.head(3).tolist()})")
    
    # Check scores
    for score_col in ['scores_walkScore', 'scores_transitScore', 'scores_bikeScore']:
        if score_col in df.columns and score_col in imputation_counts:
            col = df[score_col]
            out_of_range = col[(col < 0) | (col > 100)]
            
            if len(out_of_range) > 0:
                weird_found = True
                score_name = score_col.replace('scores_', '')
                print(f"\n   {score_name}:")
                print(f"      Out of range [0,100]: {len(out_of_range)} (e.g., {out_of_range.head(3).tolist()})")
    
    # Check school ratings
    for col_name in ['schools_elementary_school_rating', 'schools_middle_school_rating', 'schools_high_school_rating']:
        if col_name in df.columns and col_name in imputation_counts:
            col = df[col_name]
            out_of_range = col[(col < 1) | (col > 10)]
            
            if len(out_of_range) > 0:
                weird_found = True
                display_name = col_name.replace('schools_', '').replace('_', ' ')
                print(f"\n   {display_name}:")
                print(f"      Out of range [1,10]: {len(out_of_range)} (e.g., {out_of_range.head(3).tolist()})")
    
    # Check school distances
    for col_name in ['schools_elementary_school_distance', 'schools_middle_school_distance', 'schools_high_school_distance']:
        if col_name in df.columns and col_name in imputation_counts:
            col = df[col_name]
            negative = col[col < 0]
            
            if len(negative) > 0:
                weird_found = True
                display_name = col_name.replace('schools_', '').replace('_', ' ')
                print(f"\n   {display_name}:")
                print(f"      Negative values: {len(negative)} (e.g., {negative.head(3).tolist()})")
    
    if not weird_found:
        print("   ✅ No problematic values detected!")


def post_process_imputations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply logical constraints to imputed values.
    Does NOT clip extreme values (SF has valid outliers), only fixes invalid values.
    
    Args:
        df: DataFrame with imputed values
        
    Returns:
        DataFrame with post-processed values
    """
    df_processed = df.copy()
    
    print("\n🔧 Post-processing imputed values:")
    
    # Helper function to print examples of weird values
    def print_weird_examples(original, processed, col_name, max_examples=5):
        mask = original != processed
        if mask.sum() > 0:
            weird_indices = mask[mask].index[:max_examples]
            print(f"      Examples of adjustments:")
            for idx in weird_indices:
                print(f"        Row {idx}: {original.iloc[idx]:.4f} → {processed.iloc[idx]:.4f}")
    
    # Bedrooms: round to nearest integer, set negative to 0
    if 'basic_info_bedrooms' in df_processed.columns:
        original = df_processed['basic_info_bedrooms'].copy()
        rounded = np.round(df_processed['basic_info_bedrooms'])
        df_processed['basic_info_bedrooms'] = rounded.clip(lower=0).astype(int)
        changed = (original != df_processed['basic_info_bedrooms']).sum()
        print(f"   ✓ Bedrooms: Rounded to nearest integer, negatives → 0 ({changed} values adjusted)")
        if changed > 0:
            print_weird_examples(original, df_processed['basic_info_bedrooms'].astype(float), 'bedrooms')
    
    # Bathrooms: round to nearest 0.5 increment, set negative to 0
    if 'basic_info_bathrooms' in df_processed.columns:
        original = df_processed['basic_info_bathrooms'].copy()
        rounded = np.round(df_processed['basic_info_bathrooms'] * 2) / 2
        df_processed['basic_info_bathrooms'] = rounded.clip(lower=0)
        changed = (original != df_processed['basic_info_bathrooms']).sum()
        print(f"   ✓ Bathrooms: Rounded to nearest 0.5 increment, negatives → 0 ({changed} values adjusted)")
        if changed > 0:
            print_weird_examples(original, df_processed['basic_info_bathrooms'], 'bathrooms')
    
    # LivingArea: must be positive (≥1)
    if 'basic_info_livingArea' in df_processed.columns:
        original = df_processed['basic_info_livingArea'].copy()
        df_processed['basic_info_livingArea'] = df_processed['basic_info_livingArea'].clip(lower=1)
        changed = (original != df_processed['basic_info_livingArea']).sum()
        print(f"   ✓ LivingArea: Non-positive values → 1 ({changed} values adjusted)")
        if changed > 0:
            print_weird_examples(original, df_processed['basic_info_livingArea'], 'livingArea')
    
    # LotSize: must be non-negative (≥0)
    if 'basic_info_lotSize_sqft' in df_processed.columns:
        original = df_processed['basic_info_lotSize_sqft'].copy()
        df_processed['basic_info_lotSize_sqft'] = df_processed['basic_info_lotSize_sqft'].clip(lower=0)
        changed = (original != df_processed['basic_info_lotSize_sqft']).sum()
        print(f"   ✓ LotSize: Negatives → 0 ({changed} values adjusted)")
        if changed > 0:
            print_weird_examples(original, df_processed['basic_info_lotSize_sqft'], 'lotSize')
    
    # House age: must be non-negative (≥0)
    if 'basic_info_house_age' in df_processed.columns:
        original = df_processed['basic_info_house_age'].copy()
        df_processed['basic_info_house_age'] = df_processed['basic_info_house_age'].clip(lower=0)
        changed = (original != df_processed['basic_info_house_age']).sum()
        print(f"   ✓ House age: Negatives → 0 ({changed} values adjusted)")
        if changed > 0:
            print_weird_examples(original, df_processed['basic_info_house_age'], 'house_age')
    
    # Scores: clip to valid range [0, 100]
    for score_col in ['scores_walkScore', 'scores_transitScore', 'scores_bikeScore']:
        if score_col in df_processed.columns:
            original = df_processed[score_col].copy()
            df_processed[score_col] = df_processed[score_col].clip(lower=0, upper=100)
            changed = (original != df_processed[score_col]).sum()
            score_name = score_col.replace('scores_', '')
            print(f"   ✓ {score_name}: Clipped to [0, 100] ({changed} values adjusted)")
            if changed > 0:
                print_weird_examples(original, df_processed[score_col], score_name)
    
    # School ratings: clip to valid range [1, 10]
    school_rating_cols = [
        'schools_elementary_school_rating',
        'schools_middle_school_rating',
        'schools_high_school_rating'
    ]
    for col in school_rating_cols:
        if col in df_processed.columns:
            original = df_processed[col].copy()
            df_processed[col] = df_processed[col].clip(lower=1, upper=10)
            changed = (original != df_processed[col]).sum()
            col_name = col.replace('schools_', '').replace('_', ' ')
            print(f"   ✓ {col_name}: Clipped to [1, 10] ({changed} values adjusted)")
            if changed > 0:
                print_weird_examples(original, df_processed[col], col_name)
    
    # School distances: must be non-negative (≥0)
    school_distance_cols = [
        'schools_elementary_school_distance',
        'schools_middle_school_distance',
        'schools_high_school_distance'
    ]
    for col in school_distance_cols:
        if col in df_processed.columns:
            original = df_processed[col].copy()
            df_processed[col] = df_processed[col].clip(lower=0)
            changed = (original != df_processed[col]).sum()
            col_name = col.replace('schools_', '').replace('_', ' ')
            print(f"   ✓ {col_name}: Negatives → 0 ({changed} values adjusted)")
            if changed > 0:
                print_weird_examples(original, df_processed[col], col_name)
    
    return df_processed


def save_imputed_houses(df: pd.DataFrame, filenames: List[str], output_dir: str):
    """
    Save imputed houses back to individual JSON files.
    Filters out removed features before saving.
    
    Args:
        df: DataFrame with imputed house data
        filenames: List of original filenames
        output_dir: Directory to save imputed houses
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    print(f"\n💾 Saving imputed houses to {output_dir}...")
    
    # Filter out removed features before saving
    removed_features = get_removed_features()
    columns_to_drop = [col for col in df.columns if col in removed_features]
    if columns_to_drop:
        df_clean = df.drop(columns=columns_to_drop)
    else:
        df_clean = df
    
    saved_count = 0
    error_count = 0
    
    for idx, filename in enumerate(filenames):
        try:
            # Convert row to dictionary
            house_data = df_clean.iloc[idx].to_dict()
            
            # Remove NaN values (they'll be serialized as null in JSON)
            house_data = {k: v for k, v in house_data.items() if pd.notna(v)}
            
            # Save to JSON
            output_file = output_path / filename
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(house_data, f, indent=2, ensure_ascii=False)
            
            saved_count += 1
            
        except Exception as e:
            print(f"⚠️  Error saving {filename}: {e}")
            error_count += 1
    
    print(f"✅ Saved {saved_count} houses")
    if error_count > 0:
        print(f"❌ Errors: {error_count}")


def save_preprocessing_artifacts(
    scaler: MinMaxScaler,
    feature_names: List[str],
    features_to_impute: List[str],
    n_neighbors: int,
    weights: str,
    output_dir: str
):
    """
    Save preprocessing artifacts needed for inference.
    
    Args:
        scaler: Fitted MinMaxScaler
        feature_names: List of feature names used in KNN (in order)
        features_to_impute: List of features that were imputed
        n_neighbors: K value used for KNN
        weights: Weight function used
        output_dir: Directory to save artifacts
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    print(f"\n💾 Saving preprocessing artifacts...")
    
    # Save the scaler
    scaler_file = output_path / "knn_scaler.pkl"
    with open(scaler_file, 'wb') as f:
        pickle.dump(scaler, f)
    print(f"   ✓ Saved MinMaxScaler to {scaler_file}")
    
    # Save metadata
    metadata = {
        'feature_names': feature_names,
        'features_to_impute': features_to_impute,
        'n_neighbors': n_neighbors,
        'weights': weights,
        'description': 'KNN imputation preprocessing artifacts for inference'
    }
    
    metadata_file = output_path / "knn_metadata.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"   ✓ Saved metadata to {metadata_file}")
    
    print(f"\n📦 Preprocessing artifacts saved to: {output_dir}")
    print(f"   Files:")
    print(f"     - knn_scaler.pkl      (MinMaxScaler for feature scaling)")
    print(f"     - knn_metadata.json   (Feature names, KNN params)")


def print_imputation_report(
    missingness_before: Dict[str, Tuple[int, float]],
    imputation_counts: Dict[str, int],
    total_houses: int
):
    """
    Print a detailed report of the imputation process.
    
    Args:
        missingness_before: Missingness stats before imputation
        imputation_counts: Number of values imputed per feature
        total_houses: Total number of houses
    """
    print("\n" + "=" * 70)
    print("IMPUTATION REPORT")
    print("=" * 70)
    
    print(f"\nTotal houses: {total_houses}")
    print("\nPhase 4 Features - Imputed with KNN:")
    print("-" * 70)
    
    for feature, (null_count, null_pct) in sorted(missingness_before.items()):
        imputed = imputation_counts.get(feature, 0)
        feature_display = feature.replace('basic_info_', '').replace('scores_', '').replace('schools_', '')
        print(f"  {feature_display:40s}: {null_count:4d} nulls ({null_pct:5.2f}%) → {imputed:4d} imputed")
    
    total_nulls = sum(count for count, _ in missingness_before.values())
    total_imputed = sum(imputation_counts.values())
    
    print("-" * 70)
    print(f"  {'TOTAL':40s}: {total_nulls:4d} nulls → {total_imputed:4d} imputed")
    print("=" * 70)


def process_houses(
    source_dir: str = "houses_onehot_encoded",
    dest_dir: str = "houses_imputed",
    n_neighbors: int = 5,
    weights: str = 'distance'
):
    """
    Main processing function: Load houses, apply KNN imputation, save results.
    Works on output from one_hot_encode_houses.py (already flattened).
    
    Args:
        source_dir: Directory containing one-hot encoded houses (from one_hot_encode_houses.py)
        dest_dir: Directory to save imputed houses
        n_neighbors: Number of neighbors for KNN
        weights: 'uniform' or 'distance'
    """
    print("=" * 70)
    print("KNN IMPUTATION FOR REAL ESTATE FEATURES")
    print("=" * 70)
    print(f"📂 Source: {source_dir} (output from one_hot_encode_houses.py)")
    print(f"📂 Destination: {dest_dir}\n")
    
    # Step 1: Load houses into DataFrame
    print("📂 Step 1: Loading houses...")
    df, filenames = load_houses_to_dataframe(source_dir)
    
    # Step 1.5: Filter out rows with missing critical values (safety check)
    # These should have been filtered earlier, but check again to be safe
    initial_rows = len(df)
    
    # Create mask for valid rows
    valid_mask = pd.Series([True] * len(df), index=df.index)
    
    # Remove rows with missing lastSoldPrice (target variable)
    target_col = 'financial_lastSoldPrice'
    if target_col in df.columns:
        rows_before = valid_mask.sum()
        valid_mask = valid_mask & df[target_col].notna()
        removed_price = rows_before - valid_mask.sum()
        if removed_price > 0:
            print(f"⚠️  Removed {removed_price} rows with missing {target_col} (should have been filtered earlier)")
    
    # Remove rows with missing latitude or longitude
    lat_col = 'location_latitude'
    long_col = 'location_longitude'
    if lat_col in df.columns and long_col in df.columns:
        rows_before = valid_mask.sum()
        valid_mask = valid_mask & df[lat_col].notna() & df[long_col].notna()
        removed_location = rows_before - valid_mask.sum()
        if removed_location > 0:
            print(f"⚠️  Removed {removed_location} rows with missing location data (should have been filtered earlier)")
    
    # Apply filter to dataframe and filenames
    if initial_rows != valid_mask.sum():
        df = df[valid_mask].reset_index(drop=True)
        filenames = [filenames[i] for i in range(len(filenames)) if valid_mask.iloc[i]]
        print(f"   Total rows removed: {initial_rows - len(df)}")
        print(f"   Remaining rows: {len(df)}")
    
    # Step 2: Identify feature columns
    print("\n🔍 Step 2: Identifying feature columns...")
    feature_groups = identify_feature_columns(df)
    
    print(f"   Phase 4 (to impute): {len(feature_groups['phase4'])} features")
    print(f"   Zipcode features: {len(feature_groups['zipcode'])} features")
    print(f"   Neighborhood features: {len(feature_groups['neighborhood'])} features")
    print(f"   One-hot features: {len(feature_groups['one_hot'])} features")
    print(f"   Other numeric features: {len(feature_groups['other_numeric'])} features")
    
    # Step 3: Analyze missingness
    print("\n📊 Step 3: Analyzing missing data...")
    missingness_before = analyze_missingness(df, feature_groups['phase4'])
    
    # Step 4: Apply KNN imputation
    print("\n🤖 Step 4: Applying KNN imputation...")
    
    # Features for distance calculation: location + one-hot + other numeric
    # Note: Phase 4 features are also included in distance calculation
    features_for_distance = (
        feature_groups['zipcode'] +
        feature_groups['neighborhood'] +
        feature_groups['one_hot'] +
        feature_groups['other_numeric']
    )
    
    df_imputed, imputation_counts, scaler, feature_names = apply_knn_imputation(
        df,
        features_to_impute=feature_groups['phase4'],
        features_for_distance=features_for_distance,
        n_neighbors=n_neighbors,
        weights=weights
    )
    
    # Step 4.5: Analyze imputed values
    print_imputed_value_stats(df_imputed, imputation_counts)
    detect_weird_imputed_values(df_imputed, imputation_counts)
    
    # Step 5: Post-process imputed values
    print("\n🔧 Step 5: Post-processing...")
    df_final = post_process_imputations(df_imputed)
    
    # Step 6: Save imputed houses
    print("\n💾 Step 6: Saving results...")
    save_imputed_houses(df_final, filenames, dest_dir)
    
    # Step 7: Save preprocessing artifacts (scaler, metadata)
    save_preprocessing_artifacts(
        scaler=scaler,
        feature_names=feature_names,
        features_to_impute=feature_groups['phase4'],
        n_neighbors=n_neighbors,
        weights=weights,
        output_dir=dest_dir
    )
    
    # Step 8: Print report
    print_imputation_report(missingness_before, imputation_counts, len(df))
    
    print(f"\n✅ Imputation complete! Output saved to: {dest_dir}")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Apply KNN imputation to handle missing values in core property features. "
                    "Works on output from one_hot_encode_houses.py."
    )
    parser.add_argument(
        "--source",
        type=str,
        default="houses_onehot_encoded",
        help="Source directory containing one-hot encoded house JSON files (default: houses_onehot_encoded)"
    )
    parser.add_argument(
        "--dest",
        type=str,
        default="houses_imputed",
        help="Destination directory for imputed houses (default: houses_imputed)"
    )
    parser.add_argument(
        "--k",
        type=int,
        default=5,
        help="Number of neighbors for KNN (default: 5)"
    )
    parser.add_argument(
        "--weights",
        type=str,
        default="distance",
        choices=["uniform", "distance"],
        help="Weight function for KNN: 'uniform' or 'distance' (default: distance)"
    )
    
    args = parser.parse_args()
    
    process_houses(
        source_dir=args.source,
        dest_dir=args.dest,
        n_neighbors=args.k,
        weights=args.weights
    )
