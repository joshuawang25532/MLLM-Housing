"""KNN imputation must fill gaps without using the target and give the same result every run."""
import numpy as np
import pandas as pd
import pytest

from scripts.pipeline.impute_missing import (
    apply_knn_imputation,
    get_distance_features,
    identify_feature_columns,
)

TARGET = 'financial_lastSoldPrice'


@pytest.fixture
def houses():
    rng = np.random.default_rng(0)
    n = 60
    df = pd.DataFrame({
        'basic_info_bedrooms': rng.integers(1, 6, n).astype(float),
        'basic_info_livingArea': rng.normal(1500, 400, n),
        'scores_walkScore': rng.uniform(40, 100, n),
        'location_latitude': rng.uniform(37.70, 37.81, n),
        'location_longitude': rng.uniform(-122.51, -122.39, n),
        'financial_taxAssessedValue': rng.normal(1e6, 2e5, n),
        'basic_info_zipcode_94110': rng.integers(0, 2, n),
        'basic_info_zipcode_94122': rng.integers(0, 2, n),
        TARGET: rng.normal(1.5e6, 4e5, n),
    })
    for col, rows in [('basic_info_livingArea', [3, 17, 40]), ('scores_walkScore', [5, 22])]:
        df.loc[rows, col] = np.nan
    return df


def impute(df):
    groups = identify_feature_columns(df)
    imputed, _, _, _ = apply_knn_imputation(df, groups['phase4'], get_distance_features(groups))
    return imputed, groups


def test_target_is_not_an_imputation_feature(houses):
    groups = identify_feature_columns(houses)
    assert TARGET not in groups['phase4']
    assert TARGET not in get_distance_features(groups)


def test_imputed_values_do_not_depend_on_target(houses):
    shuffled = houses.copy()
    shuffled[TARGET] = np.random.default_rng(1).permutation(shuffled[TARGET].values)

    a, groups = impute(houses)
    b, _ = impute(shuffled)
    pd.testing.assert_frame_equal(a[groups['phase4']], b[groups['phase4']])


def test_imputation_is_deterministic(houses):
    a, _ = impute(houses)
    b, _ = impute(houses)
    pd.testing.assert_frame_equal(a, b)


def test_fills_gaps_and_keeps_observed_values(houses):
    imputed, groups = impute(houses)
    assert imputed[groups['phase4']].notna().all().all()

    observed = houses[groups['phase4']].notna()
    pd.testing.assert_frame_equal(
        imputed[groups['phase4']].where(observed), houses[groups['phase4']].where(observed)
    )
    assert imputed[TARGET].equals(houses[TARGET])
