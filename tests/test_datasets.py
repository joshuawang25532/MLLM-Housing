"""Contract checks on the committed tables the model notebook reads."""
from pathlib import Path

import pandas as pd
import pytest

DATA = Path(__file__).resolve().parent.parent / 'data'

# Valid ranges from the scoring rubric in model/description.ipynb
GEMINI_RANGES = {
    'luxury_score': (1, 10),
    'renovation_index': (0, 3),
    'distress_probability': (0, 1),
    'marketing_hype_ratio': (0, 1),
    'location_premium_inferred': (0, 2),
}


@pytest.fixture(scope='module')
def dataset():
    return pd.read_csv(DATA / 'final_dataset.csv')


@pytest.fixture(scope='module')
def gemini():
    return pd.read_csv(DATA / 'gemini_encoded_descriptions.csv')


def test_dataset_is_keyed_by_unique_zpid(dataset):
    assert dataset.columns[0] == 'zpid'
    assert dataset.zpid.is_unique


def test_target_and_location_are_present(dataset):
    assert dataset[['financial_lastSoldPrice', 'location_latitude', 'location_longitude']].notna().all().all()


def test_gemini_joins_one_to_one(dataset, gemini):
    assert gemini.zpid.is_unique
    assert set(gemini.zpid) == set(dataset.zpid)


def test_gemini_scores_within_rubric(gemini):
    for col, (lo, hi) in GEMINI_RANGES.items():
        assert gemini[col].dropna().between(lo, hi).all(), col


def test_no_gemini_scores_without_a_description(dataset, gemini):
    merged = dataset[['zpid', 'basic_info_description']].merge(gemini, on='zpid')
    empty = merged.basic_info_description.fillna('').str.strip() == ''
    assert merged.loc[empty, list(GEMINI_RANGES)].isna().all().all()
