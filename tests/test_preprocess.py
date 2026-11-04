"""A tax assessment made after the sale already reflects the sale price, so preprocessing must drop it."""
import pytest

from scripts.pipeline.preprocess_data import clean_house_data, is_post_sale_assessment


def house(tax_year, sold):
    return {
        'basic_info': {'homeType': 'CONDO', 'yearBuilt': 2000},
        'financial': {
            'lastSoldPrice': 1_000_000,
            'taxAssessedValue': 1_000_000,
            'taxAssessedYear': tax_year,
            'dateSoldString': sold,
        },
    }


@pytest.mark.parametrize('tax_year, sold, expected', [
    (2025, '2024-12-03', True),
    (2025, '2025-06-10', False),
    (2024, '2025-01-15', False),
    (None, '2024-12-03', False),
    (2025, None, False),
])
def test_is_post_sale_assessment(tax_year, sold, expected):
    assert is_post_sale_assessment(tax_year, sold) is expected


def test_post_sale_assessment_is_removed():
    cleaned = clean_house_data(house(2025, '2024-12-03'))
    assert cleaned['financial']['taxAssessedValue'] is None
    assert 'taxAssessedYear' not in cleaned['financial']
    assert 'dateSoldString' not in cleaned['financial']


def test_pre_sale_assessment_is_kept():
    cleaned = clean_house_data(house(2025, '2025-06-10'))
    assert cleaned['financial']['taxAssessedValue'] == 1_000_000
