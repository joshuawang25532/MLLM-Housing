"""The final CSV carries each listing's zpid (from its filename) as the first column."""
import json

import pandas as pd

from scripts.pipeline.json_to_csv import convert_to_csv


def test_zpid_comes_from_filename_and_is_first_column(tmp_path):
    src = tmp_path / 'houses'
    src.mkdir()
    for zpid, price in [(15063376, 1_200_000), (111711065, 950_000)]:
        (src / f'{zpid}.json').write_text(json.dumps({
            'financial_lastSoldPrice': price,
            'location_latitude': 37.75,
            'location_longitude': -122.45,
        }))
    (src / 'knn_metadata.json').write_text('{}')

    out = tmp_path / 'final.csv'
    convert_to_csv(str(src), str(out))
    df = pd.read_csv(out)

    assert df.columns[0] == 'zpid'
    assert dict(zip(df.zpid, df.financial_lastSoldPrice)) == {15063376: 1_200_000, 111711065: 950_000}
