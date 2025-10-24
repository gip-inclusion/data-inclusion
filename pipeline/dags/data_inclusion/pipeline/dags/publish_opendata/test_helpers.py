import pandas as pd
import pytest

from data_inclusion.pipeline.dags.publish_opendata import helpers


@pytest.mark.parametrize(
    ("format", "to_format_fn"),
    helpers.FORMATS_FN.items(),
)
def test_to_format(format, to_format_fn):
    import io

    df = pd.DataFrame(
        {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
            "latitude": [48.8566, 43.6045, 45.7640],
            "longitude": [2.3522, 1.4442, 4.8357],
        }
    )

    with io.BytesIO() as buf:
        to_format_fn(df, buf)
