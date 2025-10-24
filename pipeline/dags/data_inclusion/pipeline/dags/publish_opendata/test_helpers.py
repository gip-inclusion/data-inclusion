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


@pytest.mark.parametrize(
    ("input_df", "expected_df"),
    [
        (
            pd.DataFrame(
                [{"id": 1, "_in_opendata": True}, {"id": 2, "_in_opendata": False}]
            ),
            pd.DataFrame([{"id": 1}]),
        ),
    ],
)
def test_remove_closed_sources(input_df: pd.DataFrame, expected_df: pd.DataFrame):
    result_df = helpers.remove_closed_sources(input_df)
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    ("input_df", "expected_df"),
    [
        (
            pd.DataFrame(
                [
                    {
                        "id": 1,
                        "_has_pii": True,
                        "courriel": "john@doe.com",
                        "telephone": "123-456-7890",
                    },
                    {
                        "id": 2,
                        "_has_pii": False,
                        "courriel": "john@doe.com",
                        "telephone": "123-456-7890",
                    },
                ]
            ),
            pd.DataFrame(
                [
                    {"id": 1, "courriel": None, "telephone": None},
                    {"id": 2, "courriel": "john@doe.com", "telephone": "123-456-7890"},
                ]
            ),
        ),
    ],
)
def test_remove_pii(input_df: pd.DataFrame, expected_df: pd.DataFrame):
    result_df = helpers.remove_pii(input_df)
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    ("input_df", "expected_df"),
    [
        (
            pd.DataFrame([{"id": 1, "_is_valid": True}, {"id": 2, "_is_valid": False}]),
            pd.DataFrame([{"id": 1}]),
        ),
    ],
)
def test_remove_invalid_rows(input_df: pd.DataFrame, expected_df: pd.DataFrame):
    result_df = helpers.remove_invalid_rows(input_df)
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    ("input_df", "expected_df"),
    [
        (
            pd.DataFrame([{"id": 1, "_internal": True, "public": True}]),
            pd.DataFrame([{"id": 1, "public": True}]),
        ),
    ],
)
def test_remove_internal_columns(input_df: pd.DataFrame, expected_df: pd.DataFrame):
    result_df = helpers.remove_internal_columns(input_df)
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    ("structures_df", "services_df", "expected_structures_df", "expected_services_df"),
    [
        (
            pd.DataFrame(
                [
                    {"id": 1, "_is_closed": False},
                    {"id": 2, "_is_closed": True},
                ]
            ),
            pd.DataFrame(
                [
                    {"id": 10, "structure_id": 1},
                    {"id": 20, "structure_id": 2},
                ]
            ),
            pd.DataFrame(
                [
                    {"id": 1},
                ]
            ),
            pd.DataFrame(
                [
                    {"id": 10, "structure_id": 1},
                ]
            ),
        ),
    ],
)
def test_remove_closed_structures(
    structures_df, services_df, expected_structures_df, expected_services_df
):
    result_structures_df, result_services_df = helpers.remove_closed_structures(
        structures_df, services_df
    )
    pd.testing.assert_frame_equal(result_structures_df, expected_structures_df)
    pd.testing.assert_frame_equal(result_services_df, expected_services_df)
