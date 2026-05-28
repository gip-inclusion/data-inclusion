import polars as pl

from data_inclusion.pipeline.dags.extract_publics import model


def test_int__extracted_profiles():
    services_df = pl.DataFrame(
        [
            {
                "id": "1",
                "publics": None,
                "publics_precisions": "some precision",
            }
        ]
    )

    def extract_fn(input: str) -> dict | None:
        return {"profils": [{"age": {"max": 30}}, {"allocation": "rsa"}]}

    model.int__extracted_profiles(
        services_df=services_df,
        extract_fn=extract_fn,
    )
