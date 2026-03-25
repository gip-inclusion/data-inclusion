# `data_inclusion.pipeline.models`

This package contains python transformations (models).

Models inputs are polars dataframes and ouputs one polars dataframe.

Models package must contain:

- `model.py` module: the model.
- `data_tests.py` module: data tests that should be applied on production pipeline.
- `test_model.py` module: unit tests.

Data tests are based on the `pointblank` framework, which provides the ability to define thresholds, to check relationships, to outputs reports.

Models are orchestrated as airflow tasks. A typical task should fetch the data, run the model and its data tests, and write back the outputs somewhere.
