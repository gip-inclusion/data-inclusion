# `dbt` project guidelines

## Layout

The models are organized according to [the conventional layout](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview):

* the **source** layer contains the raw data. The source tables are created in postgres beforehand (with airflow) and simply declared in dbt.
* the **seeds** directory contains stable long-lived data (like [our schema](https://github.com/gip-inclusion/data-inclusion-schema)). No cleaning required.
* the **staging** layer built on top of sources models. Staging models transforms raw source data into proper tables and columns. The data is cleaned, but the original data model is kept.
* the **intermediate** layer built on top of the staging models and seeds. It contains all the heavy work (changing data models, enhancing data etc.).
* the **marts** layer built on top of the intermediate models. This is the final presentational layer.

Each models in a given layer should strictly use models from the previous layer. Intermediate models should never use source models and marts models should never use staging models.

The **source** layer are loaded in two ways. On the first hand, well defined and reliable data sources (like sirene) are loaded using their actual schema. On the other hand, less reliable sources (like the inclusion data providers) are loaded using a single jsonb column (`data`) that should be parsed into proper tables in the staging layer.

The **staging** layer is essentially cleaning and testing the source data. The tests define and ensure expectations about source tables that can be trusted by the downstream models : a table has an id, which is unique and always present, etc. The staging models are practical cleaned views on the source data, which does not alter the nature of the data.

Typical staging cleanups include:

* unnesting nested data structures or arrays into their own, testable tables
* replacing empty strings with nulls
* snake casing columns
* removing html tags

Typical staging tests include testing that:

* tables have an unique id
* tables references each other
* a column is limited to a given set of expected values
* a column is not always null
* a column does not have empty strings
* important columns are filled enough

Heavy computation belong to the **intermediate** layer. Most of the work is done in this layer.

Typical intermediate computation includes:

* filtering
* renaming the columns to fit into a given schema
* changing the nature of data (e.g. places to services)
* geocoding
* joining data with other sources
* generating data
* any transformation that alter the source data

Intermediate stage tests check the results of our computations, when staging tests check their inputs.

The **marts** layer can be thought as a presentational layer. Data that will be used by downstream services should be declared here. This include the data·inclusion dataset (structures and services). It is a ready-to-use view that assembles results of our intermediate layer.

## Orchestration

In production, airflow run the dbt project, using the `main` dag especially. Related dbt models are grouped and built in dedicated airflow tasks. For instance, the staging layer models are grouped by source.

The `main` dag is designed to prevent corrupted sources to block the entire pipeline execution. Some models are first ran and built in temporary database schema with a dedicated (non-blocking) airflow task. This way, when a task fails, the last known data will remain in use for downstream models and the pipeline continues to run.

## Testing models

#### `data_tests` vs `unit_tests` vs `contract`:

* with `dbt build`, `data_tests` are run **after** model execution **on the actual data**. A failing test will not prevent the faulty data to be propagated downstream, unless properly managed by the orchestration.
* with `dbt build`, `unit_tests` are run **before** model execution **on mock-up data**. This is great to test logic, but requires to make assumptions on the input data.
* `contract`s are enforced using actual DB constraints, **on the actual data**. A failing constraint will stop the model execution and prevent faulty data to be propagated downstream. Unlike `data_tests`, we cannot set a severity level. There is no middle ground. And the faulty data cannot be easily queried.

✅ use `unit_tests` to test **complex logic** on well-defined data (e.g. converting opening hours).

❌ avoid `unit_tests` for simple transformations. There are costly to maintain and will very often just duplicate the implementation.

✅ always add a few `data_tests`.

✅ use `contract`s on `marts`. Marts data can be consumed by clients.

#### which layer (`source`, `staging`, `intermediate`, `marts`) should I test ?

It's better to test data early, so we can make assumption on which which we can later build.

Our `source` layer is essentially tables containing the raw data in jsonb `data` columns. While this is very handy to load data, it is unpractical to test with `data_tests`.

Therefore our tests start at the `staging` layer.

`staging`:

* ✅ use `data_tests` extensively
* ✅ assumptions on data used in downstream models should be tested
* ❌ avoid testing columns that are not used in downstream models

`intermediate`:

* ✅ use `data_tests` for primary keys and foreign keys
* ✅ use the generic tests `model_validate` **in dev**
* ✅ use `dbt_utils.equal_rowcount` between `staging` and `intermediate` when applicable simply

`marts`:

* ✅ use `contracts`
* ✅ use generic tests `model_validate`

#### which type of `data_tests` should I use ?

To stay manageable, our tests should be more or less uniform across the codebase.

✅ always use native `unique` and `not_null` for primary keys.

✅ always use `relationships` for foreign keys.

✅ use `not_null`, `dbt_utils.not_empty_string` and `dbt_utils.not_constant` when possible.

✅ use `accepted_values` for categorical columns from well-defined data.

❌ avoid `accepted_values` for categorical columns of less than great data, or downgrade the test severity to `warn`. Otherwise the test could fail too regularly.

✅ when `not_null` fails, use `dbt_utils.not_null_proportion` to check that a column is usably filled. It tells use how the data can be used downstream and how confident we can be using it. Keep a large margin between the threshold and the actual proportion. For simplicity, use the `0.1`/`0.5`/`0.9` thresholds:

* `0.1`: barely filled, not enough to be used, but if the data is providing crucial information, it's there.
* `0.5`: usually filled, good enough to be used as input in a mapping, but might need some additional data.
* `0.9`: almost required.

❌ avoid setting a high or custom threshold for `dbt_utils.not_null_proportion`. A high threshold with a small margin is likely to fail in the future.

✅ use `dbt_utils.equal_rowcount` to compare layers and check that data is not unintentionally filtered out. If unequal row counts are expected (because the data is intentionally filtered), either use a `where:` in the test config OR drop the test entirely if that's too much.

✅ for simple cases, use predefined generic data tests over custom data tests (in `tests/`). Usually requires less code and is easier to read, *unless* you want to test complex logic.

## references

* https://www.datafold.com/blog/7-dbt-testing-best-practices
* https://docs.getdbt.com/best-practices
