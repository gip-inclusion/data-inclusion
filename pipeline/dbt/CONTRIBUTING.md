# `dbt` guidelines

## testing models

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
* ✅ use the generic tests `check_structure`, `check_service` and `check_address`
* ✅ use `dbt_utils.equal_rowcount` between `staging` and `intermediate` when applicable simply

`marts`:

* ✅ use `contracts`
* ✅ use generic tests `check_structure`, `check_service` and `check_address`

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
