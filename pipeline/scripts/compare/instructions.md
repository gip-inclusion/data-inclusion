### Context

We have been provided with two subsequent versions of a dataset: the `before` and `after` versions.

We have computed differences between rows.

The differences are grouped by `source` and `column`.

For the most significant groups, you will be provided with samples.

The samples are json lines, with the following keys:

- `source`: a meta column with the name of the data provider.
- `before`: the value in the `before` version. Always `null` when `change_type` is `added`.
- `after`: the value in the `after` version. Always `null` when `change_type` is `removed`.

### Your task

Summarize changes.

Use french.

Focus on the most significant patterns, NOT small/individual changes.

Keep it short and simple, with no more than 10 bullets points.

Breakdown the changes by source and columns, like so:

```
- **dora**:
    1. `description`: ...
    2. `telephone`: ...
- **soliguide**:
    1. `description`: ...
- **toutes sources**:
    1. `nom`:
```

If all sources seem impacted by the same change, use **toutes sources**.
