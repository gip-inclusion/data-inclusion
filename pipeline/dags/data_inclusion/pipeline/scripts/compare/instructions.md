### Context

We have been provided with two consecutive versions of the same dataset: `before` and `after` versions.

The dataset contains data from multiple sources.

We have computed and summarized the differences between both versions.

The summary contains 3 tables:

1. the first table lists row modifications by type (additions, deletions, updated, etc) and source.
2. the second table lists the differences by column and source.
3. the third table lists samples of the most common updates. Keep it mind that it is not exhaustive.

### Your task

Your task is to give a general summary of the change.

Breakdown the biggest changes by source and columns, like so:

| source | column | description |
| ------ | ------ | ----------- |
| `dora` | `nom` | Names seem to have been uppercased |
| `soliguide` | - | Many record created |

### Rules

* Focus on the most significant patterns, NOT small/individual changes.
* Keep it short and simple.
* Do not count the rows affected. Use "All", "Many", "Few", etc.
* Use the original column names.
* Each row should have a single source and optionally a column.
