# Github Actions deployment & checks
All of our CI checks and deployments are managed through Github actions.

## Automatic checks
Every pull request gets first checked through the jobs in `pipeline.yml`.

Depending on the sub-directory you were working on, a series of linters
and test suites will be run and verify that no assertion is broken.

Of course, this does not ensure that your changes do not have an impact
on the data or features of the product; our coverage can't be very high
as, being a data project, we can't test our changes at scale, on real data,
almost by definition.

## Deploying the pipeline to staging
Which is why there is a way to test your code in the `staging` area; to
enable a deployment in staging -and thus, removing the **pipeline** code that
would be currently deployed there- you need to choose the `deploy-to-staging`
label in Github to immediately trigger a deployment of your branch to the
`staging` platform.

Once this label is set, any further push or change on the pull request will
trigger a new deployment. It is expected to only have one of these labels in
all of our PRs at one given moment, even though it is currently not enforced
for practical reasons (it would be a little annoying)

Most of the time you'll still have to manually execute DAGs or DBT steps in
the staging environment to get to a dataset that validates your changes.

## Deploying the API to staging
The `staging` API in Scalingo will remain untouched, as it keeps following the
`main` branch for its deployments only. If you also want to test changes in the
API code in staging, you are welcome to trigger a manual deployment of it in
the Scalingo dashboard or though its CLI.

## Maintaining the staging data
It is most welcome to always have the staging data the closest possible from the
production data, so that both the Dora team and our beloved POs can do their
jobs without having too many artifacts.

It is our job to maintain the `staging` environment as stable and close to the
production data as possible.

## Deploying in production
Merging a PR to the `main` branch does not automatically deploy its code in production.

The `prod` environment follows the `release` branch, and Github only triggers a
deployment once manually validated in its interface (at the time of writing).

Thus, you have to manually rebase (fast-forward) the `release` branch to the latest `main`
head when you're ready to deploy to prod (and upload the new reference to Github)

Once the pipeline is successfully deployed to Scaleway's `prod` environment, an API
deployment on the `data-inclusion-api-prod` project is triggered through a hook
in Scalingo.
