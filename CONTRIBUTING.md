# Contributing

## Local setup

Requires Python 3.9+ and [uv](https://github.com/astral-sh/uv) (or pip).

```bash
python -m venv .venv
source .venv/bin/activate
pip install dbt-core dbt-duckdb
dbt deps
```

## Running locally

Seeds replace the Fivetran source tables for local dev — no warehouse needed.

```bash
dbt seed       # load CSV fixtures into DuckDB
dbt run        # build all models
dbt test       # run schema + singular tests
```

## Running integration tests

```bash
cd integration_tests
dbt deps
dbt seed
dbt run
dbt test
```

## Submitting changes

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Run `dbt run && dbt test` from the root — all tests must pass
4. Open a pull request with a description of what changed and why

## Adding a new source table

1. Add the table to `models/sources.yml`
2. Create a staging model in `models/staging/stg_attio__<table>.sql`
3. Add column tests to `models/staging/stg_attio__staging.yml`
4. Add a matching seed CSV to `seeds/` with representative rows

## Repo structure

```
models/
  staging/       # one view per source table, rename + light cleaning only
  intermediate/  # ephemeral — business logic, pivots, joins
  marts/         # tables — final analytics-ready models
macros/          # reusable Jinja macros
seeds/           # CSV fixtures for local DuckDB development
tests/           # singular data tests
integration_tests/ # standalone dbt project for end-to-end testing
```
