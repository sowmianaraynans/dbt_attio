# dbt_attio

A dbt package for modelling [Attio](https://attio.com) CRM data loaded by Fivetran. Transforms raw Attio source tables into clean staging views, intermediate models, and mart tables ready for analytics.

## Supported warehouses

DuckDB · Snowflake · Databricks · BigQuery

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - package: sowmianaraynans/dbt_attio
    version: [">=0.1.0", "<0.2.0"]
```

Then run:

```bash
dbt deps
```

## Sources

The package expects Fivetran's Attio connector schema. Configure where to find it in your `dbt_project.yml`:

```yaml
vars:
  attio_schema: "attio"          # default — the Fivetran schema name
  attio_database: "raw"          # optional — omit to use target.database
```

## Models

| Model | Description |
|---|---|
| `dim_attio__companies` | One row per company record with pivoted attributes |
| `dim_attio__people` | One row per person record with pivoted attributes |
| `fct_attio__deals` | One row per deal with resolved status labels and owner |
| `fct_attio__notes` | All notes across all object types |
| `fct_attio__list_entries` | One row per list entry (pipeline stage grain) |

## Configuration

Add to your `dbt_project.yml`:

```yaml
vars:
  # Source location
  attio_schema: "attio"
  attio_database: "raw"            # optional

  # Disable models your workspace doesn't use
  attio__using_deals: true         # set false if no deals pipeline
  attio__using_notes: true         # set false if notes not used
  attio__using_lists: true         # set false if no lists/pipelines

  # Object slugs — only change if your Attio workspace uses non-default names
  attio__company_object_slug: "companies"
  attio__person_object_slug: "people"
  attio__deal_object_slug: "deals"

  # Include workspace-specific custom attributes in pivoted dims
  attio__company_custom_attributes: ['arr', 'tier']
  attio__person_custom_attributes: ['persona']
  attio__deal_custom_attributes: ['close_reason']
```

## Development

This package uses DuckDB + seeds for local development. No external warehouse needed.

```bash
dbt deps
dbt seed       # loads CSV fixtures into the 'attio' schema
dbt run
dbt test
```

To run integration tests:

```bash
cd integration_tests
dbt deps
dbt seed
dbt run
dbt test
```
