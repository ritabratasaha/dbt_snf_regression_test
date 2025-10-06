# dbt_snf_regression_test

This package provides a simple regression-testing workflow for Snowflake-backed dbt projects using dbt Python models. It compares a set of reference models to a corresponding set of regression models (materialized into a parallel schema with a `_regression` suffix) and records any differences.

The project includes:
- A release note JSON template to declare impacted dbt models/columns
- Macros to manage schemas and stage configuration files in Snowflake
- Two dbt Python models to execute and summarize regression results

---

## Purpose of the package

- Run data regression tests between two Snowflake tables for the same model: the reference version in the base schema and the regression version in a sibling schema with a `_regression` suffix.
- Upload and read config and release metadata from a Snowflake stage to dynamically determine which models and columns are included in a given test run.
- Persist the comparison results for auditing.

---


## Release template

Purpose: Declare which existing models changed in a release and which columns changed in each model. Only existing models participate in regression testing.

File: `release_template/release_v<x>.0.json`

Structure example:

```json
{
  "releases": [
    {
      "version": 1.0,
      "description": "Release notes for the current Data Product / dbt package",
      "date": "2025-01-01",
      "models_impacted": {
        "MODEL_NAME_1": ["COLUMN_1", "COLUMN_2"],
        "MODEL_NAME_2": ["COLUMN_1", "COLUMN_2"]
      }
    }
  ]
}
```

Sample usage:
1) Copy a template to a real file, e.g. `/releases/release_v1.0.json`, and fill in impacted models/columns. You can use github mcp to do this job seamlessly for you. To use mcp you need to create a pull request and then share the pull request url to the mcp server to do the job for you
2) Upload both to Snowflake using the macro `push_configs`:

Let us review the macros in the next section

---

## Macros

### `generate_schema_name(custom_schema_name, node)`
Purpose: Append `_regression` to the target schema when the active dbt target is `regression`. This lets you materialize the same models into a parallel schema for A/B comparison without changing model code.

Note: You do not need to execute this macro directly. dbt invokes `generate_schema_name` automatically during model materialization. When you run with `--target sandbox`, models materialize to the base schema (for example `STAGING`), and when you run with `--target regression`, models materialize to the suffixed schema (for example `STAGING_REGRESSION`).

Effects:
- Running with `--target sandbox` materializes to the base schema (e.g., `STAGING`).
- Running with `--target regression` materializes to the suffixed schema (e.g., `STAGING_REGRESSION`).

### `push_configs(release_notes_file, regression_config_file)`
Purpose: The macro creates (or replaces):
- Schema: `VALIDATION_REGRESSION`
- Stage: `VALIDATION_REGRESSION.CONFIGS` (JSON file format)
Then PUT two local JSON files into that stage:
- `release_notes_file`: describes impacted models/columns for the release
- `regression_config_file`: contains per-model regression configuration


Sample usage:

```bash
dbt run-operation push_configs --target regression \
--args '{"release_notes_file": "path/to/release_v1.1.json", 
         "regression_config_file": "path/to/regression_config.json"
        }'

```

Notes:
- Use absolute file paths, with the dbt product folder as the root.

Login to snowflake and test the files

```sql
ls @product_database.validation_regression.configs;

```
---

## dbt models

### `models/validation/regression_execution.py`
Purpose: Orchestrates the regression comparison for each impacted model. For each model listed in the uploaded release and config files, it:
- Builds a column list excluding changed columns
- Reads the reference dataset from `<database>.<schema>.<model>`
- Reads the regression dataset from `<database>.<schema>_regression.<model>`
- Sorts and (optionally) filters rows consistently
- Compares the two DataFrames and stores a text summary in `VALIDATION_REGRESSION.<model>`
- Writes logs to `VALIDATION_REGRESSION.REGRESSION_EXECUTION_LOG`

Sample usage:

```bash
# 1) Build regression models into validation_regression.
dbt run --target regression -s regression_outcome
```

# Note: 
For the result of this model to be materialised in the `validation_regression` schema, add the following selction to your dbt_project.yml

```
models:
  dbt_snf_regression_test:
    validation:
      +schema: validation      
      +materialized: table

```

Outputs:
- Comparison results per model in `VALIDATION_REGRESSION.<MODEL_NAME>` (single `results` text column)
- Execution log table `VALIDATION_REGRESSION.REGRESSION_EXECUTION_LOG`

### `models/validation/regression_outcome.py`
Purpose: Aggregates the individual results captured by `regression_execution` and returns a single-row table indicating whether all results meet a threshold. Currently, it checks the length of the stored `results` strings and returns `'TRUE'` only if all have the expected size (placeholder rule).

Sample usage:

```bash
dbt run --target regression -s regression_outcome
```

Output:
- Single-row table with a `result` column (`TRUE`/`FALSE`).

---

## Sample `regression_config.json`

The execution model expects a JSON list where each element configures a model to include in comparison. Example:

```json
[
  {
    "name": "MY_MODEL",
    "database": "MY_DATABASE",
    "schema": "STAGING",
    "filter_column": "BUSINESS_DATE",    
    "filter_operator": "==",
    "filter_column_value": "2025-01-01"
  },
  {
    "name": "ANOTHER_MODEL",
    "database": "MY_DATABASE",
    "schema": "STAGING"
  }
]
```

Notes:
- `name`, `database`, and `schema` are required per-model.
- Optional filter fields further restrict the comparison.
- Columns listed under `models_impacted` in the release file are automatically excluded from the comparison for the corresponding model.

---

## Profiles and targets

This project defines two Snowflake targets in `profiles.yml` (you will supply credentials):
- `sandbox`: builds the reference models into `<schema>`
- `regression`: builds the regression models into `<schema>_regression` (via the macro)

Example `profiles.yml` entry name must match the project name (`dbt_snf_regression_test`). See `profiles.yml` in this repo for the shape.

---

## End-to-end workflow (manual)

1) Build reference models: `dbt build --target sandbox -s <model_selector>`
2) Build regression models: `dbt build --target regression -s <model_selector>`
3) Push configuration files: `dbt run-operation push_configs --args '{"release_notes_file": "/path/to/release.json", "regression_config_file": "/path/to/regression_config.json"}'`
4) Execute regression: `dbt run -s regression_execution`
5) Summarize outcome: `dbt run -s regression_outcome`

---

## GitHub Actions workflows

Two reusable workflows are included to run each model. They assume Snowflake credentials are provided via secrets and will:
- Build reference and regression targets
- Upload configs (optional)
- Run the requested model

See files in `.github/workflows/` and examples in the dispatch forms.

