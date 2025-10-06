# 🔵 dbt_snf_regression_test

This package provides a simple regression-testing workflow for Snowflake-backed dbt projects using dbt Python models. It compares a set of reference models to a corresponding set of regression models (materialized into a parallel schema with a `_regression` suffix) and records any differences.
This package cannot be executed on its own. It needs to be imported into a dbt project that is building a data product on snowflake.

The project includes:
- A release note JSON template to declare impacted dbt models/columns
- Macros to manage schemas and stage configuration files in Snowflake
- Two dbt Python models to execute and summarize regression results

---

## 🔵 Purpose of the package

- Run data regression tests between two Snowflake tables for the same model: the reference version in the base schema and the regression version in a sibling schema with a `_regression` suffix.
- Upload and read config and release metadata from a Snowflake stage to dynamically determine which models and columns are included in a given test run.
- Persist the comparison results for auditing.

---

## 🔵 Dependencies

This package requires the following Python packages to be available in your Snowflake environment:

- `snowflake-snowpark-python` - Snowflake Snowpark Python API
- `yaml` - YAML file parsing
- `pandas` - Data manipulation and analysis
- `modin` - High-performance pandas alternative for large datasets

These packages are automatically installed when the Python models run, as they are specified in the `dbt.config()` calls within each model.

<br />

# 🔵 Lets study the primary project artefacts

## 🔵 1. Release template

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

## 🔵 2. Macros

### a. `generate_schema_name(custom_schema_name, node)`
Purpose: Append `_regression` to the target schema when the active dbt target is `regression`. This lets you materialize the same models into a parallel schema for A/B comparison without changing model code.

Note: You do not need to execute this macro directly. dbt invokes `generate_schema_name` automatically during model materialization. When you run with `--target sandbox`, models materialize to the base schema (for example `STAGING`), and when you run with `--target regression`, models materialize to the suffixed schema (for example `STAGING_REGRESSION`).

Effects:
- Running with `--target sandbox` materializes to the base schema (e.g., `STAGING`).
- Running with `--target regression` materializes to the suffixed schema (e.g., `STAGING_REGRESSION`).

### b. `push_configs(release_notes_file, regression_config_file)`
Purpose: The macro creates (or replaces):
- Schema: `validation_regression`
- Stage: `validation_regression.configs` (JSON file format)
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

## 🔵 3. dbt models to perform regression test

### a. `models/validation/regression_execution.py`
Purpose: Orchestrates the regression comparison for each impacted model. This model performs comprehensive regression testing with the following workflow:

Here are some of the high level functions performed by the model :

**Pre-execution validation:**
- Checks if the required schema and config files exist

**Dynamic configuration parsing:**
- Automatically selects the latest release file from the stage based on modification date
- Parses release notes to identify impacted models and changed columns
- Reads regression configuration to determine which models to test

**Regression For each impacted model:**
- Builds a column list excluding changed columns (from release notes)
- Reads the reference dataset from `<database>.<schema>.<model>`
- Reads the regression dataset from `<database>.<schema>_regression.<model>`
- Applies optional filtering (by column, operator, value) if configured
- Sorts data consistently for comparison
- Compares DataFrame sizes before detailed comparison
- Performs row-by-row comparison using pandas `.compare()`
- Stores detailed results in `validation_regression.<model>` (truncated to first 10 differences)
- Writes comprehensive logs to `validation_regression.regression_execution_log`. This log table is updated at every step so that the progress of the model can be found incase of any failure

**Error handling:**
- Extensive logging throughout execution
- Graceful handling of missing schemas, files, or data mismatches
- SQL injection protection for log messages

Sample usage:

```bash
# Run the regression execution model
dbt run --target regression -s regression_execution
```

### Note: 
For the result of this model to be materialised in the `validation_regression` schema, add the following selction to your dbt_project.yml

```
models:
  dbt_snf_regression_test:
    validation:
      +schema: validation      
      +materialized: table

```

Outputs:
- Comparison results per model in `validation_regression.<MODEL_NAME>` (single `results` text column)
- Execution log table `validation_regression.regression_execution_log`

**Technical Notes:**
- Results are limited to the first 10 differences for performance
- Uses `eval()` for dynamic pandas command execution (ensure trusted data sources)
- Creates temporary file format `validation_regression.config_format` for JSON parsing
- Only processes models that appear in both release notes and regression config
- Handles data size mismatches gracefully with detailed error messages

### b. `models/validation/regression_outcome.py`
Purpose: Aggregates the individual results captured by `regression_execution` and returns a single-row table indicating the status of the overall regression test. 
Currently, it checks the length of the stored `results` strings and returns `'TRUE'` only if all have the expected size (placeholder rule).

Sample usage:

```bash
dbt run --target regression -s regression_outcome
```

Output:
- Single-row table with a `result` column (`TRUE`/`FALSE`).

---
<br />

# 🔵 Steps to execute regression on a data product

## 🔵 1. Import this project into your data product repo by adding the repo details to your package.yml

Example:

```yml
packages:
  - git: "https://github.com/ritabratasaha/dbt_snf_regression_test.git"
    revision: main
```

Use dbt deps to download all dependencies in the data product package

``` dbt deps ```

---

## 🔵 2. Setup profiles and targets of your data product repo

Create a Snowflake target in `profiles.yml` (you will supply credentials):
- `regression`: builds the regression models into `<schema>_regression` (via the macro)

Example:

```yml
sample_dbt:
  target: sandbox
  outputs:
    sandbox:
      type: snowflake
      account: ""
      user: ""
      password: ""
      role: ""
      database: sample_dbt
      warehouse: compute_wh
      schema: staging
      threads: 4
      client_session_keep_alive: false
      query_tag: dbt_sample_dbt_sandbox
    regression:
      type: snowflake
      account: ""
      user: ""
      password: ""
      role: ""
      database: sample_dbt
      warehouse: compute_wh
      schema: staging
      threads: 4
      client_session_keep_alive: false
      query_tag: dbt_sample_dbt_regression
```


---

## 🔵 3. Create the release notes from release template

Refer to [Release Notes](#1-release-template)

## 🔵 4. Create the regression config file

This file is owned by the data product repo. This is a one time effort and needs to be created meticulously for each model that needs to participate in the regression process.

Example : 

```JSON
[
  {
    "name": "my_model",
    "database": "sample_dbt",
    "schema": "marts",
    "filter_column": "business_date",    
    "filter_operator": "==",
    "filter_column_value": "2025-01-01"
  },
  {
    "name": "annual_sales_by_region",
    "database": "sample_dbt",
    "schema": "marts",
    "filter_column": "sale_year",
    "filter_operator": "==",
    "filter_column_value": "2024"
  }
]

```

## 🔵 5. Upload configs to snowflake

Once [3](#3-create-the-release-notes-from-release-template) and [4](#4-create-the-regression-config-file) are created we need to upload them to snowflake using the [macro](#push_configsrelease_notes_file-regression_config_file)

## 🔵 6. Run the dbt models of the data product on its original schema

```bash 
dbt run --target sandbox --select staging marts
```

## 🔵 7. Run the dbt dbt models of the data product on the _regression schema

```bash 
dbt run --target regression --select staging marts
```

## 🔵 8. Run the dbt models of the imported regression package

```bash 
dbt run --target regression --select regression_execution 
dbt run --target regression --select regression_outcome 
```

## 🔵 9. Verify the data from the snowflake tables

```sql
Select * from <database>.validation_regression.regression_execution;
Select * from <database>.validation_regression.regression_outcome
```

---


