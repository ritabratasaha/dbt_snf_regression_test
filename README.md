# 🧪 dbt_snf_regression_test

A regression testing framework for Snowflake-backed dbt projects using dbt Python models. It compares reference models to regression models (materialized into parallel schemas with `_regression` suffix) and records any differences.

> **Note**: This package cannot be executed on its own. It must be imported into a dbt project building a data product on Snowflake.

## Table of Contents

- [Components](#components)
- [Purpose](#purpose)
- [Dependencies](#dependencies)
- [Project Artifacts](#project-artifacts)
- [Implementation Steps](#implementation-steps)

## 📦 Components

- **Release Template**: JSON template to declare impacted dbt models/columns
- **Macros**: 
  - `push_configs`: Upload config files to Snowflake stage
  - `generate_schema_name`: Create schema with `_regression` suffix when target is regression in your dbt command
- **Python Models**:
  - `data_type_validation`: Schema-level validation (data types, lengths, precision)
  - `regression_execution`: Data content comparison between reference and regression models
  - `regression_outcome`: Overall test result aggregation

---

## 🎯 Purpose

- **Schema Validation**: Compare data types, lengths, and precision between reference and regression model schemas to ensure structural consistency.
- **Data Regression Testing**: Run comprehensive data regression tests between two Snowflake tables for the same model: the reference version in the base schema and the regression version in a sibling schema with a `_regression` suffix.
- **Dynamic Configuration**: Upload and read config and release metadata from a Snowflake stage to dynamically determine which models and columns are included in a given test run.
- **Audit Trail**: Persist all comparison results and execution logs for comprehensive auditing and troubleshooting.

---

## 📋 Dependencies

Required Python packages (auto-installed via dbt model config):

- `snowflake-snowpark-python` - Snowflake Snowpark Python API
- `yaml` - YAML file parsing
- `pandas` - Data manipulation and analysis
- `modin` - Modin is a Python library designed to accelerate and scale Pandas workflows, particularly when dealing with large datasets. It acts as a drop-in replacement for Pandas, meaning users can often achieve significant performance improvements by simply changing the import statement in their code.

These packages are automatically installed when the Python models run, as they are specified in the `dbt.config()` calls within each model.

<br />

# 🔧 Project Artifacts

## 📄 1. Release Template

**Purpose**: Declare which existing models changed in a release and which columns changed in each model.

> Only existing models participate in regression testing.

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

### 📋 Usage

1. Copy template to real file (e.g., `/releases/release_v1.0.json`) and fill in impacted models/columns
2. Upload to Snowflake using the `push_configs` macro

> **Tip**: Use GitHub MCP for automation - create a PR and share the URL with the MCP server

---

## ⚙️ 2.dbt Macros

### 🏗️ `generate_schema_name(custom_schema_name, node)`

**Purpose**: Append `_regression` to target schema when using `--target regression`

- `--target sandbox` → Base schema (e.g., `STAGING`)
- `--target regression` → Suffixed schema (e.g., `STAGING_REGRESSION`)

> **Note**: dbt invokes this macro automatically during model materialization

### 📤 `push_configs(release_notes_file, regression_config_file)`

**Purpose**: Upload configuration files to Snowflake stage

**Creates**:
- Schema: `validation_regression`
- Stage: `validation_regression.configs`

**Uploads**:
- `release_notes_file`: Impacted models/columns for the release
- `regression_config_file`: Per-model regression configuration


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

## 🐍 3. dbt Python Models

### 🏗️ `data_type_validation.py`

**Purpose**: Schema-level validation to ensure structural consistency

**Validation checks**:
- Data type comparison between schemas
- Character length validation for VARCHAR/TEXT columns
- Numeric precision and radix validation
- Column presence verification

**Workflow**:
1. Read column metadata from `INFORMATION_SCHEMA.COLUMNS`
2. Perform left join to identify mismatches
3. Compare data types, lengths, precision, and radix values
4. Return pass/fail status with detailed failure information

**Outputs:**
- Validation results per model in `validation_regression.data_type_validation` (timestamp, model, status, message columns)
- Execution log table `validation_regression.data_type_validation_log`

Sample usage:

```bash
dbt run --target regression -s data_type_validation
```

**Technical Notes:**
- Uses `INFORMATION_SCHEMA.COLUMNS` for metadata comparison
- Handles NULL values with `NVL()` functions for robust comparison
- Returns detailed failure information for troubleshooting
- Creates separate log table for audit trail

### 🔄 `regression_execution.py`

**Purpose**: Orchestrate regression comparison for each impacted model

**Workflow**:

1. **Pre-execution validation**: Check required schema and config files exist
2. **Configuration parsing**: Auto-select latest release file, parse impacted models/columns
3. **Model processing**: For each model:
   - Exclude changed columns from comparison
   - Read reference and regression datasets
   - Apply optional filtering and sorting
   - Compare DataFrame sizes and perform row-by-row comparison
   - Store results in `validation_regression.<model>` (first 10 differences)
   - Log progress to `validation_regression.regression_execution_log`
4. **Error handling**: Extensive logging, graceful error handling, SQL injection protection

Sample usage:

```bash
# Run the regression execution model
dbt run --target regression -s regression_execution
```

### Note: 
For the results of these models to be materialised in the `validation_regression` schema, add the following configuration to your dbt_project.yml

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



### 📊 `regression_outcome.py`

**Purpose**: Aggregate individual results into overall regression test status

**Current logic**: Checks length of stored `results` strings and returns `'TRUE'` if all have expected size

Sample usage:

```bash
dbt run --target regression -s regression_outcome
```

Output:
- Single-row table with a `result` column (`TRUE`/`FALSE`).

---
<br />

# 🚀 How will you implementation regression in your data product ?

## 📥 1. Import Package in your data product dbt project

Example:

```yml
packages:
  - git: "https://github.com/ritabratasaha/dbt_snf_regression_test.git"
    revision: main
```

Use dbt deps to download all dependencies in the data product package

``` dbt deps ```

---

## ⚙️ 2. Configure Profiles

Create a Snowflake target in your data product `profiles.yml`:
- `regression`: builds models into `<schema>_regression` (via macro)

Example (Assumption: data product dbt project name is sample_dbt):

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

## 📝 3. Create Release Notes

Refer to [Release Template](#-1-release-template)

## ⚙️ 4. Create Regression Config

The filters are optional. However they help you to keep the dataset size within limit. Else the snowflake warehouse will need vertical scaling
> **Note**: One-time setup for each model participating in regression testing

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

## ☁️ 5. Upload Configs to Snowflake

Upload release notes and regression config using the [`push_configs` macro](#-push_configsrelease_notes_file-regression_config_file)

## 🏗️ 6. Build Reference Models

Assumption: Env where regression is performed is sandbox

```bash 
dbt run --target sandbox --select staging marts
```

## 🔄 7. Build Regression Models

```bash 
dbt run --target regression --select staging marts
```

## 🧪 8. Execute Regression Tests

```bash 
dbt run --target regression --select data_type_validation
dbt run --target regression --select regression_execution 
dbt run --target regression --select regression_outcome 
```

## ✅ 9. Verify Results

```sql
-- Check data type validation results
SELECT * FROM <database>.validation_regression.data_type_validation;

-- Check regression execution results  
SELECT * FROM <database>.validation_regression.regression_execution;

-- Check overall regression outcome
SELECT * FROM <database>.validation_regression.regression_outcome;

-- Check execution logs
SELECT * FROM <database>.validation_regression.data_type_validation_log;
SELECT * FROM <database>.validation_regression.regression_execution_log;
```
Once you are done with all these, login to Snowflake and you should see similar structures as below: 

![Sample Database Objects Part1](/sample_screenshots/database_output_part1.png)

![Sample Database Objects Part2](/sample_screenshots/database_output_part2.png)

---

That is it !!. Hope you find this useful..

