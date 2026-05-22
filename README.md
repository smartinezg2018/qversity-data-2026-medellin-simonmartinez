# Qversity Technical Project 2026 — Fintech/Banking Data Pipeline

> **End-to-end ELT pipeline** for a fictional LATAM Fintech company using PySpark, Apache Airflow, dbt, PostgreSQL, and PowerBI, fully containerized with Docker.

---

## Author

| Field    | Info                          |
|----------|-------------------------------|
| **Name** | Simon Martinez                |
| **Email** | simonmartinez1820@gmail.com |
| **City** | Medellín, Colombia            |
| **Cohort** | Qversity 2026              |

---

## Architecture Overview

```
S3 (fintech_banking_dataset.json)
│
▼
Apache Airflow (DAG: qversity_fintech_pipeline)
│
▼
BRONZE — PostgreSQL (schema: bronze)
  └── raw_fintech_data (id, data jsonb, load_timestamp)
│
├──────────► PySpark (inside Airflow container)
│                └── Flatten arrays: accounts, transactions, loans
│                └── Deduplicate records
│                └── Write → silver.stg_customers / stg_accounts / stg_transactions / stg_loans
│
▼
dbt (dbt-core + dbt-postgres)
  ├── Silver models: clean, normalize, flatten nested objects
  └── Gold models:  analytics tables answering 24 business questions
│
▼
GOLD — PostgreSQL (schema: gold)
│
▼
PowerBI Desktop
  └── 4-page dashboard connected to gold schema
```

---

## Prerequisites

Make sure the following tools are installed on your machine **before** running the project:

| Tool | Version | Download |
|------|---------|----------|
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop |
| Docker Compose | Bundled with Docker Desktop | — |
| Git | Latest | https://git-scm.com/downloads |
| PowerBI Desktop | Latest | https://powerbi.microsoft.com/desktop |
| Python *(optional, for local dev)* | 3.11+ | https://www.python.org/downloads |

> **Windows users:** Make sure Docker Desktop is running and WSL 2 backend is enabled before proceeding.

> **Note:** Python, PySpark, Airflow, and dbt all run inside Docker containers — you do **not** need to install them locally unless you want to develop outside Docker.

---

## Environment Setup

### 1. Clone the Repository

```bash
git clone https://github.com/smartinezg2018/qversity-data-2026-medellin-simonmartinez.git
cd qversity-data-2026-medellin-simonmartinez
```

### 2. Configure Environment Variables

Copy the example environment file and fill in your values:

```bash
# Linux / macOS
cp env.example .env

# Windows (PowerShell)
Copy-Item env.example .env
```

Open `.env` and review the variables:

```env
# PostgreSQL Configuration
POSTGRES_USER=qversity-admin
POSTGRES_PASSWORD=qversity-admin
POSTGRES_DB=qversity

# Airflow Admin UI Configuration (Optional, but good practice to externalize)
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

> **Never commit `.env` to Git.** It is already listed in `.gitignore`.  
> The `env.example` file is safe to commit — it contains no real secrets.

---

### 3. Build and Start All Services

```bash
docker compose up -d --build
```

This command will:
1. Build the **Airflow** image (installs Java, PySpark, Docker CLI, and the PostgreSQL JDBC driver) and start the **dbt** service (dbt-core + dbt-postgres)
2. Pull the **PostgreSQL** image
3. Start all containers in detached mode

> The first build may take **5–10 minutes** due to dependency installation.

#### Verify Services Are Healthy

```bash
docker compose ps
```

Expected output (all services should show `running` or `healthy`):

```
NAME                  STATUS          PORTS
qversity-postgres-1   running (healthy)   0.0.0.0:5432->5432/tcp
qversity-airflow-1    running         0.0.0.0:8080->8080/tcp
qversity-dbt-1        running
```

If PostgreSQL is not yet healthy, wait 30 seconds and run `docker compose ps` again.

---


### 4. Configure the PostgreSQL Connection in Airflow

Before triggering the DAG, set up the Airflow → PostgreSQL connection:

1. In the Airflow UI, go to **Admin → Connections**
2. Click **+** to add a new connection
3. Fill in the fields:

| Field | Value |
|-------|-------|
| Connection Id | `postgres_default` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Schema | `qversity` |
| Login | `qversity-admin` |
| Password | `qversity-admin` |
| Port | `5432` |

4. Click **Save**

---

### 5. Activate and Run the Airflow DAG

#### Option A — From the Console (recommended)

All Airflow CLI commands run inside the `airflow` container via `docker compose exec`:

```bash
# 1. Activate (unpause) the DAG
docker compose exec -u airflow airflow airflow dags unpause qversity_fintech_pipeline
# 2. Trigger the DAG
docker compose exec -u airflow airflow airflow dags trigger qversity_fintech_pipeline
```

#### Monitoring from the console

```bash
# Check overall DAG run status
docker compose exec -u airflow airflow airflow dags list-runs -d qversity_fintech_pipeline

# List all tasks in the DAG
docker compose exec -u airflow airflow airflow tasks list qversity_fintech_pipeline

# Check the state of a specific task in the latest run
docker compose exec -u airflow airflow airflow tasks states-for-dag-run qversity_fintech_pipeline <execution_date>

# View logs for a specific task
docker compose exec -u airflow airflow airflow tasks log qversity_fintech_pipeline setup_schemas <execution_date>
```

> Replace `<execution_date>` with the run ID shown by `dags list-runs` (e.g. `manual__2026-05-20T22:00:00+00:00`).

#### Option B — From the Web UI

1. Open **http://localhost:8080** and log in (`admin` / `admin`)
2. Find the DAG named **`qversity_fintech_pipeline`**
3. Toggle it **ON** (enable it)
4. Click the **Trigger DAG** button to run it manually
5. Monitor task progress in the **Graph View** or **Grid View**


---

### 6. Connect PowerBI to PostgreSQL

1. Open **PowerBI Desktop**
2. Click **Get Data → PostgreSQL database**
3. Enter the connection details:

| Field | Value |
|-------|-------|
| Server | `localhost` |
| Database | `qversity` |

4. Select **DirectQuery** or **Import** mode
5. Navigate to the **`gold`** schema and import the analytics tables
6. Build your dashboards on top of the Gold layer

> **Note:** PowerBI requires the **Npgsql PostgreSQL driver** to connect. Download it from https://github.com/npgsql/npgsql/releases if prompted.

---

##  Stopping and Resetting

### Stop all containers (keep data)

```bash
docker compose down
```

### Stop and remove all data volumes (full reset)

```bash
docker compose down -v
```

> Using `-v` will **delete the PostgreSQL database volume**. You will need to re-run the DAG to reload data.

### View container logs

```bash
# All services
docker compose logs -f

# Only Airflow
docker compose logs -f airflow

# Only PostgreSQL
docker compose logs -f postgres
```


All runtime dependencies are pre-installed inside the Docker images. For reference:

| Package | Version | Purpose |
|---------|---------|---------|
| `apache-airflow` | 2.7.3 | Pipeline orchestration |
| `apache-airflow-providers-postgres` | 5.6.0 | Airflow → PostgreSQL operator |
| `pyspark` | 3.5.0 | Array flattening & deduplication |
| `dbt-core` | 1.7.0 | SQL-based transformations & testing |
| `dbt-postgres` | 1.7.0 | dbt adapter for PostgreSQL |
| `psycopg2-binary` | 2.9.7 | Python → PostgreSQL connector |
| `boto3` | 1.28.0 | AWS S3 download |
| `pandas` | 2.0.3 | Data manipulation |
| `python-dotenv` | 1.0.0 | `.env` file loading |

---
# Technical Decisions
## Bronze


### JSONB as the Storage Format in Bronze

Since the dataset is a JSON file with variable structure, storing each record as `JSONB` in PostgreSQL was the most pragmatic choice. It preserves the full fidelity of the original data and allows exploratory queries directly on the raw layer without requiring a rigid schema upfront. Normalization happens in later layers (Silver/Gold), not here.

### S3 Download with Unsigned (Public) Access

The bucket is publicly accessible, so `signature_version=UNSIGNED` was set on the boto3 client. This avoids the need to manage AWS credentials for a source that doesn't require them, keeping both the local environment and the Airflow production setup simple.


### One operator per task

Each pipeline step is encapsulated in its own operator: `PythonOperator` for schema setup, S3 download, and Bronze load; `BashOperator` for the PySpark Silver staging job (`run_spark_silver`). This follows the single-responsibility principle within Airflow—each concern can fail, be retried, and be monitored independently.

### PostgresHook for Schema Management and Data Loading

Rather than managing raw `psycopg2` connections manually, **PostgresHook** was used throughout the pipeline. Since the project runs on Airflow, `PostgresHook` is the natural fit: it reads the connection details directly from the `postgres_default` connection configured in the Airflow UI, which means no credentials are hardcoded in the code and the same DAG works across local, staging, and production environments by simply changing the connection in Airflow.

For schema setup, `hook.run()` handles the DDL statements in a single call with automatic connection lifecycle management — no need to manually open, commit, or close a connection. The `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` guards make this task safely re-runnable on every DAG execution without side effects.

For data loading, `hook.insert_rows()` performs a bulk insert in a single operation rather than looping over individual `INSERT` statements, which is significantly more efficient for large datasets. Combined with the `TRUNCATE` at the start of the task, this gives the load step a clean, predictable behavior on every run.

### Bronze Table Structure: `bronze.raw_fintech_data`

The table was designed to be as simple and flexible as possible, since the Bronze layer's only responsibility is to store raw data without losing any information.

| Column | Type | Description |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | Auto-incremented surrogate key. Uniquely identifies each ingested record without relying on any field from the source data. |
| `data` | `JSONB NOT NULL` | The raw JSON record exactly as it came from the source file. JSONB allows direct querying of nested fields in later exploration stages. |
| `load_timestamp` | `TIMESTAMP DEFAULT NOW()` | Batch ingest time for the DAG run (one value per full reload). Useful for auditing when Bronze was last loaded; not used in Silver dedup. |

## Silver

PySpark runs inside the Airflow container (`run_spark_silver` task) and rebuilds the Silver staging layer from Bronze on every DAG run: flatten nested arrays, deduplicate by business keys, and write relational tables back to PostgreSQL via JDBC. Cleaning, typing, and analytics modeling stay in dbt.

| File | Role |
|------|------|
| `spark/script.py` | Entry point: builds and writes all four `silver.stg_*` tables |
| `spark/schemas.py` | `StructType` definitions for `from_json` when parsing Bronze JSONB |
| `spark/spark_utils.py` | Spark session, JDBC config, Bronze read, dedup helper, JDBC overwrite writer |

### Defining schemas in PySpark

Bronze stores each record as JSONB, which is correct—but ingest is not guaranteed to be consistent (numbers as strings, mixed formats, nulls, and so on). If PySpark used `IntegerType`, `DoubleType`, `DateType`, etc. at parse time, `from_json` would fail or null out values that do not match strictly.

In `schemas.py`, every parsed field—including nested structs and array elements—is typed as `StringType`. That keeps the full payload visible in Spark even when a value is malformed for its logical type. Type casting, date normalization, and category cleanup are deferred to dbt, where EDA-driven rules (mixed date formats, sentinels like `'null'` and `'N/A'`) can be applied and tested in one place.

### `get_spark_session` and `get_jdbc_config`

The transformation code should not have to care where it is running or how it reaches the database. One function spins up Spark in local mode with the PostgreSQL JDBC driver on the classpath; the other reads host, database, and credentials from environment variables.

The result is that every staging write lands in the same PostgreSQL instance the raw data came from, with no configuration scattered across the codebase to keep in sync. `read_bronze()` uses the same JDBC connection to pull `bronze.raw_fintech_data` in parallel partitions when the table is large enough, then parses the `data` column with `final_schema` from `schemas.py`.

### `write_silver`

Each run fully overwrites the four staging tables below via JDBC (`mode="overwrite"`). Staging is not meant to grow forever or merge row-by-row. Each Airflow run means “rebuild Silver staging from Bronze.” Overwrite keeps that mental model simple: either the job finished and all tables are fresh, or it failed and you fix and rerun—you are not debugging half-old, half-new staging data.

| Staging table | Source in JSON | PySpark transform | Dedup key |
|---------------|----------------|-------------------|-----------|
| `silver.stg_customers` | Flat customer fields + nested objects | One row per customer; objects kept as JSON strings (see below) | `customer_id` |
| `silver.stg_accounts` | `accounts[]` | `explode_outer(accounts)`; drop rows where `account_id` is null | `account_id` |
| `silver.stg_loans` | `loans[]` | `explode_outer(loans)`; drop rows where `loan_id` is null | `loan_id` |
| `silver.stg_transactions` | `transactions[]` | `explode_outer(transactions)`; drop rows where `transaction_id` is null | `transaction_id` |

Dedup uses Bronze row `id` only inside Spark for tie-breaking, then drops it before writing—`id` is not a column in the staging tables. Silver staging does not carry `load_timestamp`; that column stays on Bronze as batch-level ingest metadata (one value per full reload).

### Dedup

Rows with the same business key (`customer_id`, `account_id`, `loan_id`, or `transaction_id`) count as duplicates within the current Bronze snapshot. We keep one row per key, choosing the row with the highest Bronze `id` (last inserted row for that key), using a window function in `spark_utils.dedup()`.

Because each DAG run truncates and reloads Bronze with a single `load_timestamp` for the whole batch, timestamp does not discriminate between rows in the same run—only `id` does. Bronze can still hold the same business ID more than once in the raw file (or after `explode_outer`). Staging needs one row per entity so dbt’s uniqueness tests and joins do not break. Staging is fully rebuilt each run, so there is no merge logic across partial runs.

### `credit_info` and `digital_engagement` as JSON strings

PySpark’s job in Silver is to flatten nested **arrays** into relational rows. `accounts[]`, `transactions[]`, and `loans[]` need `explode` (or `explode_outer` when the array may be empty) so each child record gets its own row. `credit_info{}` and `digital_engagement{}` are different: they are single objects per customer, already at the same grain as `stg_customers`.

Flattening those structs into many columns inside Spark would work, but it would push type casting, null handling, and category cleanup into the same step that only needs to deduplicate and land staging data. EDA showed mixed date formats (for example `20251102` vs `17/03/2026` on login dates), string sentinels, and numeric fields stored as text—exactly the kind of cleanup dbt is meant to own.

In `script.py`, both objects are written with `to_json()` into `silver.stg_customers` as plain text columns. That keeps the full nested payload intact through JDBC without inventing a wide, strongly typed Spark schema for fields that still need validation. dbt reads those strings, parses them with JSON operators, and flattens them into proper relational columns with casts, accepted-value tests, and documented rules—the same pattern as deferring strict types to dbt for the flat customer fields.

### dbt Silver models

After Spark lands `silver.stg_*`, the Airflow DAG runs `dbt seed` (lookup CSVs into schema `raw`) and `dbt run --select silver`. Seven models materialize as tables in schema `silver`: four dimensions and three facts. Macros in `dbt/macros/` centralize the EDA-driven cleanup rules so models stay readable and tests in `schema.yml` can target stable output values.

| Model | Source | Role |
|-------|--------|------|
| `dim_customer` | `stg_customers` | Customer profile: names, contact, geo, KYC, risk, segment, status |
| `dim_credit_info` | `stg_customers` (`credit_info` JSON) | 1:1 credit profile per customer |
| `dim_digital_engagement` | `stg_customers` (`digital_engagement` JSON) | 1:1 digital behavior per customer |
| `fct_account` | `stg_accounts` | Account balances and lifecycle |
| `fct_transactions` | `stg_transactions` | Transaction events |
| `fct_loans` | `stg_loans` | Loan portfolio snapshots |
| `dim_date` | Union of all parsed dates from the models above | Conformed date dimension (`date_key` = `YYYYMMDD`) |

`dim_date` is built last in the DAG graph: it unions every non-null date from customer, account, transaction, loan, and engagement models, then applies `date_key`. Downstream models reference `dim_date` via `relationships` tests on `*_date_key` columns.

### dbt macros

Macros are Jinja SQL fragments invoked as `{{ macro_name('column') }}`. Lower-level helpers (`safe_numeric_*`) are composed inside higher-level macros (`clamp_numeric`, `clamp_numeric_int`) rather than called directly from every model.

#### Primitive parsing and typing

**`parse_date(column_expr)`**

Staging still stores dates as text with mixed formats. This macro tries, in order: ISO `YYYY-MM-DD`, compact `YYYYMMDD`, US `MM-DD-YYYY`, and `DD/MM/YYYY`. Anything else becomes `NULL` so bad values do not break casts or `date_key`.

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `date_of_birth`, `registration_date` |
| `dim_digital_engagement` | `last_login_date` (from `de->>'last_login_date'`) |
| `fct_account` | `opened_date` |
| `fct_transactions` | `date` |
| `fct_loans` | `start_date`, `end_date` |

**`safe_string(column_expr)`**

Trims whitespace and maps sentinels (`''`, `N/A`, `NA`, `null`, `nan`) to `NULL`. Used for categorical or free-text fields that should not carry placeholder strings into analytics.

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `gender`, `nationality`, `country`, `address`, `relationship_manager` |
| `fct_account` | `account_type` |
| `fct_transactions` | `category`, `merchant`, `description` |
| `fct_loans` | `collateral_type` |

**`safe_numeric(column_expr)`**, **`safe_numeric_int(column_expr)`**, **`safe_numeric_signed(column_expr)`**

Shared null/sentinel handling, then regex validation before cast. `safe_numeric` allows non-negative decimals; `safe_numeric_int` casts to integer (fractional strings still match the regex); `safe_numeric_signed` allows a leading sign and casts to `numeric`. These are building blocks—models usually call `clamp_*` or `clean_currency` instead.

| Macro | Used directly in silver models |
|-------|--------------------------------|
| `safe_numeric` | `dim_digital_engagement` → `avg_monthly_logins` |
| `safe_numeric_int` | `dim_credit_info` → account counts, ages, payments, inquiries; `fct_loans` → `term_months`, `days_past_due` |
| `safe_numeric_signed` | Inside `clamp_numeric` only (not called from model SQL) |

**`clamp_numeric(column_expr, min_val, max_val)`**

Parses via `safe_numeric_signed`, keeps the value only if it lies in `[min_val, max_val]`, otherwise `NULL`. Prevents impossible coordinates or rates from entering facts/dims.

| Silver model | Columns | Range |
|--------------|---------|-------|
| `dim_customer` | `lat`, `lon`, `risk_score` | −90..90, −180..180, 0..100 |
| `dim_credit_info` | `utilization_pct` | 0..100 |
| `fct_account` | `interest_rate` | 0..100 |
| `fct_loans` | `interest_rate` | 0..100 |

**`clamp_numeric_int(column_expr, min_val, max_val)`**

Same pattern as `clamp_numeric`, but uses `safe_numeric_int` and integer bounds. Used where the business rule is a whole number in a fixed band.

| Silver model | Columns | Range |
|--------------|---------|-------|
| `dim_credit_info` | `credit_score` | 350..850 |

**`clean_currency(column_expr)`**

Strips `$` and `USD`, normalizes comma decimals to dot, validates a numeric pattern (including scientific notation), then casts to `numeric`. Invalid money strings become `NULL`.

| Silver model | Columns |
|--------------|---------|
| `dim_credit_info` | `total_limit`, `total_used` |
| `fct_account` | `balance`, `credit_limit` |
| `fct_transactions` | `amount` |
| `fct_loans` | `principal`, `outstanding_balance`, `monthly_payment` |

**`cast_to_boolean(column_name)`**

Maps common truthy/falsy string tokens (`true`/`1`/`yes`/`si`, etc.) to PostgreSQL `boolean`; unknown values → `NULL`. Schema tests expect boolean columns stored as `'true'`/`'false'` in accepted-value tests.

| Silver model | Columns |
|--------------|---------|
| `dim_credit_info` | `bankruptcy_flag` |
| `dim_digital_engagement` | `mobile_app_registered`, `web_banking_registered`, `push_notifications`, `paperless_statements` |

#### Name, contact, and email hygiene

**`proper_case(column_expr)`**

Nulls sentinels like `safe_string`, then `initcap(lower(...))` with collapsed whitespace. Standardizes person names without manual per-row fixes.

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `first_name`, `last_name` |

**`clean_email(column_expr)`**

Lowercases, removes whitespace and `#`, collapses duplicate `@`, trims edge dots. Does not drop invalid emails—that is left to `is_valid_email` for a separate flag column.

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `email` |

**`clean_phone(column_expr)`**

After sentinel nulling, keeps only values that match E.164-style `+` and 10–15 digits (spaces, dashes, and parentheses stripped for the check).

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `phone_number` |

**`is_valid_email(column_expr)`**

Runs on the **already cleaned** email. Returns `false` for null, internal spaces, bad `@` placement, or regex mismatch; otherwise `true`. Lets dashboards filter on validity without discarding the raw-ish cleaned string.

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `is_email_valid` (from `email`) |

#### Category normalization via seeds

**`map_from_seed(column_name, seed_name)`**

At compile time, loads `raw.<seed_name>` (dbt seeds) and emits a `CASE` mapping `lower(trim(source))` to `normalized_value`. Unmapped values pass through as `trim(column_name)` so new source codes are visible rather than silently dropped. Seeds are versioned CSVs under `dbt/seeds/` and loaded before models run.

| Silver model | Source column | Seed |
|--------------|---------------|------|
| `dim_customer` | `city` | `city_name_mapping` |
| `dim_customer` | `kyc_status` | `kyc_status_mapping` |
| `dim_customer` | `customer_segment` | `customer_segment_mapping` |
| `dim_customer` | `status` | `customer_status_mapping` |
| `fct_account` | `status` | `account_status_mapping` |
| `fct_loans` | `type` | `loan_type_mapping` |
| `fct_loans` | `status` | `loan_status_mapping` |
| `fct_transactions` | `type` | `transaction_type_mapping` |
| `fct_transactions` | `status` | `status_mapping` |

#### Derived attributes and conformed keys

**`calculate_years(date_column)`**

Uses PostgreSQL `age(current_date, date_column)` and extracts whole years. Applied only after `parse_date` so the input is a real `date`.

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `age` (from `date_of_birth`), `tenure` (from `registration_date`) |

**`age_bucket(age_column)`**

Bands customer age for segmentation and `accepted_values` tests in `schema.yml`.

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `age_bucket` (from `age`) |

**`risk_tier(risk_score_column)`**

Maps clamped `risk_score` to Low / Medium / High / Critical / Unknown.

| Silver model | Columns |
|--------------|---------|
| `dim_customer` | `risk_tier` (from `risk_score`) |

**`credit_score_bucket(credit_score_column)`** and **`utilization_bucket(utilization_column)`**

Standard FICO-style score bands and utilization tiers on top of clamped metrics in `dim_credit_info`.

| Silver model | Columns |
|--------------|---------|
| `dim_credit_info` | `credit_score_bucket`, `utilization_bucket` |

**`date_key(date_column)`**

Surrogate integer key `YYYYMMDD` for joining to `dim_date` without storing redundant calendar attributes on every fact/dimension row.

| Silver model | Columns |
|--------------|---------|
| `dim_date` | `date_key` (from `full_date`) |
| `dim_customer` | `registration_date_key` |
| `dim_digital_engagement` | `last_login_date_key` |
| `fct_account` | `opened_date_key` |
| `fct_transactions` | `transaction_date_key` |
| `fct_loans` | `start_date_key`, `end_date_key` |

#### Project configuration (not used in model SQL)

**`generate_schema_name(custom_schema_name, node)`**

Overrides dbt’s default schema naming so `+schema: silver` in `dbt_project.yml` lands models in the `silver` schema instead of `<target_schema>_silver`. No silver model calls this macro directly; it applies to every dbt node at build time.

#### Columns without a dedicated macro

Some staging fields are passed through intentionally or pending further EDA:

| Silver model | Column | Notes |
|--------------|--------|-------|
| `dim_customer` | `customer_id` | Business key from staging; uniqueness enforced in tests |
| `fct_account` | `currency`, `branch_code` | No cleaning macro |
| `fct_transactions` | `currency`, `channel` | `channel` tested via `accepted_values` only |
| `fct_loans` | `loan_id`, `customer_id` | Keys only in `cleaned` CTE |
| `dim_digital_engagement` | `preferred_channel` | Raw JSON text; not `safe_string` |