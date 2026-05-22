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
1. Build the **Airflow** image (installs Java, PySpark, dbt, and the PostgreSQL JDBC driver)
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