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
│                └── Write → silver.stg_accounts / stg_transactions / stg_loans
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
2. Pull the **PostgreSQL 15** image
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

### 4. Access the Airflow Web UI

Once the containers are running, open your browser and navigate to:

```
http://localhost:8080
```

Login with:
- **Username:** `admin`
- **Password:** `admin`

---

### 5. Configure the PostgreSQL Connection in Airflow

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

### 6. Activate and Run the Airflow DAG

#### Option A — From the Console (recommended)

All Airflow CLI commands run inside the `airflow` container via `docker compose exec`:

```bash
# 1. Activate (unpause) the DAG
docker compose exec airflow gosu airflow airflow dags unpause qversity_fintech_pipeline

# 2. Trigger the DAG
docker compose exec airflow gosu airflow airflow dags trigger qversity_fintech_pipeline
```

#### Monitoring from the console

```bash
# Check overall DAG run status
docker compose exec airflow gosu airflow airflow dags list-runs -d qversity_fintech_pipeline

# List all tasks in the DAG
docker compose exec airflow gosu airflow airflow tasks list qversity_fintech_pipeline

# Check the state of a specific task in the latest run
docker compose exec airflow gosu airflow airflow tasks states-for-dag-run qversity_fintech_pipeline <execution_date>

# View logs for a specific task
docker compose exec airflow gosu airflow airflow tasks logs qversity_fintech_pipeline setup_schemas <execution_date>
```

> Replace `<execution_date>` with the run ID shown by `dags list-runs` (e.g. `manual__2026-05-20T22:00:00+00:00`).

#### Option B — From the Web UI

1. Open **http://localhost:8080** and log in (`admin` / `admin`)
2. Find the DAG named **`qversity_fintech_pipeline`**
3. Toggle it **ON** (enable it)
4. Click the ▶ **Trigger DAG** button to run it manually
5. Monitor task progress in the **Graph View** or **Grid View**


---

### 7. Connect PowerBI to PostgreSQL

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

## 🧹 Stopping and Resetting

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
# Bronze

what i did

1. **Security**: By using `postgres_conn_id`, sensitive database credentials are kept out of the source code and managed securely within Airflow's encrypted metadata store.
2. **Resource Management**: It automates connection lifecycles, opening and closing the database session safely to prevent connection leaks without requiring boilerplate code.
3. **Atomicity**: The `.run()` method wraps the DDL script in a single transaction. If any schema or table creation fails, it triggers an automatic rollback, preventing partial or corrupted configurations.