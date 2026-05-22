# Qversity Technical Project v2
## Fintech/Banking Data Pipeline with PySpark, dbt & PowerBI

---

## 1. Objectives

Design and deliver an **end-to-end ELT data pipeline** for a Fintech/Banking use case that:

- Ingests a raw JSON dataset from **AWS S3**
- Implements a **Bronze / Silver / Gold** architecture on **PostgreSQL**
- Uses **Apache Airflow** for orchestration
- Uses **PySpark** (inside the Airflow container) for large-scale array flattening and deduplication
- Uses **dbt (dbt-core + dbt-postgres)** for transformations, modeling, and testing
- Uses **PowerBI** to expose insights via dashboards
- Runs fully in **Docker / Docker Compose**
- Is versioned and delivered via **GitHub**

By the end of this project, we want to see that you can:

- Understand and model a **complex Fintech/Banking domain**
- Design a **layered ELT architecture** with clear responsibilities per layer
- Handle **nested, semi-structured data** using PySpark + PostgreSQL
- Apply **data quality practices and tests** using dbt
- Translate business questions into **metrics, tables, and BI dashboards**
- Ship a **reproducible**, containerized solution with clear documentation and git milestones

### You must use:

- Docker & Docker Compose
- PostgreSQL 15+ (warehouse, with `bronze`, `silver`, `gold` schemas)
- Apache Airflow 2.7+ (orchestration)
- PySpark 3.5+ (inside the Airflow container)
- dbt-core + dbt-postgres 1.7+ (transformations/tests)
- PowerBI Desktop (latest) (BI)
- Python 3.11+ (DAGs, Spark scripts)
- GitHub (source of truth and submission)

---

## 2. Tools & Technologies

| Tool | Version | Purpose |
|---|---|---|
| Docker & Docker Compose | Latest | Containerized development environment |
| PostgreSQL | 15+ | Data warehouse (bronze/silver/gold schemas) |
| Apache Airflow | 2.7+ | Workflow orchestration |
| dbt (dbt-core + postgres) | 1.7+ | SQL-based transformations & testing |
| PySpark | 3.5+ | Array flattening, deduplication, JDBC I/O |
| PowerBI Desktop | Latest | Dashboards and data visualization |
| Python | 3.11+ | Scripting, DAGs, PySpark jobs |
| GitHub | — | Version control and project submission |

### Required roles of each technology:

- **Airflow:** Orchestrates the entire pipeline: S3 → Bronze → Silver (PySpark) → Silver/Gold (dbt).
- **PostgreSQL:** Single warehouse where all layers live:
  - `bronze` schema for raw JSON (as `jsonb`)
  - `silver` schema for flattened, cleaned tables
  - `gold` schema for analytics models
- **PySpark:**
  - Runs inside the Airflow container
  - Reads Bronze data from PostgreSQL
  - Flattens nested arrays and applies dedup logic
  - Writes Silver staging tables back to PostgreSQL via JDBC
- **dbt:**
  - Consumes Bronze and Silver staging tables
  - Cleans, normalizes, and finishes flattening nested objects
  - Builds analytics-oriented Gold models
  - Implements data tests (unique, not-null, accepted values, referential integrity, etc.)
- **PowerBI:**
  - Connects directly to PostgreSQL (`gold` schema)
  - Provides a 4-page dashboard answering the key business questions

> You can decide how to implement scripts, models, and visuals, as long as these tools are used in the roles above.

---

## 3. Data Source

### 3.1 Source Location

The dataset is a single JSON file hosted on an AWS S3 public bucket:

- **Bucket:** `s3://qversity-raw-public-data/`
- **File:** `fintech_banking_dataset.json`
- **Public URL:** https://qversity-raw-publicdata.s3.amazonaws.com/fintech_banking_dataset.json

**What we expect:**

Your Airflow DAG is responsible for:
- Downloading this file from S3
- Ingesting it into the Bronze layer in PostgreSQL

> You must not replace this dataset with a different one. This is the single source of truth for the whole pipeline.

### 3.2 Dataset Overview

The dataset contains ~5,000 customer records from a fictional LATAM fintech company operating across: **Colombia, Uruguay, Argentina, Mexico, Chile, Peru, and Brazil**.

Each customer record describes:
- Core customer attributes (demographics, risk, relationship, KYC, status)
- Nested product arrays: `accounts[]`, `transactions[]`, `loans[]`
- Nested behavioral/credit objects: `credit_info{}`, `digital_engagement{}`

### 3.3 Record Structure

#### Flat Fields

| Field | Type | Description |
|---|---|---|
| `customer_id` | string | Unique identifier (e.g., `CUST-0000001`) |
| `first_name` | string | Customer first name |
| `last_name` | string | Customer last name |
| `email` | string | Email address |
| `phone_number` | string | Phone with country prefix |
| `date_of_birth` | string | Date of birth |
| `gender` | string | M / F / Other |
| `nationality` | string | ISO country code |
| `city` | string | City name |
| `country` | string | ISO country code (CO, UY, AR, MX, CL, PE, BR) |
| `address` | string | Full address |
| `lat` / `lon` | float | Geographic coordinates |
| `registration_date` | string | Account registration date |
| `kyc_status` | string | verified / pending / expired / rejected |
| `risk_score` | float | 0–100 risk assessment |
| `customer_segment` | string | retail / premium / private_banking / sme |
| `relationship_manager` | string | Assigned RM name |
| `status` | string | active / inactive / suspended / closed |

We expect you to derive:
- Age and age buckets from `date_of_birth`
- Tenure from `registration_date`
- Key segmentation dimensions using `customer_segment`, `country`, `status`, `kyc_status`, and `risk_score`

#### Nested Array — `accounts[]` (2–5 per customer)

| Field | Type | Description |
|---|---|---|
| `account_id` | string | Unique account ID |
| `account_type` | string | savings / checking / investment / credit_card |
| `currency` | string | Account currency |
| `balance` | float | Current balance |
| `credit_limit` | float | Credit limit (credit_card only) |
| `interest_rate` | float | Annual interest rate |
| `opened_date` | string | Account opening date |
| `status` | string | active / closed / frozen |
| `branch_code` | string | Branch identifier |

This is central to your products/accounts modeling.

#### Nested Array — `transactions[]` (5–30 per customer)

| Field | Type | Description |
|---|---|---|
| `transaction_id` | string | Unique transaction ID |
| `account_id` | string | Associated account |
| `date` | string | Transaction date |
| `amount` | float | Transaction amount |
| `currency` | string | Transaction currency |
| `type` | string | deposit / withdrawal / transfer / payment / refund / fee |
| `category` | string | salary / groceries / utilities / rent / etc. |
| `merchant` | string | Merchant name |
| `channel` | string | mobile / web / atm / branch / pos |
| `status` | string | completed / pending / failed / reversed |
| `description` | string | Transaction description |

This is the backbone for revenue and behavioral analysis.

#### Nested Array — `loans[]` (0–3 per customer)

| Field | Type | Description |
|---|---|---|
| `loan_id` | string | Unique loan ID |
| `type` | string | personal / mortgage / auto / education / business |
| `principal` | float | Original loan amount |
| `outstanding_balance` | float | Remaining balance |
| `interest_rate` | float | Annual interest rate |
| `term_months` | int | Loan term in months |
| `monthly_payment` | float | Monthly installment |
| `start_date` | string | Loan start date |
| `end_date` | string | Loan end date |
| `status` | string | current / delinquent / default / paid_off |
| `days_past_due` | int | Days overdue |
| `collateral_type` | string | Type of collateral |

This drives risk & portfolio metrics.

#### Nested Object — `credit_info{}`

| Field | Type | Description |
|---|---|---|
| `credit_score` | int | Credit score (300–850 range) |
| `utilization_pct` | float | Credit utilization percentage |
| `total_limit` | float | Total credit limit |
| `total_used` | float | Total credit used |
| `num_credit_accounts` | int | Number of credit accounts |
| `oldest_account_age_months` | int | Age of oldest account |
| `late_payments_12m` | int | Late payments in last 12 months |
| `inquiries_6m` | int | Credit inquiries in last 6 months |
| `bankruptcy_flag` | bool | Bankruptcy indicator |

This is key for credit risk and utilization analysis.

#### Nested Object — `digital_engagement{}`

| Field | Type | Description |
|---|---|---|
| `mobile_app_registered` | bool | Mobile app signup |
| `web_banking_registered` | bool | Web banking signup |
| `last_login_date` | string | Most recent login date |
| `avg_monthly_logins` | int | Average logins per month |
| `preferred_channel` | string | Preferred banking channel |
| `push_notifications` | bool | Push notifications enabled |
| `paperless_statements` | bool | Paperless statements enabled |

This powers digital adoption and engagement analysis.

### 3.4 Data Quality & EDA

We expect you to:

- Perform **exploratory data analysis (EDA)** (via notebooks, SQL, or dbt) to understand:
  - Missing values and outliers
  - Inconsistent categories (unexpected statuses, segments, etc.)
  - Data distributions (ages, balances, credit scores, utilization, etc.)
- Use EDA findings to:
  - Define your **Silver-layer cleaning logic**
  - Drive your **dbt tests** (unique, not-null, accepted values, referential integrity)
  - Document **assumptions and decisions** in your README (e.g., how you treat nulls or invalid data)

> The way you **understand, clean, and model** this data source is a major part of the evaluation.

---

## 4. Target Architecture

High-level data flow:

```
S3 (fintech_banking_dataset.json)
│
▼
Apache Airflow (DAG)
│
▼
BRONZE (PostgreSQL, schema=bronze)
  - Raw JSON as jsonb + metadata
│
├─────────────► PySpark (inside Airflow container)
│                   - Flatten nested arrays
│                   - Deduplicate
│                   - Write staging to schema=silver
│
▼
dbt (PostgreSQL)
  - Silver models: clean, standardize, finish flattening objects
  - Gold models: analytics tables per business question
│
▼
GOLD (PostgreSQL, schema=gold)
│
▼
PowerBI Desktop
  - 4-page dashboard on top of gold schema
```

**Flow steps:**

1. **S3 → Airflow → Bronze:** DAG downloads JSON from S3 and loads it into PostgreSQL `bronze` schema as a `jsonb` table.
2. **Bronze → Silver (PySpark):** PySpark reads the Bronze table, flattens arrays, applies deduplication, and writes staging tables to `silver`.
3. **Silver (PySpark) → Silver/Gold (dbt):** dbt cleans and standardizes staging tables, finishes flattening objects, builds facts/dimensions, then Gold analytics models.
4. **Gold → PowerBI:** PowerBI connects to the `gold` schema and presents insights in a 4-page dashboard.

---

## 5. ELT Requirements by Layer

### 5.1 Bronze Layer (Airflow + PostgreSQL)

**Goal:** Persist the raw dataset faithfully in PostgreSQL with minimal transformation.

What we want:

- An Airflow DAG that:
  - Downloads `fintech_banking_dataset.json` from S3
  - Loads all customer records into a single Bronze table
- A table in schema `bronze` that:
  - Stores each record as a `jsonb` column (full original structure)
  - Includes ingestion metadata (e.g., `load_timestamp`, and an internal `id`)
  - Contains ≥ 1,000 records

**Expected output example:**

```sql
bronze.raw_fintech_data(id, data jsonb, load_timestamp)
```

> We care that you use Airflow to orchestrate this (not manual scripts).

### 5.2 Silver Layer — PySpark (Flattening & Dedup)

**Goal:** Use PySpark 3.5+ (inside the Airflow container) to flatten nested arrays and deduplicate data.

**Constraints:**
- PySpark runs within the Airflow container (no external cluster).
- PySpark reads from the Bronze table in PostgreSQL via JDBC.
- PySpark writes staging tables into the `silver` schema via JDBC.
- PySpark scripts live in a dedicated folder (e.g., `spark/`) and are triggered from Airflow.

**What we want you to do with PySpark:**

- Flatten array fields into relational tables, e.g.:
  - `silver.stg_accounts`
  - `silver.stg_transactions`
  - `silver.stg_loans`
- Apply deduplication logic for customers and/or transactions where appropriate.
- Clearly define how you decide what is a duplicate.
- Ensure the resulting staging tables:
  - Do not contain nested arrays
  - Are suitable as inputs for dbt's Silver models

**We evaluate:**
- The structure of your Spark jobs
- How well arrays and deduplication are handled
- How cleanly PySpark integrates with Airflow and PostgreSQL

### 5.3 Silver Layer — dbt (Cleaning & Normalization)

**Goal:** Use dbt to turn PySpark outputs + nested objects into a clean, normalized Silver model with tests.

**Constraints:**
- dbt project targets the same PostgreSQL instance.
- Uses schemas `bronze`, `silver`, and `gold` appropriately.
- dbt models are organized (e.g., `models/bronze`, `models/silver`, `models/gold`).

**What we want from dbt Silver:**

- Clean and standardize PySpark staging outputs:
  - Normalize types and formats (dates, numerics, categories)
  - Apply naming conventions and consistent semantics
- Flatten nested objects (`credit_info`, `digital_engagement`) into relational structures.
- Build a normalized data model with:
  - Dimensions (customers, products, geography, segments, etc.)
  - Facts (transactions, loans, balances, etc.)
- Implement dbt tests for:
  - Uniqueness of primary keys
  - Not-null constraints on key fields
  - Accepted values (statuses, segments, channels, etc.)
  - Referential integrity between facts and dimensions
  - Any important custom quality rules you identify

**We're looking at:**
- Model design quality and naming
- Test coverage and clarity
- How Silver sets up a clean base for Gold

### 5.4 Gold Layer — dbt (Analytics Models)

**Goal:** Use dbt to build Gold models that directly support the 24 business questions, with at least **21 answered**.

**Constraints:**
- Gold models live under `dbt/models/gold/`.
- They materialize to the `gold` schema in PostgreSQL.

**What we want from dbt Gold:**

- Analytics-ready tables/views with clearly defined grain (e.g., customer-month, segment, loan, channel).
- Metrics and dimensions sufficient to answer the questions in Section 7:
  - Revenue and balances
  - Delinquency and risk
  - Demographic breakdowns
  - Transaction patterns
  - Digital engagement
  - Product mix
- Explicit business logic:
  - How you define "revenue"
  - How you define "delinquent", "current", etc.
  - How you bucket ages, risk scores, credit scores, utilization, days past due
- dbt tests that still pass at Gold level.

**We assess:**
- Alignment between Gold models and business questions
- Reusability and clarity of metrics
- Model structure and documentation

---

## 6. PowerBI Dashboard

**Goal:** Use PowerBI Desktop to present Gold-layer insights in a compelling, 4-page dashboard.

**Constraints:**
- PowerBI must connect directly to PostgreSQL `gold` schema:
  - Server: `localhost:5432` (or equivalent)
  - Database: same used by the project
  - Schema: `gold`
- You must use PowerBI (not another BI tool).

**What we want in the dashboard:**

4 pages, each focused on a major theme:

#### a. Executive Overview
High-level KPIs: customers, assets under management, geographic distribution, segment mix, customer status.

#### b. Revenue & Transactions
Volume and value trends, categories, channels, average ticket sizes, failure rates.

#### c. Risk & Credit
Credit score distribution, delinquency, utilization vs delinquency, loan portfolio composition, days past due.

#### d. Customer & Engagement
Acquisition trends, age distribution, digital adoption, preferred channels, KYC status.

Use filters/slicers where appropriate (time, country, segment, etc.).

Provide in the README a brief narrative per page:
- What it shows
- Which business questions it answers
- What decisions it can support

**Deliverables:**
- `powerbi/dashboard.pbix`
- Screenshots for each page in `powerbi/screenshots/`

---

## 7. Business Questions (24 Total — Answer ≥ 21)

You must ensure your Gold models and PowerBI dashboard can answer at least **21** of these questions.

### Revenue & Profitability (4)

1. What is the average revenue per customer by segment?
2. What are the total account balances by country?
3. What is the revenue breakdown by transaction channel?
4. What is the interest income by loan type?

### Risk & Credit (5)

5. What is the loan delinquency rate by customer segment?
6. What is the credit score distribution by country?
7. Is there a relationship between credit utilization and delinquency?
8. What is the days past due distribution by loan type?
9. How do risk scores segment customers (low/medium/high/critical)?

### Customer Demographics (5)

10. What is the customer count by country and city?
11. What is the age distribution by customer segment?
12. What is the customer acquisition trend over time (monthly)?
13. What is the customer status breakdown (active/inactive/suspended/closed)?
14. What is the KYC status distribution?

### Transaction Patterns (5)

15. What are the most common transaction categories by volume and value?
16. What is the transaction volume by day of week?
17. What is the average transaction size by channel?
18. What is the failed transaction rate by channel?
19. What are the international transfer patterns (e.g., by currency/country)?

### Digital Engagement (2)

20. What is the mobile app adoption rate by segment?
21. What is the digital vs branch preference by age group?

### Product Analysis (3)

22. What are the most popular account types?
23. What is the loan portfolio composition (outstanding balance by type and status)?
24. What is the average number of products per customer by segment?

> Your Gold models should make these questions answerable in a clear, auditable way.

---

## 8. Repository & Submission Requirements

### Expected Structure

```
qversity-data-2026-<city>-<firstname><lastname>/
├── docker-compose.yml
├── env.example
├── requirements.txt
├── README.md
├── .gitignore
├── dags/
│   └── qversity_dag.py
├── spark/
│   └── ... PySpark scripts ...
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── tests/
├── powerbi/
│   ├── dashboard.pbix
│   └── screenshots/
├── data/
│   └── raw/
└── scripts/
```

### Git Tags (Milestones)

| Tag | Milestone |
|---|---|
| `v0.1.0-bronze` | Bronze complete |
| `v0.2.0-silver` | Silver complete |
| `v0.3.0-gold` | Gold complete |
| `v0.4.0-powerbi` | Dashboard ready |
| `v1.0.0` | Final submission |

### Repository Setup

**Create Repository** using this naming convention:

```
qversity-data-[year]-[city]-[your-name][your-lastname]
```

Example: `qversity-data-2026-montevideo-johnsmith`

**Make Repository Accessible:** Make the repository **private** and add the following collaborators: `@serasio`, `@lualopezpe`, `@luciafrances`, `@AgusOlivera`

We will:
- Clone your GitHub repo
- Use tags to review progression
- Run the project using your Docker setup

---

## 9. Documentation Requirements (README)

Your `README.md` should allow someone new to:

1. **Understand the project**
   - Short overview of the business problem and goals
   - Architecture diagram (text or image)

2. **Know who you are**
   - Full name, email, city, cohort

3. **Run the pipeline**
   - Prerequisites (Docker, Python, PowerBI)
   - Environment setup (`env.example` → `.env`)
   - How to start via Docker
   - How to trigger the Airflow DAG
   - Key dbt commands (`dbt run`, `dbt test`)

4. **Understand the architecture**
   - Description of Bronze, Silver (PySpark + dbt), and Gold layers
   - Role of each main technology

5. **Understand the data model**
   - ERD or relationship diagram for core Silver and/or Gold tables
   - Explanation of main facts and dimensions

6. **Understand PySpark logic**
   - Purpose of each script
   - What each script flattens
   - How deduplication works conceptually

7. **See the insights**
   - Summary of key findings from the business questions
   - References and screenshots of PowerBI pages

8. **Know your assumptions**
   - Data quality assumptions
   - Business logic decisions (bucketing, definitions)
   - Design trade-offs

---

## 10. Checklist

Use this to verify completeness before submission:

### Environment
- [ ] `docker compose up -d --build` runs without errors
- [ ] PostgreSQL and Airflow are healthy
- [ ] `env.example` exists with all required variables

### Bronze
- [ ] DAG downloads JSON from S3
- [ ] Raw data loaded into `bronze` schema as `jsonb`
- [ ] Table contains ≥ 1,000 records

### Silver — PySpark
- [ ] PySpark runs inside Airflow container
- [ ] Arrays (`accounts`, `transactions`, `loans`) flattened into silver staging tables
- [ ] Deduplication logic implemented and documented
- [ ] Staging tables contain no nested arrays

### Silver — dbt
- [ ] Silver models clean and standardize PySpark outputs
- [ ] Dimension and fact tables created
- [ ] dbt tests defined and passing

### Gold — dbt
- [ ] At least 21/24 business questions answerable from Gold models
- [ ] Aggregations and joins implemented clearly
- [ ] All dbt tests pass on Gold

### PowerBI
- [ ] `powerbi/dashboard.pbix` exists, connected to `gold` schema
- [ ] 4 dashboard pages implemented
- [ ] Screenshots saved in `powerbi/screenshots/`

### Git & Docs
- [ ] All five git tags present (`v0.1.0-bronze` … `v1.0.0`)
- [ ] `README.md` includes all required sections
- [ ] ERD / data model diagram included
- [ ] Business insights documented
- [ ] No secrets/credentials committed (use `.env` and `.gitignore`)

---

## 11. How We Evaluate

We focus on:

- **Correct and thoughtful use of the required tools** (Airflow, PySpark, dbt, PostgreSQL, PowerBI, Docker, GitHub)
- **Data modeling quality** (Bronze/Silver/Gold separation, ERD, grains)
- **Data quality and testing** (dbt tests, handling of anomalies)
- **End-to-end robustness and reproducibility**
- **Ability to extract and communicate business insights** (Gold models + dashboards)
