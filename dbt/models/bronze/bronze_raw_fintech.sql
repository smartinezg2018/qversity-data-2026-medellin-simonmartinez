{{ config(materialized='view') }}

-- Bronze layer: Raw fintech/banking data view
-- Reads directly from the PostgreSQL raw table ingested by Airflow.

select
    id,
    data,
    load_timestamp
from bronze.raw_fintech_data

