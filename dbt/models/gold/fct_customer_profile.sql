{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    country,
    city,
    customer_segment,
    status,
    kyc_status,
    gender,
    registration_date,
    date_trunc('month', registration_date) as registration_month,
    age,
    age_bucket,
    risk_score,
    risk_tier
from {{ ref('dim_customer') }}
