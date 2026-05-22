-- Grain: one row per customer_id from stg_customers (same as PySpark dedup).
-- Cleansing may NULL individual attributes; columns without not_null in schema.yml
-- (e.g. gender, phone_number, lat/lon) may stay NULL. not_null tests mark fields
-- required for this dataset after rules — failures are data-quality debt, not imputation.
-- depends_on: {{ ref('city_name_mapping') }}
-- depends_on: {{ ref('country_name_mapping') }}
-- depends_on: {{ ref('kyc_status_mapping') }}
-- depends_on: {{ ref('customer_segment_mapping') }}
-- depends_on: {{ ref('customer_status_mapping') }}
{{ config(materialized='table') }}

with base as (
    select *
    from {{ source('silver', 'stg_customers') }}
),

cleaned as (
    select
        customer_id,
        {{ proper_case('first_name') }}                                     as first_name,
        {{ proper_case('last_name') }}                                      as last_name,
        {{ clean_email('email') }}                                          as email,
        {{ clean_phone('phone_number') }}                                   as phone_number,
        {{ parse_date('date_of_birth') }}                                   as date_of_birth,
        {{ safe_string('gender') }}                                         as gender,
        {{ safe_string('nationality') }}                                    as nationality,
        {{ map_from_seed('city', 'city_name_mapping') }}                    as city,
        {{ map_from_seed('country', 'country_name_mapping') }}              as country,
        {{ safe_string('address') }}                                        as address,
        {{ clamp_numeric('lat', -90, 90) }}                                 as lat,
        {{ clamp_numeric('lon', -180, 180) }}                               as lon,
        {{ parse_date('registration_date') }}                               as registration_date,
        {{ map_from_seed('kyc_status', 'kyc_status_mapping') }}             as kyc_status,
        {{ clamp_numeric('risk_score', 0, 100) }}                           as risk_score,
        {{ map_from_seed('customer_segment', 'customer_segment_mapping') }} as customer_segment,
        {{ safe_string('relationship_manager') }}                           as relationship_manager,
        {{ map_from_seed('status', 'customer_status_mapping') }}            as status
    from base
),

temporal as (
    select
        *,
        {{ calculate_years('date_of_birth') }}                              as age,
        {{ calculate_years('registration_date') }}                          as tenure
    from cleaned
)

select
    customer_id,
    first_name,
    last_name,
    email,
    {{ is_valid_email('email') }}                                           as is_email_valid,
    phone_number,
    date_of_birth,
    age,
    {{ age_bucket('age') }}                                                 as age_bucket,
    gender,
    nationality,
    city,
    country,
    address,
    lat,
    lon,
    registration_date,
    {{ date_key('registration_date') }}                                     as registration_date_key,
    tenure,
    kyc_status,
    risk_score,
    {{ risk_tier('risk_score') }}                                           as risk_tier,
    customer_segment,
    relationship_manager,
    status
from temporal
