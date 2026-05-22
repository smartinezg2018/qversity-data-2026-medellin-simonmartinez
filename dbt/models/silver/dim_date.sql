{{ config(materialized='table') }}

with all_dates as (
    select registration_date as full_date
    from {{ ref('dim_customer') }}
    where registration_date is not null

    union

    select opened_date
    from {{ ref('fct_account') }}
    where opened_date is not null

    union

    select date as full_date
    from {{ ref('fct_transactions') }}
    where date is not null

    union

    select start_date
    from {{ ref('fct_loans') }}
    where start_date is not null

    union

    select end_date
    from {{ ref('fct_loans') }}
    where end_date is not null

    union

    select last_login_date
    from {{ ref('dim_digital_engagement') }}
    where last_login_date is not null
),

distinct_dates as (
    select distinct full_date::date as full_date
    from all_dates
)

select
    {{ date_key('full_date') }}                                             as date_key,
    full_date,
    extract(year from full_date)::int                                       as year_number,
    extract(month from full_date)::int                                      as month_number,
    extract(day from full_date)::int                                        as day_of_month,
    extract(quarter from full_date)::int                                    as quarter_number,
    trim(to_char(full_date, 'Day'))                                         as day_name,
    extract(isodow from full_date)::int                                     as day_of_week,
    (extract(isodow from full_date) in (6, 7))                              as is_weekend,
    to_char(full_date, 'YYYY-MM')                                           as year_month
from distinct_dates
