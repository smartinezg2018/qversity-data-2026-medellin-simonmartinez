{{ config(materialized='table') }}

select
    d.customer_id,
    c.customer_segment,
    c.age_bucket,
    d.mobile_app_registered,
    d.web_banking_registered,
    d.preferred_channel,
    d.avg_monthly_logins,
    d.push_notifications,
    d.paperless_statements,
    d.last_login_date,
    d.mobile_app_registered                                         as is_mobile_user,
    case
        when d.preferred_channel in ('mobile', 'web') then 'digital'
        else                                               'branch/other'
    end                                                             as channel_preference
from {{ ref('dim_digital_engagement') }} d
join {{ ref('dim_customer') }} c using (customer_id)
