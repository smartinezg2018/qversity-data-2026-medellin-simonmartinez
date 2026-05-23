-- Gold: fct_revenue grain is one row per customer_id + channel
select
    customer_id,
    channel,
    count(*) as row_count
from {{ ref('fct_revenue') }}
group by customer_id, channel
having count(*) > 1
