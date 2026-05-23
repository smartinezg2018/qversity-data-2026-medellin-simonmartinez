-- Gold: revenue amounts must be non-negative
select *
from {{ ref('fct_revenue') }}
where total_revenue_usd < 0
