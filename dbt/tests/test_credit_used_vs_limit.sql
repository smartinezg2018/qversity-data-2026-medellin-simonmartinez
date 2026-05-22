-- Test: total_used should not exceed total_limit
select *
from {{ ref('dim_credit_info') }}
where total_used > total_limit
