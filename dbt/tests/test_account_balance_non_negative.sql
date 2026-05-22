-- Test: account balance should not be negative
select *
from {{ ref('fct_account') }}
where balance < 0
