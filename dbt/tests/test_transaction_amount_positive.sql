-- Test: transaction amount must be positive
select *
from {{ ref('fct_transactions') }}
where amount <= 0
