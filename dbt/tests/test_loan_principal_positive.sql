-- Test: loan principal must be positive
select *
from {{ ref('fct_loans') }}
where principal <= 0
