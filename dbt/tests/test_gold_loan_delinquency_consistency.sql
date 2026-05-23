-- Gold: is_delinquent must align with loan_status
select *
from {{ ref('fct_loan_portfolio') }}
where (is_delinquent = true and loan_status not in ('Delinquent', 'Default'))
   or (is_delinquent = false and loan_status in ('Delinquent', 'Default'))
