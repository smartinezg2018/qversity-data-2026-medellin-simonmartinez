-- Gold: each account row should reflect at least one product for that customer
select *
from {{ ref('fct_product_summary') }}
where total_product_count < 1
