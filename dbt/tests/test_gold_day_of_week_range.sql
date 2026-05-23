-- Gold: day_of_week_num must be a valid PostgreSQL DOW (0 = Sunday .. 6 = Saturday)
select *
from {{ ref('fct_transaction_patterns') }}
where day_of_week_num < 0 or day_of_week_num > 6
