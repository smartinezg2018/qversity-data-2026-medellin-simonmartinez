-- Test: credit_score must be within [350, 850] range after clamping
select *
from {{ ref('dim_credit_info') }}
where credit_score < 350 or credit_score > 850
