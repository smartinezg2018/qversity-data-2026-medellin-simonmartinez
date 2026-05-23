-- Gold: credit scores in fct_credit_risk stay within the analytic band [350, 850]
select *
from {{ ref('fct_credit_risk') }}
where credit_score is not null
  and (credit_score < 350 or credit_score > 850)
