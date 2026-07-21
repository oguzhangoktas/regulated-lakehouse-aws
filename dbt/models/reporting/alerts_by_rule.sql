-- Alert volume and fraud catch rate per detection rule.
-- One row per rule: how many alerts it raised, how many were truly fraud, and the
-- precision that implies — the summary a monitoring team reviews.

select
    rule,
    count(*)                                    as alert_count,
    sum(label)                                  as true_fraud,
    round(sum(label) * 1.0 / count(*), 4)       as precision
from {{ source('txn_monitoring', 'alerts') }}
group by rule
order by alert_count desc
