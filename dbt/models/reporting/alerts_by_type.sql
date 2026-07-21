-- Alert volume and value by transaction type.
-- Shows which transaction types drive alerts and how much value they move, so review
-- effort can be aimed where the exposure is.

select
    type,
    count(*)              as alert_count,
    sum(label)            as true_fraud,
    sum(amount)           as total_amount,
    round(avg(amount), 2) as avg_amount
from {{ source('txn_monitoring', 'alerts') }}
group by type
order by alert_count desc
