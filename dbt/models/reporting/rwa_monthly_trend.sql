-- Monthly capital and RWA trend across reporting dates.
-- Aggregates the per-exposure RWA output into the figures a risk report tracks month
-- over month: total exposure, total RWA, capital held, and the average risk weight.

select
    reporting_date,
    count(*)                              as exposure_count,
    sum(ead)                              as total_ead,
    sum(rwa)                              as total_rwa,
    sum(capital_required)                 as total_capital,
    round(sum(rwa) / nullif(sum(ead), 0), 4)  as avg_risk_weight
from {{ source('credit_risk', 'rwa_output') }}
group by reporting_date
order by reporting_date
