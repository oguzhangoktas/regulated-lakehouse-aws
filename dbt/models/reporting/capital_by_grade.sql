-- Capital and RWA broken down by rating grade, for the latest reporting date.
-- Shows where regulatory capital concentrates across the rating scale.

with latest as (
    select max(reporting_date) as reporting_date
    from {{ source('credit_risk', 'rwa_output') }}
)

select
    r.reporting_date,
    r.rating_grade,
    count(*)                  as exposure_count,
    sum(r.ead)                as total_ead,
    sum(r.rwa)                as total_rwa,
    sum(r.capital_required)   as total_capital
from {{ source('credit_risk', 'rwa_output') }} r
join latest l on r.reporting_date = l.reporting_date
group by r.reporting_date, r.rating_grade
order by r.rating_grade
