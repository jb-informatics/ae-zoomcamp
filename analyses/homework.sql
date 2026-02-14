-- select count(*) from {{ ref('fct_monthly_zone_revenue')}}

-- select 
--     pickup_zone,
--     sum(revenue_monthly_total_amount) as total
-- from {{ ref('fct_monthly_zone_revenue')}}
-- where service_type = 'Green' and extract(year from revenue_month) = 2020
-- group by pickup_zone
-- order by total desc
-- limit 1

select 
    service_type,
    sum(total_monthly_trips) as total_trips
from {{ ref('fct_monthly_zone_revenue')}}
where service_type = 'Green' 
    and extract(year from revenue_month) = 2019
    and extract(month from revenue_month) = 10
group by service_type