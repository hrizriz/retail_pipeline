with base as (
    select
        transaction_date::date    as date,
        store_id,
        store_name,
        store_city,
        sum(gross_sales)          as total_sales,
        sum(quantity)             as total_qty
    from {{ ref('fact_sales') }}
    group by 1,2,3,4
)

select
    *,
    rank() over (partition by date order by total_sales desc) as sales_rank_in_day
from base
order by date, sales_rank_in_day

