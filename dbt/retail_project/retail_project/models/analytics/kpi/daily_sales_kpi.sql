select
    transaction_date::date                            as date,
    sum(gross_sales)                                  as total_sales,
    sum(quantity)                                     as total_qty,
    count(distinct customer_id)                       as unique_customers,
    case
        when sum(quantity) > 0 then sum(gross_sales) / sum(quantity)
        else null
    end                                               as avg_price_per_unit,
    case
        when count(distinct transaction_id) > 0
            then sum(gross_sales) / count(distinct transaction_id)
        else null
    end                                               as avg_order_value
from {{ ref('fact_sales') }}
group by 1
order by 1

