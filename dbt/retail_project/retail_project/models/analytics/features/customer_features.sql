with base as (
    select
        customer_id,
        customer_name,
        customer_signup_date,
        current_date - customer_signup_date as customer_tenure_days
    from {{ ref('dim_customer') }}
),

agg_sales as (
    select
        customer_id,
        count(distinct transaction_id)        as total_txn,
        sum(gross_sales)                      as total_spent,
        max(transaction_date)::date           as last_transaction_date
    from {{ ref('fact_sales') }}
    group by customer_id
)

select
    b.customer_id,
    b.customer_name,
    b.customer_signup_date,
    b.customer_tenure_days,

    a.total_txn                                 as frequency,
    a.total_spent                               as monetary,
    a.last_transaction_date,
    current_date - a.last_transaction_date      as recency_days,
    case
        when a.total_txn > 0 then a.total_spent / a.total_txn
        else null
    end                                         as avg_order_value
from base b
left join agg_sales a
  on b.customer_id = a.customer_id

