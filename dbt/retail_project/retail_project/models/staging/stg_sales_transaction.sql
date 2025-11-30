with source as (
    select
        transaction_id,
        transaction_date,
        customer_id,
        product_id,
        store_id,
        quantity,
        total_price
    from {{ source('raw', 'sales_transaction_raw') }}
),

typed as (
    select
        transaction_id,
        customer_id,
        product_id,
        store_id,
        transaction_date::date        as transaction_date,
        quantity::integer             as quantity,
        total_price::numeric(18,2)    as total_price,
        case
            when quantity::numeric <> 0
                then total_price::numeric(18,2) / quantity::numeric
            else null
        end                           as unit_price
    from source
)

select * from typed

