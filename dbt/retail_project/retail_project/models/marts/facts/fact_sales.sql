with sales as (
    select
        transaction_id,
        transaction_date,
        customer_id,
        product_id,
        store_id,
        quantity,
        unit_price,
        quantity * unit_price as gross_sales
    from {{ ref('stg_sales_transaction') }}
),

joined as (
    select
        s.transaction_id,
        s.transaction_date,
        s.customer_id,
        s.product_id,
        s.store_id,
        s.quantity,
        s.unit_price,
        s.gross_sales,

        c.customer_name,
        p.product_name,
        -- p.category,   -- sementara DIHAPUS biar nggak error
        st.store_name,
        st.store_city,
        st.store_region
    from sales s
    left join {{ ref('dim_customer') }} c
        on s.customer_id = c.customer_id
    left join {{ ref('dim_product') }} p
        on s.product_id = p.product_id
    left join {{ ref('dim_store') }} st
        on s.store_id = st.store_id
)

select * from joined

