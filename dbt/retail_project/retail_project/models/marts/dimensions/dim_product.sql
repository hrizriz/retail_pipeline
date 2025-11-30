select
    product_id,
    product_name,
    product_category      as category,
    product_sub_category  as sub_category,
    product_price         as price
from {{ ref('stg_products') }}

