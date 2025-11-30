SELECT
    product_id,
    product_name,
    TRIM(category) AS product_category,
    TRIM(sub_category) AS product_sub_category,
    CAST(price AS NUMERIC) AS product_price
FROM
    {{ source('raw', 'products_raw') }}
