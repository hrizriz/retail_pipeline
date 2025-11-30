SELECT
    customer_id,
    customer_name,
    customer_gender,
    customer_city,
    customer_signup_date
FROM
    {{ ref('stg_customers') }}
