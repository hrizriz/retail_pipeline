SELECT
    customer_id,
    customer_name,
    UPPER(TRIM(gender)) AS customer_gender,
    TRIM(city) AS customer_city,
    CAST(signup_date AS DATE) AS customer_signup_date
FROM
    {{ source('raw', 'customers_raw') }}
