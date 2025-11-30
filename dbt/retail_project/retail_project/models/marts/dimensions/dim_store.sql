select
    store_id,
    store_name,
    city as store_city,
    store_region
from {{ ref('stg_stores') }}

