with source as (
    select
        store_id,
        store_name,
        city,
        region
    from {{ source('raw', 'stores_raw') }}
),

renamed as (
    select
        store_id,
        store_name,
        city,
        region as store_region
    from source
)

select * from renamed
