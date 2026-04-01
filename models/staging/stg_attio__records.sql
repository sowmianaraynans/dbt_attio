with source as (
    select * from {{ source('attio', 'record') }}
),

renamed as (
    select
        id                          as record_id,
        object_id,
        created_at,
        -- Fivetran audit columns
        _fivetran_synced,
        _fivetran_deleted
    from source
    where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
