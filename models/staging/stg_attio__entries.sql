with source as (
    select * from {{ source('attio', 'entries') }}
),

renamed as (
    select
        id          as entry_id,
        list_id,
        record_id,
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
