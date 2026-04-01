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
    where _fivetran_deleted is not true
)

select * from renamed
