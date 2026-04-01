with source as (
    select * from {{ source('attio', 'entries_value') }}
),

renamed as (
    select
        id                          as entry_value_id,
        entry_id,
        name                        as attribute_slug,  -- e.g. 'stage', 'status', 'owner'
        value,                                          -- raw scalar value (text)
        active_from,
        active_until,
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where _fivetran_deleted is not true
)

select * from renamed
