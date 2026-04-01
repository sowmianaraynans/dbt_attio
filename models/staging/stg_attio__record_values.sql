with source as (
    select * from {{ source('attio', 'record_value') }}
),

renamed as (
    select
        id                          as record_value_id,
        record_id,
        record_object_id,           -- denormalised object_id on the value row
        record_workspace_id,
        name                        as attribute_slug,  -- e.g. 'email_addresses', 'domains'
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
