with source as (
    select * from {{ source('attio', 'object_attribute') }}
),

renamed as (
    select
        id              as attribute_id,
        object_id,
        api_slug,
        title,
        type,           -- text, number, boolean, date, timestamp, record-reference, etc.
        is_required,
        is_unique,
        is_multiselect,
        is_archived,
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
