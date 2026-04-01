with source as (
    select * from {{ source('attio', 'object_attribute_option') }}
),

renamed as (
    select
        id              as option_id,
        attribute_id,
        title,
        color,
        is_archived,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where _fivetran_deleted is not true
)

select * from renamed
