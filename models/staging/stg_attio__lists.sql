 with source as (
    select * from {{ source('attio', 'list') }}
),

renamed as (
    select
        id          as list_id,
        api_slug,
        name,
        object_id,  -- the object type this list is associated with
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where _fivetran_deleted is not true
)

select * from renamed
