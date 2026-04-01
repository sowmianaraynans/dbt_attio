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
    where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
