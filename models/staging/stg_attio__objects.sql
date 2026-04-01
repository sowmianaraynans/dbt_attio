with source as (
    select * from {{ source('attio', 'object') }}
),

renamed as (
    select
        id              as object_id,
        api_slug,
        singular_noun,
        plural_noun,
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
