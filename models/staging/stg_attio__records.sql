with source as (
    select * from {{ source('attio', 'record') }}
),

renamed as (
    select
        id                      as record_id,
        object_id,
        workspace_id,
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where _fivetran_deleted is not true
)

select * from renamed
