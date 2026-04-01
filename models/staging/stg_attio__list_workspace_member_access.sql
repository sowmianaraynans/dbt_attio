with source as (
    select * from {{ source('attio', 'list_workspace_member_access') }}
),

renamed as (
    select
        list_id,
        workspace_member_id,
        access_level,
        _fivetran_synced
    from source
)

select * from renamed
