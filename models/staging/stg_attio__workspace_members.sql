with source as (
    select * from {{ source('attio', 'workspace_member') }}
),

renamed as (
    select
        id                  as workspace_member_id,
        first_name,
        last_name,
        first_name || ' ' || last_name  as full_name,
        email_address,
        access_level,
        is_confirmed,
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
