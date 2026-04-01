with source as (
    select * from {{ source('attio', 'workspace_member') }}
),

renamed as (
    select
        id                  as workspace_member_id,
        first_name,
        last_name,
        {{ dbt.concat(["first_name", "' '", "last_name"]) }} as full_name,
        email_address,
        access_level,
        is_confirmed,
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where _fivetran_deleted is not true
)

select * from renamed
