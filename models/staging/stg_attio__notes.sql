with source as (
    select * from {{ source('attio', 'note') }}
),

renamed as (
    select
        id                                  as note_id,
        record_id,
        parent_object                       as record_object_slug,
        title,
        content_plaintext                   as body,
        created_by_workspace_member_id,
        created_at,
        updated_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where _fivetran_deleted is not true
)

select * from renamed
