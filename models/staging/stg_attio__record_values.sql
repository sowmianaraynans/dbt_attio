with source as (
    select * from {{ source('attio', 'record_value') }}
),

renamed as (
    select
        id                          as record_value_id,
        record_id,
        attribute_id,
        -- value columns vary by attribute type; keep all for downstream pivoting
        value_text,
        value_number,
        value_boolean,
        value_date,
        value_timestamp,
        value_record_reference_id,  -- FK to another record
        value_workspace_member_id,  -- FK to workspace_member
        value_option_id,            -- FK to object_attribute_option
        value_status_id,            -- FK to object_attribute_status
        value_currency_value,
        value_currency_currency_code,
        active_from,
        active_until,
        created_at,
        _fivetran_synced,
        _fivetran_deleted
    from source
    where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
