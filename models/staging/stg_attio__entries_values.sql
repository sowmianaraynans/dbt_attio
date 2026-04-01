with source as (
    select * from {{ source('attio', 'entries_value') }}
),

renamed as (
    select
        id                          as entry_value_id,
        entry_id,
        attribute_id,
        value_text,
        value_number,
        value_boolean,
        value_date,
        value_timestamp,
        value_record_reference_id,
        value_workspace_member_id,
        value_option_id,
        value_status_id,
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
