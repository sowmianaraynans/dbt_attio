/*
  fct_attio__entries
  ------------------
  One row per list entry, with key EAV values pivoted.
  The list entry is the join between a record and a list (like a pipeline stage).
*/

with entries as (
    select * from {{ ref('stg_attio__entries') }}
),

lists as (
    select
        list_id,
        name        as list_name,
        api_slug    as list_slug,
        object_id
    from {{ ref('stg_attio__lists') }}
),

records as (
    select
        record_id,
        object_id
    from {{ ref('stg_attio__records') }}
),

-- Flatten entries_value with attribute slug for pivoting
entry_values_with_slug as (
    select
        ev.entry_id,
        la.api_slug,
        la.type             as attribute_type,
        ev.value_text,
        ev.value_number,
        ev.value_boolean,
        ev.value_date,
        ev.value_timestamp,
        ev.value_option_id,
        ev.value_status_id,
        ev.active_until
    from {{ ref('stg_attio__entries_values') }} ev
    left join {{ ref('stg_attio__list_attributes') }} la
        on ev.attribute_id = la.attribute_id
    where ev.active_until is null
),

-- Pivot common list/pipeline attributes — extend as needed
pivoted as (
    select
        entry_id,
        max(case when api_slug = 'stage'            then value_text end)    as stage,
        max(case when api_slug = 'status'           then value_text end)    as status,
        max(case when api_slug = 'value'            then value_number end)  as deal_value,
        max(case when api_slug = 'close_date'       then value_date end)    as close_date,
        max(case when api_slug = 'owner'            then value_text end)    as owner,
        max(case when api_slug = 'priority'         then value_text end)    as priority
    from entry_values_with_slug
    group by entry_id
),

final as (
    select
        e.entry_id,
        e.list_id,
        l.list_name,
        l.list_slug,
        e.record_id,
        p.stage,
        p.status,
        p.deal_value,
        p.close_date,
        p.owner,
        p.priority,
        e.created_at
    from entries e
    left join lists l    on e.list_id = l.list_id
    left join pivoted p  on e.entry_id = p.entry_id
)

select * from final
