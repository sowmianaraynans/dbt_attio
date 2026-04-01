/*
  int_attio__list_pipeline
  ------------------------
  List entries joined to their EAV values (pivoted) and status/option labels.
  Provides a clean, wide row per entry ready for the fct_attio__list_entries mart.
*/

with entries as (
    select * from {{ ref('stg_attio__entries') }}
),

entry_values as (
    select * from {{ ref('stg_attio__entries_values') }}
    where active_until is null
),

lists as (
    select
        list_id,
        name        as list_name,
        api_slug    as list_slug,
        object_id
    from {{ ref('stg_attio__lists') }}
),

list_attrs as (
    select
        attribute_id,
        api_slug,
        type,
        list_id
    from {{ ref('stg_attio__list_attributes') }}
),

status_labels as (
    select
        s.status_id,
        s.attribute_id,
        s.title     as status_label
    from {{ ref('stg_attio__list_attribute_statuses') }} s
),

option_labels as (
    select
        o.option_id,
        o.attribute_id,
        o.title     as option_label
    from {{ ref('stg_attio__list_attribute_options') }} o
),

-- Enrich entry_values with attribute slug and resolved labels
entry_values_enriched as (
    select
        ev.entry_value_id,
        ev.entry_id,
        ev.attribute_slug,
        la.type             as attribute_type,
        ev.value,
        coalesce(sl.status_label, ol.option_label, ev.value) as resolved_value
    from entry_values ev
    left join list_attrs la
        on ev.attribute_slug = la.api_slug
    left join status_labels sl
        on la.attribute_id = sl.attribute_id
        and ev.value = sl.status_id
    left join option_labels ol
        on la.attribute_id = ol.attribute_id
        and ev.value = ol.option_id
),

-- Pivot common pipeline entry attributes
pivoted as (
    select
        entry_id,
        max(case when attribute_slug = 'stage'      then resolved_value end)    as stage,
        max(case when attribute_slug = 'status'     then resolved_value end)    as status,
        max(case when attribute_slug = 'value'      then value end)             as deal_value,
        max(case when attribute_slug = 'close_date' then value end)             as close_date,
        max(case when attribute_slug = 'owner'      then value end)             as owner_member_id,
        max(case when attribute_slug = 'priority'   then resolved_value end)    as priority
    from entry_values_enriched
    group by entry_id
)

select
    e.entry_id,
    e.list_id,
    l.list_name,
    l.list_slug,
    l.object_id,
    e.record_id,
    p.stage,
    p.status,
    p.deal_value,
    p.close_date,
    p.owner_member_id,
    p.priority,
    e.created_at
from entries e
left join lists l    on e.list_id = l.list_id
left join pivoted p  on e.entry_id = p.entry_id
