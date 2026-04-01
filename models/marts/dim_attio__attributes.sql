/*
  dim_attio__attributes
  ---------------------
  Union of object attributes and list attributes into a single attribute
  dimension, enriched with their option/status labels.
*/

with object_attrs as (
    select
        attribute_id,
        'object'    as source_type,
        object_id   as parent_id,
        api_slug,
        title,
        type,
        is_required,
        is_unique,
        is_multiselect,
        is_archived,
        created_at
    from {{ ref('stg_attio__object_attributes') }}
),

list_attrs as (
    select
        attribute_id,
        'list'      as source_type,
        list_id     as parent_id,
        api_slug,
        title,
        type,
        is_required,
        is_unique,
        is_multiselect,
        is_archived,
        created_at
    from {{ ref('stg_attio__list_attributes') }}
),

all_attrs as (
    select * from object_attrs
    union all
    select * from list_attrs
),

-- Aggregate option labels per attribute
object_options as (
    select
        attribute_id,
        string_agg(title, ', ' order by title) as option_labels
    from {{ ref('stg_attio__object_attribute_options') }}
    where not is_archived
    group by attribute_id
),

list_options as (
    select
        attribute_id,
        string_agg(title, ', ' order by title) as option_labels
    from {{ ref('stg_attio__list_attribute_options') }}
    where not is_archived
    group by attribute_id
),

object_statuses as (
    select
        attribute_id,
        string_agg(title, ', ' order by title) as status_labels
    from {{ ref('stg_attio__object_attribute_statuses') }}
    where not is_archived
    group by attribute_id
),

list_statuses as (
    select
        attribute_id,
        string_agg(title, ', ' order by title) as status_labels
    from {{ ref('stg_attio__list_attribute_statuses') }}
    where not is_archived
    group by attribute_id
),

options_combined as (
    select * from object_options
    union all
    select * from list_options
),

statuses_combined as (
    select * from object_statuses
    union all
    select * from list_statuses
),

final as (
    select
        a.attribute_id,
        a.source_type,
        a.parent_id,
        a.api_slug,
        a.title,
        a.type,
        a.is_required,
        a.is_unique,
        a.is_multiselect,
        a.is_archived,
        o.option_labels,
        s.status_labels,
        a.created_at
    from all_attrs a
    left join options_combined o  on a.attribute_id = o.attribute_id
    left join statuses_combined s on a.attribute_id = s.attribute_id
)

select * from final
