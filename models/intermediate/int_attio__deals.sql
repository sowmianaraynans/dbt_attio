/*
  int_attio__deals
  ----------------
  Records filtered to the deals object, with status/stage labels resolved
  from the option/status dimension tables.
*/

with records as (
    select * from {{ ref('stg_attio__records') }}
),

objects as (
    select * from {{ ref('stg_attio__objects') }}
),

pivoted as (
    select * from {{ ref('int_attio__pivoted_records') }}
),

object_attrs as (
    select
        attribute_id,
        api_slug,
        object_id
    from {{ ref('stg_attio__object_attributes') }}
    where api_slug in ('stage', 'status')
),

status_labels as (
    select
        s.status_id,
        s.attribute_id,
        s.title     as status_label
    from {{ ref('stg_attio__object_attribute_statuses') }} s
),

{% if var('attio__deal_custom_attributes', []) | length > 0 %}
custom_values as (
    select
        record_id,
        {% for slug in var('attio__deal_custom_attributes') %}
        max(case when attribute_slug = '{{ slug }}' then value end) as {{ slug }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('stg_attio__record_values') }}
    where active_until is null
    group by record_id
),
{% endif %}

deals as (
    select
        r.record_id,
        r.object_id,
        r.workspace_id,
        p.name                  as deal_name,
        p.status                as status_id,
        coalesce(sl.status_label, p.status) as status_label,
        p.stage                 as stage_id,
        p.deal_value,
        p.close_date,
        p.owner                 as owner_member_id,
        p.description
        {% if var('attio__deal_custom_attributes', []) | length > 0 %}
        {% for slug in var('attio__deal_custom_attributes') %}
        , c.{{ slug }}
        {% endfor %}
        {% endif %}
        ,r.created_at
    from records r
    inner join objects o
        on r.object_id = o.object_id
        and o.api_slug = '{{ var("attio__deal_object_slug") }}'
    left join pivoted p
        on r.record_id = p.record_id
    left join object_attrs oa
        on o.object_id = oa.object_id
        and oa.api_slug = 'status'
    left join status_labels sl
        on oa.attribute_id = sl.attribute_id
        and p.status = sl.status_id
    {% if var('attio__deal_custom_attributes', []) | length > 0 %}
    left join custom_values c
        on r.record_id = c.record_id
    {% endif %}
)

select * from deals
