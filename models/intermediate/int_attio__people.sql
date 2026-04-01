/*
  int_attio__people
  -----------------
  Records filtered to the people object, with person-relevant pivoted
  attributes and custom attribute support via dbt var.
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

{% if var('attio__person_custom_attributes', []) | length > 0 %}
custom_values as (
    select
        record_id,
        {% for slug in var('attio__person_custom_attributes') %}
        max(case when attribute_slug = '{{ slug }}' then value end) as {{ slug }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('stg_attio__record_values') }}
    where active_until is null
    group by record_id
),
{% endif %}

people as (
    select
        r.record_id,
        r.object_id,
        r.workspace_id,
        p.name                  as full_name,
        p.email_addresses       as primary_email,
        p.phone_numbers         as primary_phone,
        p.job_title,
        p.linkedin_url,
        p.twitter_handle,
        p.avatar_url,
        p.primary_location,
        p.owner                 as owner_member_id,
        p.record_type
        {% if var('attio__person_custom_attributes', []) | length > 0 %}
        {% for slug in var('attio__person_custom_attributes') %}
        , c.{{ slug }}
        {% endfor %}
        {% endif %}
        ,r.created_at
    from records r
    inner join objects o
        on r.object_id = o.object_id
        and o.api_slug = '{{ var("attio__person_object_slug") }}'
    left join pivoted p
        on r.record_id = p.record_id
    {% if var('attio__person_custom_attributes', []) | length > 0 %}
    left join custom_values c
        on r.record_id = c.record_id
    {% endif %}
)

select * from people
