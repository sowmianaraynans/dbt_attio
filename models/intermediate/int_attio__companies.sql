/*
  int_attio__companies
  --------------------
  Records filtered to the companies object, with company-relevant pivoted
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

{% if var('attio__company_custom_attributes', []) | length > 0 %}
custom_values as (
    select
        record_id,
        {% for slug in var('attio__company_custom_attributes') %}
        max(case when attribute_slug = '{{ slug }}' then value end) as {{ slug }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('stg_attio__record_values') }}
    where active_until is null
    group by record_id
),
{% endif %}

companies as (
    select
        r.record_id,
        r.object_id,
        r.workspace_id,
        p.name                  as company_name,
        p.primary_domain,
        p.description,
        p.categories,
        p.employee_range,
        p.annual_revenue,
        p.primary_location,
        p.logo_url,
        p.foundation_date,
        p.linkedin_url,
        p.twitter_handle,
        p.owner                 as owner_member_id,
        p.record_type
        {% if var('attio__company_custom_attributes', []) | length > 0 %}
        {% for slug in var('attio__company_custom_attributes') %}
        , c.{{ slug }}
        {% endfor %}
        {% endif %}
        ,r.created_at
    from records r
    inner join objects o
        on r.object_id = o.object_id
        and o.api_slug = '{{ var("attio__company_object_slug") }}'
    left join pivoted p
        on r.record_id = p.record_id
    {% if var('attio__company_custom_attributes', []) | length > 0 %}
    left join custom_values c
        on r.record_id = c.record_id
    {% endif %}
)

select * from companies
