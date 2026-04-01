/*
  dim_attio__companies
  --------------------
  One row per company record. The authoritative dimension for company data.
*/

with companies as (
    select * from {{ ref('int_attio__companies') }}
),

note_counts as (
    select
        record_id,
        count(*) as note_count
    from {{ ref('stg_attio__notes') }}
    group by record_id
),

members as (
    select
        workspace_member_id,
        full_name   as owner_name
    from {{ ref('stg_attio__workspace_members') }}
)

select
    c.record_id                     as company_id,
    c.company_name,
    c.primary_domain,
    c.description,
    c.categories,
    c.employee_range,
    c.annual_revenue,
    c.primary_location,
    c.logo_url,
    c.foundation_date,
    c.linkedin_url,
    c.twitter_handle,
    c.owner_member_id,
    m.owner_name,
    coalesce(nc.note_count, 0)      as note_count,
    c.created_at
    -- custom attributes are passed through from int_attio__companies
    {% for slug in var('attio__company_custom_attributes', []) %}
    , c.{{ slug }}
    {% endfor %}
from companies c
left join members m
    on c.owner_member_id = m.workspace_member_id
left join note_counts nc
    on c.record_id = nc.record_id
