/*
  dim_attio__people
  -----------------
  One row per person (contact) record. The authoritative dimension for people.
*/

with people as (
    select * from {{ ref('int_attio__people') }}
),

note_counts as (
    select
        record_id,
        count(*) as note_count
    from {{ ref('int_attio__contact_notes') }}
    group by record_id
),

members as (
    select
        workspace_member_id,
        full_name   as owner_name
    from {{ ref('stg_attio__workspace_members') }}
)

select
    p.record_id                     as person_id,
    p.full_name,
    p.primary_email,
    p.primary_phone,
    p.job_title,
    p.linkedin_url,
    p.twitter_handle,
    p.avatar_url,
    p.primary_location,
    p.owner_member_id,
    m.owner_name,
    coalesce(nc.note_count, 0)      as note_count,
    p.created_at
    {% for slug in var('attio__person_custom_attributes', []) %}
    , p.{{ slug }}
    {% endfor %}
from people p
left join members m
    on p.owner_member_id = m.workspace_member_id
left join note_counts nc
    on p.record_id = nc.record_id
