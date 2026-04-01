/*
  int_attio__contact_notes
  ------------------------
  Notes scoped to people (contacts) only, joined to author details.
  Used by fct_attio__notes mart and for contact-level note counts.
*/

with notes as (
    select * from {{ ref('stg_attio__notes') }}
),

people as (
    select record_id
    from {{ ref('int_attio__people') }}
),

members as (
    select
        workspace_member_id,
        full_name       as author_name,
        email_address   as author_email
    from {{ ref('stg_attio__workspace_members') }}
)

select
    n.note_id,
    n.record_id,
    n.title,
    n.body,
    n.created_by_workspace_member_id,
    m.author_name,
    m.author_email,
    n.created_at,
    n.updated_at
from notes n
inner join people p
    on n.record_id = p.record_id
left join members m
    on n.created_by_workspace_member_id = m.workspace_member_id
