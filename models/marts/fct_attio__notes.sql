/*
  fct_attio__notes
  ----------------
  One row per note, joined to record context and author info.
*/

with notes as (
    select * from {{ ref('stg_attio__notes') }}
),

records as (
    select
        record_id,
        object_id
    from {{ ref('stg_attio__records') }}
),

objects as (
    select
        object_id,
        api_slug    as object_slug,
        singular_noun as object_type
    from {{ ref('stg_attio__objects') }}
),

members as (
    select
        workspace_member_id,
        full_name   as author_name,
        email_address as author_email
    from {{ ref('dim_attio__workspace_members') }}
),

final as (
    select
        n.note_id,
        n.record_id,
        r.object_id,
        o.object_slug,
        o.object_type,
        n.title,
        n.body,
        n.created_by_workspace_member_id,
        m.author_name,
        m.author_email,
        n.created_at,
        n.updated_at
    from notes n
    left join records r      on n.record_id = r.record_id
    left join objects o      on r.object_id = o.object_id
    left join members m      on n.created_by_workspace_member_id = m.workspace_member_id
)

select * from final
