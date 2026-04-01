{{ config(enabled=var('attio__using_lists', true)) }}

/*
  fct_attio__list_entries
  -----------------------
  One row per list entry — the grain of a pipeline stage.
  Joins to dim_attio__companies or dim_attio__people via record_id
  depending on the list's object type.
*/

with pipeline as (
    select * from {{ ref('int_attio__list_pipeline') }}
),

members as (
    select
        workspace_member_id,
        full_name   as owner_name
    from {{ ref('stg_attio__workspace_members') }}
)

select
    p.entry_id,
    p.list_id,
    p.list_name,
    p.list_slug,
    p.object_id,
    p.record_id,
    p.stage,
    p.status,
    p.deal_value,
    p.close_date,
    p.owner_member_id,
    m.owner_name,
    p.priority,
    p.created_at
from pipeline p
left join members m
    on p.owner_member_id = m.workspace_member_id
