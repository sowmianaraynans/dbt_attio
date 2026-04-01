/*
  fct_attio__deals
  ----------------
  One row per deal record with resolved status labels and owner details.
  Pipeline grain — join to fct_attio__list_entries for stage-level history.
*/

with deals as (
    select * from {{ ref('int_attio__deals') }}
),

members as (
    select
        workspace_member_id,
        full_name   as owner_name,
        email_address as owner_email
    from {{ ref('stg_attio__workspace_members') }}
)

select
    d.record_id                     as deal_id,
    d.deal_name,
    d.status_id,
    d.status_label,
    d.stage_id,
    d.deal_value,
    d.close_date,
    d.description,
    d.owner_member_id,
    m.owner_name,
    m.owner_email,
    d.created_at
    {% for slug in var('attio__deal_custom_attributes', []) %}
    , d.{{ slug }}
    {% endfor %}
from deals d
left join members m
    on d.owner_member_id = m.workspace_member_id
