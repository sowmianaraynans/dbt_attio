/*
  dim_attio__workspace_members
  ----------------------------
  Clean dimension of Attio workspace users, used for resolving
  value_workspace_member_id references in record/entry values.
*/

select
    workspace_member_id,
    first_name,
    last_name,
    full_name,
    email_address,
    access_level,
    is_confirmed,
    created_at
from {{ ref('stg_attio__workspace_members') }}
