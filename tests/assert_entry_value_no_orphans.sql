-- Singular test: every entries_value must reference an existing entry
select
    ev.entry_value_id,
    ev.entry_id
from {{ ref('stg_attio__entries_values') }} ev
left join {{ ref('stg_attio__entries') }} e
    on ev.entry_id = e.entry_id
where e.entry_id is null
