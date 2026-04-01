-- Singular test: every record_value must reference an existing record
select
    rv.record_value_id,
    rv.record_id
from {{ ref('stg_attio__record_values') }} rv
left join {{ ref('stg_attio__records') }} r
    on rv.record_id = r.record_id
where r.record_id is null
