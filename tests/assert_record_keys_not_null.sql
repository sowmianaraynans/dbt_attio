-- Singular test: no record should be missing record_id or object_id
select
    record_id,
    object_id
from {{ ref('stg_attio__records') }}
where record_id is null
   or object_id is null
