-- Singular test: no record should be missing both record_id and object_id
-- Returns rows that violate the constraint (dbt fails if any rows returned)

select
    record_id,
    object_id
from {{ ref('stg_attio__records') }}
where record_id is null
   or object_id is null
