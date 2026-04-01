-- Asserts that dim_attio__people has no null PKs.
-- Returns rows that violate the assertion (zero rows = pass).

select record_id
from {{ ref('dim_attio__people') }}
where record_id is null
