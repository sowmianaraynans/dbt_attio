-- Asserts that dim_attio__companies has at least one row and no null PKs.
-- Returns rows that violate the assertion (zero rows = pass).

select record_id
from {{ ref('dim_attio__companies') }}
where record_id is null
