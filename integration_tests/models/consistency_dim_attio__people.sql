-- Asserts that dim_attio__people has no null PKs.
-- Returns rows that violate the assertion (zero rows = pass).

select person_id
from {{ ref('dim_attio__people') }}
where person_id is null
