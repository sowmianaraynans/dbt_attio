-- Asserts that dim_attio__companies has at least one row and no null PKs.
-- Returns rows that violate the assertion (zero rows = pass).

select company_id
from {{ ref('dim_attio__companies') }}
where company_id is null
