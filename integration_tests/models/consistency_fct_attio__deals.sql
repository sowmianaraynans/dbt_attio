-- Asserts that fct_attio__deals has no null PKs (only runs if attio__using_deals=true).
-- Returns rows that violate the assertion (zero rows = pass).

{{ config(enabled=var('attio__using_deals', true)) }}

select deal_id
from {{ ref('fct_attio__deals') }}
where deal_id is null
