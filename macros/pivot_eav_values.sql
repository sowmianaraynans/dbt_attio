{% macro pivot_eav_values(relation, id_col, attribute_slug_col, value_col, slugs) %}
/*
  Generic EAV pivot macro.

  Usage:
    {{ pivot_eav_values(
        relation      = ref('stg_attio__record_values'),
        id_col        = 'record_id',
        attribute_slug_col = 'api_slug',
        value_col     = 'value_text',
        slugs         = ['name', 'email_addresses', 'phone_numbers']
    ) }}

  Produces one column per slug using MAX(CASE WHEN ...) aggregation.
  For multi-value attributes, values are aggregated as comma-separated strings.
*/
select
    {{ id_col }},
    {% for slug in slugs %}
    max(case when {{ attribute_slug_col }} = '{{ slug }}' then {{ value_col }} end)
        as {{ slug }}
    {%- if not loop.last %},{%- endif %}
    {% endfor %}
from {{ relation }}
group by {{ id_col }}
{% endmacro %}
