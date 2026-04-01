{% macro pivot_record_values(slugs, custom_slugs_var=none) %}
{#
  Generates MAX(CASE WHEN attribute_slug = '...' THEN value END) AS <slug>
  for every slug in `slugs` (standard) plus any slugs in the dbt var
  referenced by `custom_slugs_var`.

  Usage inside a CTE that already has `record_id` and `attribute_slug`, `value`:

    select
        record_id,
        {{ pivot_record_values(
            slugs=['name', 'email_addresses', 'domains'],
            custom_slugs_var='attio__company_custom_attributes'
        ) }}
    from ...
    group by record_id
#}
{% set all_slugs = slugs %}
{% if custom_slugs_var %}
    {% set all_slugs = slugs + var(custom_slugs_var, []) %}
{% endif %}
{% for slug in all_slugs %}
    max(case when attribute_slug = '{{ slug }}' then value end) as {{ slug }}
    {%- if not loop.last %},{% endif %}
{% endfor %}
{% endmacro %}
