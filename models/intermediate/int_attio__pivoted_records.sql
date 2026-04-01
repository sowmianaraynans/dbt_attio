/*
  int_attio__pivoted_records
  --------------------------
  Generic pivot of record_value EAV → one wide row per record.

  Covers all standard Attio attributes shared across object types.
  Object-specific intermediates (int_attio__companies, etc.) filter
  this model by object_slug and add any object-specific columns.

  To add custom workspace attributes, set vars in dbt_project.yml:
    attio__company_custom_attributes: ['arr', 'tier']
*/

with record_values as (
    select * from {{ ref('stg_attio__record_values') }}
    -- Only current (non-historical) values
    where active_until is null
),

pivoted as (
    select
        record_id,
        record_object_id,
        -- Identity
        max(case when attribute_slug = 'name'               then value end) as name,
        -- People
        max(case when attribute_slug = 'email_addresses'    then value end) as email_addresses,
        max(case when attribute_slug = 'phone_numbers'      then value end) as phone_numbers,
        max(case when attribute_slug = 'job_title'          then value end) as job_title,
        max(case when attribute_slug = 'linkedin'           then value end) as linkedin_url,
        max(case when attribute_slug = 'twitter'            then value end) as twitter_handle,
        max(case when attribute_slug = 'avatar_url'         then value end) as avatar_url,
        -- Companies
        max(case when attribute_slug = 'domains'            then value end) as primary_domain,
        max(case when attribute_slug = 'description'        then value end) as description,
        max(case when attribute_slug = 'categories'         then value end) as categories,
        max(case when attribute_slug = 'employee_range'     then value end) as employee_range,
        max(case when attribute_slug = 'annual_revenue'     then value end) as annual_revenue,
        max(case when attribute_slug = 'primary_location'   then value end) as primary_location,
        max(case when attribute_slug = 'logo_url'           then value end) as logo_url,
        max(case when attribute_slug = 'foundation_date'    then value end) as foundation_date,
        -- Deals / generic
        max(case when attribute_slug = 'status'             then value end) as status,
        max(case when attribute_slug = 'stage'              then value end) as stage,
        max(case when attribute_slug = 'value'              then value end) as deal_value,
        max(case when attribute_slug = 'close_date'         then value end) as close_date,
        max(case when attribute_slug = 'owner'              then value end) as owner,
        -- Shared metadata
        max(case when attribute_slug = 'type'               then value end) as record_type
    from record_values
    group by record_id, record_object_id
)

select * from pivoted
