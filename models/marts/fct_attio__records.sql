/*
  fct_attio__records
  ------------------
  One row per CRM record, enriched with:
    - object type metadata (slug, noun)
    - key EAV values pivoted into columns via a lateral join pattern
    - note and entry counts

  Because attribute slugs vary per workspace, the pivoted columns here use
  the most common Attio standard attributes. Add workspace-specific slugs
  to the pivot CTEs as needed.
*/

with records as (
    select * from {{ ref('stg_attio__records') }}
),

objects as (
    select * from {{ ref('stg_attio__objects') }}
),

-- Flatten record_values by joining attribute slug for pivot
record_values_with_slug as (
    select
        rv.record_id,
        oa.api_slug,
        oa.type                         as attribute_type,
        rv.value_text,
        rv.value_number,
        rv.value_boolean,
        rv.value_date,
        rv.value_timestamp,
        rv.value_record_reference_id,
        rv.value_workspace_member_id,
        rv.value_option_id,
        rv.value_status_id,
        rv.value_currency_value,
        rv.value_currency_currency_code,
        rv.active_from,
        rv.active_until
    from {{ ref('stg_attio__record_values') }} rv
    left join {{ ref('stg_attio__object_attributes') }} oa
        on rv.attribute_id = oa.attribute_id
    -- Only current values (no active_until means still active)
    where rv.active_until is null
),

-- Pivot common standard attributes — extend this list for your workspace
pivoted as (
    select
        record_id,
        max(case when api_slug = 'name'             then value_text end)        as name,
        max(case when api_slug = 'email_addresses'  then value_text end)        as primary_email,
        max(case when api_slug = 'phone_numbers'    then value_text end)        as primary_phone,
        max(case when api_slug = 'domains'          then value_text end)        as primary_domain,
        max(case when api_slug = 'description'      then value_text end)        as description,
        max(case when api_slug = 'linkedin'         then value_text end)        as linkedin_url,
        max(case when api_slug = 'twitter'          then value_text end)        as twitter_handle,
        max(case when api_slug = 'categories'       then value_text end)        as categories,
        max(case when api_slug = 'employee_range'   then value_text end)        as employee_range,
        max(case when api_slug = 'annual_revenue'   then value_currency_value end) as annual_revenue,
        max(case when api_slug = 'annual_revenue'   then value_currency_currency_code end) as revenue_currency,
        max(case when api_slug = 'primary_location' then value_text end)        as primary_location,
        max(case when api_slug = 'job_title'        then value_text end)        as job_title
    from record_values_with_slug
    group by record_id
),

note_counts as (
    select
        record_id,
        count(*) as note_count
    from {{ ref('stg_attio__notes') }}
    group by record_id
),

final as (
    select
        r.record_id,
        r.object_id,
        o.api_slug                          as object_slug,
        o.singular_noun                     as object_type,
        p.name,
        p.primary_email,
        p.primary_phone,
        p.primary_domain,
        p.description,
        p.linkedin_url,
        p.twitter_handle,
        p.categories,
        p.employee_range,
        p.annual_revenue,
        p.revenue_currency,
        p.primary_location,
        p.job_title,
        coalesce(nc.note_count, 0)          as note_count,
        r.created_at
    from records r
    left join objects o
        on r.object_id = o.object_id
    left join pivoted p
        on r.record_id = p.record_id
    left join note_counts nc
        on r.record_id = nc.record_id
)

select * from final
