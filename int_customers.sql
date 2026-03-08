  with stg_customers as (
select customer_id
      ,customer_name
      ,customer_email
      ,signup_at
      ,region_raw
      ,marketing_source_raw
  from {{ ref('stg_customers') }}
)
select customer_id
      ,customer_name
      ,customer_email
      ,cast(signup_at as date) as signup_date
      ,case when region_raw = 'None' then cast(null as varchar)
            else region_raw 
        end as region
      ,case when marketing_source_raw = 'organic' then 'Organic'
            when marketing_source_raw = 'paid_social' then 'Paid Social'
            when marketing_source_raw = 'google_ads' then 'Google Ads' 
            when marketing_source_raw = 'referral' then 'Referral'
        end as marketing_source
  from stg_customers