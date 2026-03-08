{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['year', 'month'],
        unique_key='mrr_sk',
        on_schema_change='fail'
    )
}}
  with customers as (
select customer_id
      ,customer_name
      ,customer_email
      ,signup_date
      ,region
      ,marketing_source
  from {{ ref('int_customers') }}
)
, subscriptions as (
select subscription_id
      ,customer_id
      ,plan_name
      ,subscription_status
      ,start_date
      ,end_date
      ,monthly_cost
  from {{ ref('int_subscriptions') }}
 where subscription_status in ('Active', 'Cancelled')
)
, date_spine as (
{{ dbt_utils.date_spine(
      datepart="day",
      start_date="2024-01-01",
      end_date="2025-05-31"
     )
  }}
)
, customer_subscription_daily as (
select d.date_day
      ,cast(date_trunc('month',d.date_day) as date) as date_month
      ,s.subscription_id
      ,s.customer_id
      ,s.plan_name
      ,s.subscription_status
      ,s.start_date
      ,s.end_date
      ,s.monthly_cost
  from date_spine d
 cross join subscriptions s
 where s.start_date <= d.date_day
   and (s.end_date is null or s.end_date >= d.date_day)

   {% if is_incremental() %}
   and date(date_trunc('month', d.date_day)) >= date_add(month, -2, date_trunc('month', current_date))
   {% endif %}

)
, customer_subscription_monthly as (
select date_month
      ,subscription_id
      ,customer_id
      ,monthly_cost * count(date_day) / 30 as monthly_revenue
      ,start_date
      ,end_date
  from customer_subscription_daily
 group by date_month, subscription_id, customer_id, start_date, end_date, monthly_cost
)
, final as (
select date_format(date_month, '%Y') as year
      ,date_format(date_month, '%m') as month
      ,customer_id
      ,sum(monthly_revenue) as monthly_recurring_revenue
  from customer_subscription_monthly
 group by date_month, customer_id
)
select {{ dbt_utils.generate_surrogate_key(['year', 'month', 'customer_id']) }} as mrr_sk
      ,year
      ,month
      ,customer_id
      ,monthly_recurring_revenue
  from final
