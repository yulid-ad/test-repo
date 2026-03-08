  with subscriptions as (
select subscription_id
      ,customer_id
      ,plan_id
      ,status_code
      ,start_date
      ,end_date
  from {{ ref('stg_subscriptions') }}
)
, plans as (
select plan_id
      ,plan_name
      ,monthly_cost
  from {{ ref('stg_plans') }}
)
, subscription_details as (
select subscription_id
      ,customer_id
      ,p.plan_name
      ,case when status_code = 'active' then 'Active' 
            when status_code in ('cancelled','canceled') then 'Cancelled' 
            when status_code = 'pending' then 'Pending'   
        end as plan_status
      ,start_date
      ,end_date
      ,p.monthly_cost
  from subscriptions s
  join plans p on s.plan_id = p.plan_id
)
select subscription_id
      ,customer_id
      ,plan_name
      ,plan_status
      ,start_date
      ,end_date
      ,monthly_cost
  from subscription_details
