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
      ,plan_status
      ,start_date
      ,end_date
      ,monthly_cost
  from {{ ref('int_subscriptions') }}
)
, subscription_lifetime_value as (
select subscription_id
      ,customer_id
      ,plan_name
      ,plan_status
      ,start_date
      ,monthly_cost
      ,case when plan_status = 'Pending' then 0
            else ceil(date_diff('day', start_date, coalesce(end_date, date('2025-05-31'))) / 30)
        end as subscription_duration_months
      ,case when plan_status = 'Pending' then 1 else 0 end as is_pending
      ,case when plan_status = 'Cancelled' then 1 else 0 end as is_cancelled
      ,case when plan_status = 'Active' then 1 else 0 end as is_active
  from subscriptions
)
, customer_subscription_metrics as (
select customer_id
      ,count(subscription_id) as total_subscriptions_count
      ,sum(is_pending) as pending_subscriptions_count
      ,sum(is_cancelled) as cancelled_subscriptions_count
      ,sum(is_active) as active_subscriptions_count
      ,sum(subscription_duration_months*monthly_cost) as total_lifetime_value
      ,array_join(array_agg(case when is_active = 1 then plan_name end), ', ') as current_active_plans
      ,min(start_date) as first_subscription_date
      ,case when sum(is_active) > 0 then 'Active'
            when sum(is_pending) > 0 then 'Pending'
            when sum(is_cancelled) > 0 then 'Cancelled'
       end as current_subscription_status
  from subscription_lifetime_value
 group by customer_id
)
select c.customer_id
      ,c.customer_name
      ,c.customer_email
      ,c.region
      ,c.marketing_source
      ,s.first_subscription_date
      ,case when s.current_subscription_status is not null then s.current_subscription_status
            else 'Never Subscribed'
        end as current_subscription_status
      ,s.current_active_plans
      ,coalesce(s.total_lifetime_value, 0) as total_lifetime_value
      ,coalesce(s.total_subscriptions_count, 0) as total_subscriptions_count
      ,coalesce(s.active_subscriptions_count, 0) as active_subscriptions_count
      ,coalesce(s.pending_subscriptions_count, 0) as pending_subscriptions_count
      ,coalesce(s.cancelled_subscriptions_count, 0) as cancelled_subscriptions_count
  from customers c
  left join customer_subscription_metrics s on c.customer_id = s.customer_id
 order by customer_id
