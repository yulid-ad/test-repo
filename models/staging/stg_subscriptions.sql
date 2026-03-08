  with source as (
select id
      ,customer_id
      ,plan_id
      ,status
      ,start_date
      ,end_date
  from {{ source('raw', 'subscriptions') }}
)
, renamed as (
select id::varchar(50) as subscription_id
      ,customer_id::int as customer_id
      ,plan_id::varchar(50) as plan_id
      ,lower(status)::varchar(50) as status_code
      ,start_date::date as start_date
      ,end_date::date as end_date
  from source
)
select subscription_id
      ,customer_id
      ,plan_id
      ,status_code
      ,start_date
      ,end_date
  from renamed
