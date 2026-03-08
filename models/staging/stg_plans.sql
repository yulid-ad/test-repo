  with source as (
select id
      ,name
      ,monthly_cost
  from {{ source('raw', 'plans') }}
)
, renamed as (
select id::varchar(50) as plan_id
      ,name::varchar(100) as plan_name
      ,monthly_cost::decimal(18, 2) as monthly_cost
  from source
)
select plan_id
      ,plan_name
      ,monthly_cost
from renamed
