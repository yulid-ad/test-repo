  with source as (
select id
      ,name
      ,email
      ,signup_at
      ,metadata
  from {{ source('raw','customers') }}
)
, renamed as (
select id::int as customer_id
      ,name::varchar(255) as customer_name
      ,email::varchar(255) as customer_email
      ,signup_at::timestamp as signup_at
      ,json_parse(metadata) as metadata_super
  from source
)
, final as (
select customer_id
      ,customer_name
      ,customer_email
      ,signup_at
      ,metadata_super."region"::varchar(50) as region_raw
      ,metadata_super."source"::varchar(50) as marketing_source_raw
  from renamed
)
select customer_id
      ,customer_name
      ,customer_email
      ,signup_at
      ,region_raw
      ,marketing_source_raw
  from final