with source as (
  select
    *
  from {{ source('jjsource', 'customer') }}
),
final as (
  select *
    , source.customer_first_name || ' ' || source.customer_last_name as customer_full_name
  from
    source
)
select * from final

