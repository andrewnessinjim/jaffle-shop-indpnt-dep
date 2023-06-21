{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}
with order_amounts as (
    select orderid as order_id, amount, status from {{ source('public','payments') }}
),

customer_orders as (
    select "ID" as order_id, user_id as customer_id from {{ source('public','orders') }}
),

succesful_order_amounts as (
    select * from order_amounts where status='success'
)

select order_id, customer_id, sum(amount) as amount, count(amount) as num_items
from succesful_order_amounts left join customer_orders using (order_id)

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where 1 = 1

{% endif %}

group by order_id, customer_id
order by order_id