
{{
  config(
    materialized='table'
  )
}}


select p.product_id
,count(distinct oi.order_id) as orders
,sum(oi.quantity) as total_ordered_quantity
,count(distinct o.user_id) as users_ordered
,count(distinct case when o.promo_id is not null then o.order_id else null end) as orders_with_promo
,min(o.created_at) as first_ordered_at
,max(o.created_at) as last_ordered_at
FROM {{ source('postgres', 'products') }} p
LEFT JOIN {{ source('postgres', 'order_items') }} oi on p.product_id = oi.product_id
LEFT JOIN {{ source('postgres', 'orders') }} o on oi.order_id = o.order_id

GROUP BY 1