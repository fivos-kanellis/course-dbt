{{
  config(
    materialized='table'
  )
}}

select p.product_id
,p.name
,p.price
,p.inventory
,o.orders
,o.total_ordered_quantity
,o.users_ordered
,o.orders_with_promo
,o.first_ordered_at
,o.last_ordered_at
,e.page_views
,e.unique_user_page_views
,e.added_to_cart
,e.unique_users_added_to_cart
FROM {{ source('postgres', 'products') }} p
LEFT JOIN {{ ref('int_product_orders_agg')}} o on p.product_id = o.product_id
LEFT JOIN {{ ref('int_product_events_agg')}} e on p.product_id = e.product_id






