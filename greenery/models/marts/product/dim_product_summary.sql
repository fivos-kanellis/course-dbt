{{
  config(
    materialized='table'
  )
}}


WITH tmp_products as
(SELECT *
 FROM {{ ref('stg_postgres_products') }}
)
,tmp_orders_agg as 
(SELECT *
 FROM {{ ref('int_product_orders_agg')}}
)

,tmp_events_agg as 
(SELECT *
 FROM {{ ref('int_product_events_agg')}}
)


SELECT p.product_id
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
FROM tmp_products p
LEFT JOIN tmp_orders_agg o on p.product_id = o.product_id
LEFT JOIN tmp_events_agg e on p.product_id = e.product_id






