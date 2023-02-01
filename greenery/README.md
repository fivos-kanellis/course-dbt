
--##### WEEK 1 Project #####--

--=== 1a. What is our overall conversion rate? ===--

```

SELECT count(DISTINCT session_id) AS total_sessions
	,count(DISTINCT o.order_id) AS converted_sessions
	,converted_sessions / total_sessions AS cvr
FROM stg_postgres_events e
LEFT JOIN stg_postgres_orders o ON o.order_id = e.order_id

```
--=== 1b. What is our conversion rate by product? ===--

-- Utilizing Product Summary table in Product data mart. Metrics are available at the unique product grain and can derive conversion rate by dividing orders including a product with unique sessions including the same product
```
SELECT name AS product_name
	,unique_sessions
	,page_views
	,orders
	,users_ordered
	,unique_user_page_views
	,orders / unique_sessions AS cvr
FROM dev_db.dbt_fkanellisgmailcom.DIM_PRODUCT_SUMMARY

```