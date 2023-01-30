
--##### WEEK 1 Project #####--

--=== 1. What is our overall conversion rate? ===--

```

SELECT count(DISTINCT session_id) AS total_sessions
	,count(DISTINCT o.order_id) AS converted_sessions
	,converted_sessions / total_sessions AS cvr
FROM stg_postgres_events e
LEFT JOIN stg_postgres_orders o ON o.order_id = e.order_id

```