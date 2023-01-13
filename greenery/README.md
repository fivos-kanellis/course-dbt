
--##### WEEK 1 Project #####--

--=== 1. How many users do we have? ===--

```
SELECT count(1)
FROM dev_db.dbt_fkanellisgmailcom.stg_postgres_users
```
--Results: 130 users

--=== 2. On average, how many orders do we receive per hour? ===--

```
SELECT count(DISTINCT o.order_id) --unique orders
	/ datediff(hour, min(created_at), max(created_at)) --hours recorded
	AS hourly_orders
FROM dev_db.dbt_fkanellisgmailcom.stg_postgres_orders o
```
--Results: 7.680851


--=== 3. On average, how long does an order take from being placed to being delivered? ===--

```
SELECT avg(datediff(day, o.created_at, o.delivered_at)) AS days_to_deliver
	,avg(datediff(hour, o.created_at, o.delivered_at)) AS hours_to_deliver
	,avg(datediff(minute, o.created_at, o.delivered_at)) AS minutes_to_deliver
FROM dev_db.dbt_fkanellisgmailcom.stg_postgres_orders o
WHERE o.STATUS = 'delivered' 
``` 
 

--Results: 3.891803 days OR 93.403279 hours OR 5,604.196721 minutes



--=== 4. How many users have only made one purchase? Two purchases? Three+ purchases? ===--

```
WITH user_order_counts
AS (
	SELECT user_id
		,count(DISTINCT order_id) AS orders
	FROM dev_db.dbt_fkanellisgmailcom.stg_postgres_orders o
	GROUP BY 1
	)
SELECT CASE 
		WHEN orders >= 3
			THEN '3+'
		ELSE to_varchar(orders)
		END AS orders
	,count(DISTINCT user_id) AS users
FROM user_order_counts
GROUP BY 1
ORDER BY 1 ASC

--Results: 
-- 1 order: 25 users
-- 2 orders: 28 users
-- 3+ orders: 71 users

    ╔════════╦═══════╗
    ║ ORDERS ║ USERS ║
    ╠════════╬═══════╣
    ║ 1      ║ 25    ║
    ╠════════╬═══════╣
    ║ 2      ║ 28    ║
    ╠════════╬═══════╣
    ║ 3+     ║ 71    ║
    ╚════════╩═══════╝
```

--Note: you should consider a purchase to be a single order. In other words, if a user places one order for 3 products, they are considered to have made 1 purchase. 

--=== 5. On average, how many unique sessions do we have per hour? ===--

```
SELECT count(DISTINCT e.session_id) --unique sessions
	/ datediff(hour, min(created_at), max(created_at)) --hours captured
	AS hourly_sessions
FROM dev_db.dbt_fkanellisgmailcom.stg_postgres_events e
```
--Results: 10.140351 sessions / hour 

--Note: If we measure time from the first created session until the last created session, on average the site had 10.140351 unique sessions / hour. If we only measure hours with at least 1 session being created the sessions/hour number will increase.
