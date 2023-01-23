--##### WEEK 2 Project #####--

--=== 1. What is our user repeat rate ===--

```
SELECT div0(count(DISTINCT CASE 
			WHEN repeat_customer
				THEN user_id
			END) , count(1)) AS repeat_rate
FROM dev_db.dbt_fkanellisgmailcom.dim_user_summary
WHERE has_ordered

```

-- REPEAT_RATE
-- 0.798387


--=== 2. How long are people spending on our site ===--


```
SELECT div0(count(DISTINCT CASE 
			WHEN repeat_customer
				THEN user_id
			END) , count(1)) AS repeat_rate
FROM dev_db.dbt_fkanellisgmailcom.dim_user_summary
WHERE has_ordered

```

-- REPEAT_RATE
-- 0.798387
