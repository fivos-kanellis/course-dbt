--##### WEEK 2 Project #####--


--Notes:
-- I've created 2 intermediate tables summarizing Sessions and Order info for each user. 
-- I then created a user summary table under the marketing model summarizing all relevant information by user (combining Sessions + Orders) which we can continue enriching with information.
-- The goal is to enable analysts to retrieve User stats fast and with short code: i.e. total lifetime value, orders that were / are running late for each user, total checkouts etc.
-- This makes answers to this week's project questions become a simple query as the work has been taken care of in intermediate and dim/facts (i.e. marketing) model layers.

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
SELECT avg(total_session_duration)/60 as avg_hours_spent_on_site
FROM dev_db.dbt_fkanellisgmailcom.dim_user_summary;

```

-- Hours spent on site
-- ~15hr / user



