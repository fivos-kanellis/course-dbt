
______________________________
--##### WEEK 3 Project #####--
______________________________


--##### PART 1 #####--

--=== 1a. What is our overall conversion rate? ===--

```
SELECT count_if(is_converted_session) / count(1) AS CVR
FROM dev_db.dbt_fkanellisgmailcom.INT_SESSION_EVENTS_AGG
-- Result:0.624567 (62.5%)

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

--Results:
╔═════════════════════╦═════════════════╦════════════╦════════╦═══════════════╦════════════════════════╦══════════╗
║ PRODUCT_NAME        ║ UNIQUE_SESSIONS ║ PAGE_VIEWS ║ ORDERS ║ USERS_ORDERED ║ UNIQUE_USER_PAGE_VIEWS ║ CVR      ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ String of pearls    ║ 64              ║ 65         ║ 39     ║ 32            ║ 48                     ║ 0.609375 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Pilea Peperomioides ║ 59              ║ 60         ║ 28     ║ 27            ║ 38                     ║ 0.474576 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Philodendron        ║ 62              ║ 63         ║ 30     ║ 25            ║ 40                     ║ 0.483871 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Rubber Plant        ║ 54              ║ 56         ║ 28     ║ 26            ║ 36                     ║ 0.518519 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Orchid              ║ 75              ║ 75         ║ 34     ║ 33            ║ 44                     ║ 0.453333 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Bird of Paradise    ║ 60              ║ 60         ║ 27     ║ 24            ║ 40                     ║ 0.45     ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Angel Wings Begonia ║ 61              ║ 62         ║ 24     ║ 19            ║ 37                     ║ 0.393443 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Birds Nest Fern     ║ 78              ║ 80         ║ 33     ║ 28            ║ 47                     ║ 0.423077 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Peace Lily          ║ 66              ║ 67         ║ 27     ║ 24            ║ 42                     ║ 0.409091 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Pink Anthurium      ║ 74              ║ 74         ║ 31     ║ 24            ║ 43                     ║ 0.418919 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Spider Plant        ║ 59              ║ 59         ║ 28     ║ 25            ║ 41                     ║ 0.474576 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Majesty Palm        ║ 67              ║ 69         ║ 33     ║ 29            ║ 45                     ║ 0.492537 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Calathea Makoyana   ║ 53              ║ 53         ║ 27     ║ 22            ║ 36                     ║ 0.509434 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Pothos              ║ 61              ║ 64         ║ 21     ║ 21            ║ 39                     ║ 0.344262 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Monstera            ║ 49              ║ 49         ║ 25     ║ 22            ║ 37                     ║ 0.510204 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Ponytail Palm       ║ 70              ║ 71         ║ 28     ║ 28            ║ 47                     ║ 0.4      ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Devil's Ivy         ║ 45              ║ 45         ║ 22     ║ 22            ║ 32                     ║ 0.488889 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Boston Fern         ║ 63              ║ 63         ║ 26     ║ 25            ║ 38                     ║ 0.412698 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Ficus               ║ 68              ║ 68         ║ 29     ║ 27            ║ 42                     ║ 0.426471 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Alocasia Polly      ║ 51              ║ 54         ║ 21     ║ 19            ║ 29                     ║ 0.411765 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Arrow Head          ║ 63              ║ 64         ║ 35     ║ 28            ║ 38                     ║ 0.555556 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Dragon Tree         ║ 62              ║ 62         ║ 29     ║ 28            ║ 40                     ║ 0.467742 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Jade Plant          ║ 46              ║ 46         ║ 22     ║ 22            ║ 30                     ║ 0.478261 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Snake Plant         ║ 73              ║ 73         ║ 29     ║ 25            ║ 45                     ║ 0.39726  ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ ZZ Plant            ║ 63              ║ 65         ║ 34     ║ 30            ║ 48                     ║ 0.539683 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Money Tree          ║ 56              ║ 56         ║ 26     ║ 22            ║ 36                     ║ 0.464286 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Cactus              ║ 55              ║ 55         ║ 30     ║ 28            ║ 43                     ║ 0.545455 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Bamboo              ║ 67              ║ 69         ║ 36     ║ 31            ║ 43                     ║ 0.537313 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Aloe Vera           ║ 65              ║ 65         ║ 32     ║ 26            ║ 36                     ║ 0.492308 ║
╠═════════════════════╬═════════════════╬════════════╬════════╬═══════════════╬════════════════════════╬══════════╣
║ Fiddle Leaf Fig     ║ 56              ║ 59         ║ 28     ║ 24            ║ 43                     ║ 0.5      ║
╚═════════════════════╩═════════════════╩════════════╩════════╩═══════════════╩════════════════════════╩══════════╝

```


--##### PART 2 #####--

