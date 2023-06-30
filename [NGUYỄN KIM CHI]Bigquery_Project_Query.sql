-- In this project, I write 08 query in Bigquery base on Google Analytics dataset.

-- Table Schema : https://support.google.com/analytics/answer/3437719?hl=en


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

#standardSQL

SELECT 
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
  sum(totals.visits) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX between '20170101' and '20170331'
GROUP BY month
ORDER BY month

-- Query 02: Bounce rate per traffic source in July 2017 ( (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
#standardSQL

SELECT 
  trafficSource.source as source,
  sum(totals.visits) as total_visits,
  sum(totals.bounces) as total_no_of_bounces,
  (sum(totals.bounces)/sum(totals.visits))*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY 1
ORDER BY 2 desc


-- Query 3: Revenue by traffic source by week, by month in June 2017

with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
ORDER BY revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
ORDER BY revenue DESC
)

SELECT * 
FROM month_data
UNION ALL
SELECT * 
FROM week_data


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

with purchase_data as(
SELECT
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
  (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix between '0601' and '0731'
AND totals.transactions>=1
GROUP BY month
),

nonpurrchase_data as(
SELECT
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
  (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix between '0601' and '0731'
AND totals.transactions is null
GROUP BY month
)

SELECT *
FROM purchase_data
JOIN nonpurrchase_data
USING(month)
ORDER BY month


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
  sum(totals.transactions)/ count(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >=1 
GROUP BY month

-- Query 06: Average amount of money spent per session
#standardSQL

SELECT
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
  ((sum(totals.totalTransactionRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE  totals.transactions is not null
GROUP BY month

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

with buyer_list as(
SELECT
  distinct fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) as product
WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
AND totals.transactions>=1
AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
AND product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL



with product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(hits.eCommerceAction.action_type) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) AS hits
WHERE _table_suffix between '0101' and '0331'
AND hits.eCommerceAction.action_type = '2' 
GROUP BY month
ORDER BY month
    ),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(hits.eCommerceAction.action_type) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, 
  UNNEST(hits) AS hits
WHERE _table_suffix between '0101' and '0331'
AND hits.eCommerceAction.action_type = '3' 
GROUP BY month
ORDER BY month
    ),

num_purchase as (
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(hits.eCommerceAction.action_type) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, 
  UNNEST(hits) AS hits
WHERE _table_suffix between '0101' and '0331'
AND hits.eCommerceAction.action_type = '6' 
GROUP BY month
ORDER BY month
    )

SELECT 
  pv.*,
  num_addtocart,
  num_purchase,
  round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
  round(num_purchase*100/num_product_view,2) as purchase_rate
FROM product_view as pv
JOIN add_to_cart a on a.month = pv.month 
JOIN  num_purchase n on n.month = pv.month
ORDER BY pv.month
