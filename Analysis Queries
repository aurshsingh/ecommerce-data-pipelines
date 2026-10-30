-- ════════════════════════════════════════════════════════════════════════════
-- E-COMMERCE ANALYSIS  (MySQL 8+ / MariaDB 10.2+)
-- Target table: fact_orders  -> ONE ROW PER ORDER (payments pre-aggregated by
-- ecommerce_etl.py), so COUNT(*) = number of orders. No DISTINCT tricks needed.
-- ════════════════════════════════════════════════════════════════════════════

-- 0. Table overview ----------------------------------------------------------
DESCRIBE fact_orders;
SHOW CREATE TABLE fact_orders;


-- 1. Headline KPIs -----------------------------------------------------------
SELECT COUNT(*)                          AS total_orders,
       ROUND(SUM(payment_total), 2)      AS total_revenue,
       ROUND(AVG(payment_total), 2)      AS avg_order_value,
       MIN(purchase_date)                AS first_order_date,
       MAX(purchase_date)                AS last_order_date
FROM fact_orders;


-- 2. Orders by status, with share of total ------------------------------------
SELECT order_status,
       COUNT(*) AS orders,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_orders
FROM fact_orders
GROUP BY order_status
ORDER BY orders DESC;


-- 3. Revenue by state (top 10) -------------------------------------------------
SELECT customer_state,
       COUNT(*)                       AS total_orders,
       ROUND(SUM(payment_total), 2)   AS total_revenue,
       ROUND(AVG(payment_total), 2)   AS avg_order_value,
       ROUND(MIN(payment_total), 2)   AS min_order,
       ROUND(MAX(payment_total), 2)   AS max_order
FROM fact_orders
GROUP BY customer_state
ORDER BY total_revenue DESC
LIMIT 10;


-- 4. Monthly trend --------------------------------------------------------------
SELECT year, month,
       COUNT(*)                     AS orders,
       ROUND(SUM(payment_total), 2) AS revenue
FROM fact_orders
GROUP BY year, month
ORDER BY year, month;


-- ════════════════════════════════════════════════════════════════════════════
-- WINDOW FUNCTIONS
-- ════════════════════════════════════════════════════════════════════════════

-- 5. Rank states by revenue
--    ROW_NUMBER : unique 1,2,3,4 (ties broken arbitrarily)
--    RANK       : ties share a rank, next rank is skipped   (1,2,2,4)
--    DENSE_RANK : ties share a rank, no gap                 (1,2,2,3)
SELECT customer_state,
       ROUND(SUM(payment_total), 2)                      AS revenue,
       ROW_NUMBER() OVER (ORDER BY SUM(payment_total) DESC) AS row_num,
       RANK()       OVER (ORDER BY SUM(payment_total) DESC) AS rank_pos,
       DENSE_RANK() OVER (ORDER BY SUM(payment_total) DESC) AS dense_rank_pos
FROM fact_orders
GROUP BY customer_state
ORDER BY revenue DESC;


-- 6. Top 3 cities inside every state
--    A window function cannot be filtered in WHERE, so rank in a CTE first.
WITH city_revenue AS (
    SELECT customer_state, customer_city,
           COUNT(*)                     AS orders,
           ROUND(SUM(payment_total), 2) AS revenue
    FROM fact_orders
    GROUP BY customer_state, customer_city
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY customer_state ORDER BY revenue DESC) AS city_rank
    FROM city_revenue
)
SELECT customer_state, customer_city, orders, revenue, city_rank
FROM ranked
WHERE city_rank <= 3
ORDER BY customer_state, city_rank;


-- 7. Running (cumulative) total of orders and revenue ---------------------------
WITH monthly AS (
    SELECT year, month, COUNT(*) AS orders, SUM(payment_total) AS revenue
    FROM fact_orders
    GROUP BY year, month
)
SELECT year, month, orders,
       SUM(orders) OVER (ORDER BY year, month)               AS running_orders,
       ROUND(SUM(revenue) OVER (ORDER BY year, month), 2)    AS running_revenue
FROM monthly
ORDER BY year, month;


-- 8. Month-over-month change with LAG (previous row's value) --------------------
WITH monthly AS (
    SELECT year, month,
           COUNT(*)                     AS orders,
           ROUND(SUM(payment_total), 2) AS revenue
    FROM fact_orders
    GROUP BY year, month
)
SELECT year, month, orders, revenue,
       LAG(orders)  OVER (ORDER BY year, month)                          AS prev_month_orders,
       orders - LAG(orders) OVER (ORDER BY year, month)                  AS order_change,
       ROUND(revenue - LAG(revenue) OVER (ORDER BY year, month), 2)      AS revenue_change,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY year, month))
             / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0), 2)   AS revenue_growth_pct
FROM monthly
ORDER BY year, month;


-- 9. LEAD (NEXT row's value)  -  e.g. "how much will the next month change?" ----
WITH monthly AS (
    SELECT year, month,
           COUNT(*)                     AS orders,
           ROUND(SUM(payment_total), 2) AS revenue
    FROM fact_orders
    GROUP BY year, month
)
SELECT year, month, orders, revenue,
       LEAD(orders) OVER (ORDER BY year, month)                          AS next_month_orders,
       LEAD(orders) OVER (ORDER BY year, month) - orders                 AS next_month_order_change,
       ROUND(LEAD(revenue) OVER (ORDER BY year, month) - revenue, 2)     AS next_month_revenue_change
FROM monthly
ORDER BY year, month;


-- ════════════════════════════════════════════════════════════════════════════
-- CTEs, CASE, SUBQUERIES
-- ════════════════════════════════════════════════════════════════════════════

-- 10. States whose revenue is above the average state
WITH state_revenue AS (
    SELECT customer_state, ROUND(SUM(payment_total), 2) AS revenue
    FROM fact_orders
    GROUP BY customer_state
),
avg_revenue AS (
    SELECT AVG(revenue) AS avg_state_revenue FROM state_revenue
)
SELECT sr.customer_state,
       sr.revenue,
       ROUND(ar.avg_state_revenue, 2)            AS avg_state_revenue,
       ROUND(sr.revenue - ar.avg_state_revenue, 2) AS above_average_by
FROM state_revenue sr
CROSS JOIN avg_revenue ar
WHERE sr.revenue > ar.avg_state_revenue
ORDER BY sr.revenue DESC;


-- 11. Market categories with CASE (if/else inside SQL)
--     Adjust the thresholds to your dataset's scale.
SELECT customer_state,
       ROUND(SUM(payment_total), 2) AS revenue,
       CASE
           WHEN SUM(payment_total) > 1000000 THEN 'High value market'
           WHEN SUM(payment_total) >  500000 THEN 'Medium value market'
           WHEN SUM(payment_total) >  100000 THEN 'Growing market'
           ELSE 'Small market'
       END AS market_category
FROM fact_orders
GROUP BY customer_state
ORDER BY revenue DESC;


-- 12. Orders paying above the average order value (subquery)
SELECT order_id, customer_state, payment_total
FROM fact_orders
WHERE payment_total > (SELECT AVG(payment_total) FROM fact_orders)
ORDER BY payment_total DESC
LIMIT 20;


-- 13. Payment mix: share of orders and money by main payment type ---------------
SELECT COALESCE(payment_type_main, 'no payment record') AS payment_type,
       COUNT(*)                                        AS orders,
       ROUND(SUM(payment_total), 2)                    AS total_value,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_orders,
       ROUND(AVG(payment_installments_max), 1)         AS avg_installments
FROM fact_orders
GROUP BY payment_type_main
ORDER BY total_value DESC;


-- 14. Delivery performance by state ----------------------------------------------
SELECT customer_state,
       COUNT(*)                                       AS delivered_orders,
       ROUND(AVG(delivery_days), 1)                   AS avg_delivery_days,
       ROUND(100 * AVG(is_late), 1)                   AS pct_late
FROM fact_orders
WHERE order_status = 'delivered' AND delivery_days IS NOT NULL
GROUP BY customer_state
ORDER BY pct_late DESC;


-- 15. Orders paid in several parts -------------------------------------------------
SELECT payment_count, COUNT(*) AS orders, ROUND(SUM(payment_total), 2) AS revenue
FROM fact_orders
GROUP BY payment_count
ORDER BY payment_count;


-- ════════════════════════════════════════════════════════════════════════════
-- DATA QUALITY + PIPELINE HEALTH
-- ════════════════════════════════════════════════════════════════════════════

-- 16. Duplicate check  -> MUST return zero rows (order_id is the primary key)
SELECT order_id, COUNT(*) AS cnt
FROM fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1;


-- 17. Orders with no payment record (not an error, but worth watching)
SELECT COUNT(*) AS orders_without_payment
FROM fact_orders
WHERE payment_count = 0;


-- 18. Rows the ETL rejected, by reason (latest run first)
SELECT etl_run_id, reject_reason, COUNT(*) AS rows_rejected
FROM etl_rejected_orders
GROUP BY etl_run_id, reject_reason
ORDER BY MAX(rejected_at) DESC, rows_rejected DESC
LIMIT 20;


-- 19. Last 10 pipeline runs
SELECT started_at, status, failed_stage,
       rows_clean, rows_rejected, rows_inserted, rows_updated, rows_unchanged,
       target_total_rows, duration_seconds, LEFT(message, 80) AS message
FROM etl_run_audit
ORDER BY started_at DESC
LIMIT 10;
