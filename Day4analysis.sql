
-- Basic Queries Analysis

--  View table
Describe orders;

show create table orders;

-- Total Orders
Select count(*) as Total_orders from orders;

-- Order by status
select Order_status,count(*) as count
from orders group by order_status 
order by count desc;

-- Aggregations 

-- Revenue by state
select customer_state,  
       count(*) as Total_orders,
       Round(sum(payment_value),2) as total_revenue,
       Round(avg(payment_value),2) as avg_order_value,
       Round(min(payment_value),2) as min_order,
       Round(max(payment_value),2) as max_order
from orders
where payment_value is not null
group by customer_state 
order by total_revenue desc
limit 10;

-- Monthly Trend
select 
      year,
      month,
      count(*) as orders,
      Round(sum(payment_value),2) as revenue
from orders 
group by year,month 
order by year,month;


-- WINDOW FUNCTIONS

-- Rank states by revenue
-- RANK gives same rank to ties, skips next number
-- DENSE_RANK gives same rank to ties, does not skip
select customer_state,
       Round(sum(payment_value),2) as revenue,
       ROW_NUMBER() over (order by sum(payment_value)desc) as row_rank_position,
       Rank() over (order by sum(payment_value)desc) as rank_position,
       Dense_Rank() over (order by sum(payment_value)desc) as dense_rank_position
from orders 
where payment_value is not null
group by customer_state;

-- Added a category to partition by
SELECT 
    customer_city, 
    customer_state, 
    ROUND(SUM(payment_value), 2) AS revenue, 
    
    ROW_NUMBER() OVER (
        PARTITION BY customer_city 
        ORDER BY SUM(payment_value) DESC
    ) AS row_rank_position, 
    
    RANK() OVER (
        PARTITION BY customer_city 
        ORDER BY SUM(payment_value) DESC
    ) AS rank_position, 
    
    DENSE_RANK() OVER (
        PARTITION BY customer_city 
        ORDER BY SUM(payment_value) DESC
    ) AS dense_rank_position 
FROM orders 
WHERE payment_value IS NOT NULL 
GROUP BY customer_city, customer_state;
-- -----------------------------------

SELECT 
    customer_state,
    customer_city, 
    ROUND(SUM(payment_value), 2) AS revenue, 
    
    -- Restarts ranking at 1 for the top city in every state
    ROW_NUMBER() OVER (
        PARTITION BY customer_state 
        ORDER BY SUM(payment_value) DESC
    ) AS city_row_num, 
    
    RANK() OVER (
        PARTITION BY customer_state 
        ORDER BY SUM(payment_value) DESC
    ) AS city_rank, 
    
    DENSE_RANK() OVER (
        PARTITION BY customer_state 
        ORDER BY SUM(payment_value) DESC
    ) AS city_dense_rank 
FROM orders 
WHERE payment_value IS NOT NULL 
GROUP BY customer_state, customer_city;

-- Running total of orders over time
-- SUM OVER with ORDER BY = cumulative/running total

select 
      year,
      month,
      count(*) as monthly_orders,
      sum(count(*)) over (order by year,month) as running_total
 from orders 
 group by year,month 
 order by year,month;

-- CTE (Common Table Expressions)
-- Month over month growth using LAG
-- LAG(column) gets the value from the previous row
-- LEAD(column) gets the value from the next row
with monthly as(
     select year,
            month,
            count(*) as orders,
            round(sum(payment_value),2) as revenue
     from orders
     group by year,month
)
select 
      year,
      month,
      orders,
      revenue,
      LAG(orders) over (order by year,month) as prev_month_orders,
      orders - LAG(orders) over (order by year,month) as order_change,
      revenue - LAG(revenue) over (order by year,month) as revenue_change
from monthly 
order by year,month;

-- Lead
with monthly as(
     select year,
            month,
            count(*) as orders,
            round(sum(payment_value),2) as revenue
     from orders
     group by year,month
)
select 
      year,
      month,
      orders,
      revenue,
      LEAD(orders) over (order by year,month) as prev_month_orders,
      orders - LEAD(orders) over (order by year,month) as order_change,
      revenue - LEAD(revenue) over (order by year,month) as revenue_change
from monthly 
order by year,month;

-- CTEs (Common Table Expressions)
-- WITH keyword creates a named temporary result
-- Makes complex queries readable
-- States above average revenue

with state_revenue as (
     select 
           customer_state,
           Round(sum(payment_value),2) as revenue
     from orders 
     where payment_value is not null 
     group by customer_state
),
avg_revenue as (
select avg(revenue) as avg_rev from state_revenue
)
select
      sr.customer_state,
      sr.revenue,
      ar.avg_rev as national_revenue,
      round(sr.revenue - ar.avg_rev,2) as above_average_by
from state_revenue sr, avg_revenue ar
where sr.revenue > ar.avg_rev
order by sr.revenue desc;

-- CASE WHEN
-- Like if/else inside SQL

select
      customer_state,
      round(sum(payment_value),2) as revenue,
      case 
      	  when sum(payment_value)>1000000 then 'High Value Market'
      	  when sum(payment_value)>500000 then 'Medium value market'
      	  when sum(payment_value)>100000 then 'Growing Market'
      	  else 'Small Market'
      end as market_category
  from orders
  where payment_value is not null 
  group by customer_state
  order by revenue desc;
      
  -- FIND DUPLICATES
  
select 
       order_id,
       count(*) as count
from orders 
group by order_id 
having count(*)>1;

-- SUBQUERY
-- A query inside another query
-- Orders with payment above average

select
      order_id,
      customer_state,
      payment_value
 from orders 
 where payment_value > (
       select avg(payment_value) from orders where payment_value is not null 
       )
       order by payment_value desc limit 20;

-- PERCENTAGE OF TOTAL (Very common in business analytics)
select 
      payment_type,
      count(*) as transactions,
      round(sum(payment_value),2) as total_value,
      round(
        count(*) * 100/sum(count(*)) over(),2
        ) as pct_of_transactions
from orders
where payment_type is not null 
group by payment_type 
order by total_value desc;
        
