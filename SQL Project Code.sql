--1. Data cleaning on different tables
--Data cleaning (update product name from Brazillian to English)
UPDATE products
SET product_cat_name = prod_name_eng FROM product_name_trans
WHERE products.product_cat_name = product_name_trans.prod_name


--Data cleaning (categorize null value into category named 'uncategorized')
UPDATE products
SET product_cat_name = 'uncatgorized'
WHERE product_cat_name IS NULL


--Data cleaning (delete orders with status 'unavailable', 'canceled')
DELETE FROM orders
WHERE order_status = 'unavailable' OR order_status = 'canceled'


--Data cleaning (delete orders in 2016 and 2018)
DELETE FROM orders
WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = '2016' OR EXTRACT(YEAR FROM order_purchase_timestamp) = '2018'


--Data cleaning (insert column 'purchase month'(YYYY-MM) for future data exploration and filter use)
ALTER TABLE orders
ADD COLUMN order_purchase_month TYPE DATE

UPDATE orders
SET order_purchase_month = order_purchase_timestamp

ALTER TABLE orders
ALTER COLUMN order_purchase_month TYPE VARCHAR(50)

UPDATE orders
SET order_purchase_month = TO_CHAR(TO_DATE(order_purchase_month, 'YYYY-MM'),'YYYY-MM')


--Data cleaning (standardize format)
UPDATE products
SET product_cat_name = REPLACE(product_cat_name, '_',' ')

UPDATE products
SET product_cat_name = CONCAT(UPPER(LEFT(product_cat_name,1)),SUBSTRING(product_cat_name,2,LENGTH(product_cat_name)))

UPDATE customers
SET customer_city = INITCAP(customer_city)


--2. Customer analysis
--Create a customer order details table by joining different tables
DROP VIEW IF EXISTS customer_order_details
CREATE VIEW customer_order_details AS
SELECT ord.order_id, ord.customer_id, ord.order_purchase_timestamp, ord.order_purchase_month, 
ord_i.product_id, pro.product_cat_name, ord_pay.payment_value, cus.customer_city, cus.customer_state
FROM orders ord
JOIN order_items ord_i
ON ord.order_id = ord_i.order_id
JOIN products pro
ON pro.product_id = ord_i.product_id
JOIN order_payments ord_pay
ON ord.order_id = ord_pay.order_id
JOIN customers cus
ON ord.customer_id = cus.customer_id


--Segment customers into groups based on RFM model
CREATE VIEW customer_segmentation AS
SELECT *,
CASE
	WHEN (R BETWEEN 4 AND 5) AND (((F+M)/2) BETWEEN 4 AND 5) THEN 'Champions'	
	WHEN (R BETWEEN 2 AND 5) AND (((F+M)/2) BETWEEN 3 AND 5) THEN 'Loyal Customers'
	WHEN (R BETWEEN 3 AND 5) AND (((F+M)/2) BETWEEN 1 AND 3) THEN 'Potential Loyalist'
	WHEN (R BETWEEN 4 AND 5) AND (((F+M)/2) BETWEEN 0 AND 1) THEN 'New Customers'
	WHEN (R BETWEEN 3 AND 4) AND (((F+M)/2) BETWEEN 0 AND 1) THEN 'Promising'
	WHEN (R BETWEEN 2 AND 3) AND (((F+M)/2) BETWEEN 2 AND 3) THEN 'Customers Needing Attention'
	WHEN (R BETWEEN 2 AND 3) AND (((F+M)/2) BETWEEN 0 AND 2) THEN 'About to Sleep'
	WHEN (R BETWEEN 0 AND 2) AND (((F+M)/2) BETWEEN 2 AND 5) THEN 'At Risk'
	WHEN (R BETWEEN 0 AND 1) AND (((F+M)/2) BETWEEN 4 AND 5) THEN 'Can''t Lost Them'
	WHEN (R BETWEEN 1 AND 2) AND (((F+M)/2) BETWEEN 1 AND 2) THEN 'Hibernating'
	WHEN (R BETWEEN 0 AND 2) AND (((F+M)/2) BETWEEN 0 AND 2) THEN 'Lost'
END customer_category
FROM
(SELECT customer_id,
MAX(order_purchase_timestamp) recent_order_date,
('2017-12-31'::date - MAX(order_purchase_timestamp)) recency,
COUNT(DISTINCT order_id) frequency,
SUM(payment_value) monetary,
NTILE(5) OVER (ORDER BY '2017-12-31'::date - MAX(order_purchase_timestamp) DESC) R,
NTILE(5) OVER (ORDER BY COUNT(DISTINCT order_id)) F,
NTILE(5) OVER (ORDER BY SUM(payment_value)) M
FROM customer_order_details
GROUP BY customer_id) RFM_Model


--Number and percentage of each customer segment
SELECT DISTINCT customer_category, COUNT(customer_category),
ROUND(CAST(COUNT(customer_category) AS numeric)/(SELECT COUNT(customer_id) FROM customer_segmentation) *100,2) percentage
FROM customer_segmentation
GROUP BY DISTINCT customer_category
ORDER BY 3 DESC


--Total number of orders, customers, cities, states
SELECT COUNT(DISTINCT order_id) as order_total, COUNT(DISTINCT customer_id) customer_total, 
COUNT(DISTINCT customer_city) city_total, COUNT(DISTINCT customer_state) state_total
FROM customer_order_details


--Top 10 city with highest sales volumn and number of customers
SELECT customer_city, customer_state, SUM(payment_value), COUNT(DISTINCT customer_id) 
FROM customer_order_details
GROUP BY customer_city, customer_state
ORDER BY 3 DESC
LIMIT 10


--Sales volume per category per month
SELECT order_purchase_month, product_cat_name, SUM(payment_value) 
FROM customer_order_details
GROUP BY order_purchase_month, product_cat_name
ORDER BY 1,2


--3. Seller analysis
--Create a seller table by joining different tables
DROP VIEW IF EXISTS seller_details
CREATE VIEW seller_details AS
SELECT ord.order_id, ord.order_delivered_customer_date, ord.order_estimated_delivery_date,
ord_i.seller_id, ord_review.review_score
FROM orders ord
JOIN order_items ord_i
ON ord.order_id = ord_i.order_id
JOIN order_review ord_review
ON ord.order_id = ord_review.order_id


--Check if the delivery date hit the target set for each order
SELECT order_id, seller_id, order_estimated_delivery_date, order_delivered_customer_date,
CASE
	WHEN order_estimated_delivery_date >= order_delivered_customer_date THEN 'Hit'
	ELSE 'Fail'
END target
FROM seller_details


--Check total orders, total number of fails of meeting the delivery target and fail percentage by each seller
WITH cte AS
(SELECT order_id, seller_id, order_estimated_delivery_date, order_delivered_customer_date,
CASE
	WHEN order_estimated_delivery_date >= order_delivered_customer_date THEN 'Hit'
	ELSE 'Fail'
END target
FROM seller_details)
SELECT seller_id, COUNT(seller_id) AS total_orders, 
COUNT(CASE WHEN target = 'Fail' THEN 1 END) AS total_fails,
ROUND((CAST(COUNT(CASE WHEN target = 'Fail' THEN 1 END) AS NUMERIC)/ COUNT(seller_id) *100),2) AS fail_percentage
FROM cte 
GROUP BY seller_id
ORDER BY 3 DESC


--Return average score of seller and total number of orders made
SELECT seller_id, ROUND(AVG(review_score),2), COUNT(seller_id) FROM seller_details
GROUP BY seller_id
ORDER BY 3 DESC
