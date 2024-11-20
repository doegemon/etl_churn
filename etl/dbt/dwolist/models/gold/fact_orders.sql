WITH orders AS (
  SELECT DISTINCT order_id, 
    customer_id, 
    order_status, 
    purchase_ts::date,
    delivered_customer_ts::date,
    (delivered_customer_ts::date - purchase_ts::date) AS days_to_deliver
  FROM {{ ref( 'silver_orders' ) }}
), 
order_items AS (
  SELECT DISTINCT order_id, 
    SUM(order_item_number) AS items_qty, 
    SUM(price) AS total_price, 
    SUM(freight_price) AS total_freight_price
  FROM {{ ref( 'silver_order_items' ) }}
  GROUP BY 1
),
customers AS (
  SELECT DISTINCT order_customer_id, 
    unique_customer_id, 
    customer_city, 
    customer_state
  FROM {{ ref( 'silver_customers' ) }}
),
payments AS (
  SELECT DISTINCT order_id, 
    SUM(value) AS total_payment_value, 
    ARRAY_AGG(method) AS payment_methods
  FROM {{ ref( 'silver_payments' ) }}
  GROUP BY 1
),
reviews AS (
  SELECT DISTINCT order_id, 
    COUNT(DISTINCT review_id) AS total_reviews,
    AVG(score) AS avg_score
  FROM {{ ref( 'silver_reviews' ) }}
  GROUP BY 1
), 
tb_1 AS (
  SELECT DISTINCT o.order_id, 
    o.customer_id,
    o.order_status, 
    o.purchase_ts,
    o.delivered_customer_ts,
    CAST(o.days_to_deliver AS INTEGER) AS days_to_deliver,
    oi.total_price, 
    oi.total_freight_price
  FROM orders AS o
  LEFT JOIN order_items AS oi
    ON o.order_id = oi.order_id
),
tb_2 AS (
  SELECT tb_1.*, 
    c.unique_customer_id,
    c.customer_city, 
    c.customer_state
  FROM tb_1
  LEFT JOIN customers AS c
    ON tb_1.customer_id = c.order_customer_id 
), 
tb_3 AS (
  SELECT tb_2.*, 
    p.total_payment_value, 
    p.payment_methods
  FROM tb_2
  LEFT JOIN payments AS p
    ON tb_2.order_id = p.order_id
),
tb_final AS (
  SELECT tb_3.*, 
    r.total_reviews, 
    r.avg_score
  FROM tb_3
  LEFT JOIN reviews AS r
    ON tb_3.order_id = r.order_id
)
SELECT *
FROM tb_final