WITH oi_order_items AS (
  SELECT DISTINCT order_id,
    order_item_number,
    product_id,
    seller_id,
    price,
    freight_price    
  FROM {{ ref( 'silver_order_items' ) }}
),
oi_customers AS (
  SELECT DISTINCT order_customer_id, 
    unique_customer_id, 
    customer_city, 
    customer_state
  FROM {{ ref( 'silver_customers' ) }}
),
oi_products AS (
  SELECT DISTINCT product_id, 
    category_name_english
  FROM {{ ref( 'silver_products' ) }}
),
oi_sellers AS (
  SELECT DISTINCT seller_id, 
    seller_city, 
    seller_state
  FROM {{ ref( 'silver_sellers' ) }}   
), 
oi_orders AS (
  SELECT DISTINCT order_id, 
    customer_id, 
    order_status, 
    purchase_ts, 
    delivered_customer_ts 
  FROM {{ ref( 'silver_orders' ) }}
), 
tb1 AS (
  SELECT DISTINCT oi.order_id, 
    oi.order_item_number,
    oi.product_id,
    oi.price,
    oi.freight_price,
    oi.seller_id,
    p.category_name_english AS category_name
  FROM oi_order_items AS oi
  LEFT JOIN oi_products AS p
    ON oi.product_id = p.product_id
), 
tb2 AS (
  SELECT tb1.*, 
    s.seller_city, 
    s.seller_state
  FROM tb1
  LEFT JOIN oi_sellers AS s
  ON tb1.seller_id = s.seller_id
),
tb3 AS (
  SELECT tb2.*, 
    o.order_status, 
    o.customer_id, 
    o.purchase_ts, 
    o.delivered_customer_ts
  FROM tb2
  LEFT JOIN oi_orders AS o 
    ON tb2.order_id = o.order_id   
),
tb_final AS (
  SELECT tb3.*, 
    c.unique_customer_id, 
    c.customer_city, 
    c.customer_state
  FROM tb3
  LEFT JOIN oi_customers AS c
    ON tb3.customer_id = c.order_customer_id                
)
SELECT *
FROM tb_final