WITH silver_oi AS (
  SELECT order_id, 
    order_item_id,
    product_id, 
    seller_id, 
    shipping_limit_date,
    price,
    freight_value
  FROM {{ ref( 'bronze_order_items' ) }}
)
SELECT CAST(order_id AS VARCHAR) AS order_id,
  CAST(order_item_id AS INTEGER) AS order_item_number, 
  CAST(product_id AS VARCHAR) AS product_id, 
  CAST(seller_id AS VARCHAR) AS seller_id, 
  CAST(price AS FLOAT) AS price, 
  CAST(freight_value AS FLOAT) AS freight_price, 
  CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts
FROM silver_oi