WITH silver_orders AS (
  SELECT order_id, 
    customer_id, 
    order_status, 
    order_purchase_timestamp, 
    order_approved_at, 
    order_delivered_carrier_date, 
    order_delivered_customer_date, 
    order_estimated_delivery_date
  FROM {{ ref( 'bronze_orders' ) }}
)
SELECT CAST(order_id AS VARCHAR) AS order_id, 
  CAST(customer_id AS VARCHAR) AS customer_id, 
  CAST(order_status AS VARCHAR) AS order_status, 
  CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_ts, 
  CAST(order_approved_at AS TIMESTAMP) AS approved_at_ts, 
  CAST(order_delivered_carrier_date AS TIMESTAMP) AS delivered_carrier_ts, 
  CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_ts, 
  CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts
FROM silver_orders