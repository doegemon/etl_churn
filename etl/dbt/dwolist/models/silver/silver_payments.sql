WITH silver_pay AS (
  SELECT order_id, 
    payment_sequential,
    payment_type, 
    payment_installments, 
    payment_value
  FROM {{ ref( 'bronze_order_payments' ) }}
)
SELECT CAST(order_id AS VARCHAR) AS order_id,
  CAST(payment_type AS VARCHAR) as method, 
  CAST(payment_sequential AS INTEGER) AS method_sequential,
  CAST(payment_installments AS INTEGER) AS installments, 
  CAST(payment_value AS FLOAT) AS value
FROM silver_pay