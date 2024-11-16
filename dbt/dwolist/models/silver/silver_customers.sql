WITH silver_custom AS (
  SELECT customer_id, 
    customer_unique_id,
    customer_zip_code_prefix, 
    customer_city, 
    customer_state
  FROM {{ ref( 'bronze_customers' ) }}
)
SELECT CAST(customer_id AS VARCHAR) AS order_customer_id, 
  CAST(customer_unique_id AS VARCHAR) AS unique_customer_id, 
  CAST(customer_city AS VARCHAR) AS customer_city, 
  CAST(customer_state AS VARCHAR) AS customer_state,
  CAST(customer_zip_code_prefix AS VARCHAR) AS zip_code_prefix
FROM silver_custom