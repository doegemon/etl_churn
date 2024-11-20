WITH silver_sell AS (
  SELECT seller_id, 
    seller_zip_code_prefix, 
    seller_city, 
    seller_state
  FROM {{ ref( 'bronze_sellers' ) }}
)
SELECT CAST(seller_id AS VARCHAR) AS seller_id, 
  CAST(seller_city AS VARCHAR) AS seller_city, 
  CAST(seller_state AS VARCHAR) AS seller_state, 
  CAST(seller_zip_code_prefix AS VARCHAR) AS zip_code_prefix
FROM silver_sell