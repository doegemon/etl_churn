WITH silver_prd AS (
  SELECT prd.product_id, 
    prd.product_category_name, 
    tr.product_category_name_english,
    prd.product_name_lenght, 
    prd.product_description_lenght, 
    prd.product_photos_qty, 
    prd.product_weight_g, 
    prd.product_length_cm, 
    prd.product_height_cm,
    prd.product_width_cm
  FROM {{ ref( 'bronze_products' ) }} AS prd
  LEFT JOIN {{ ref( 'bronze_category_translation')}} AS tr
    ON prd.product_category_name = tr.product_category_name
)
SELECT CAST(product_id AS VARCHAR) AS product_id, 
  CAST(product_category_name AS VARCHAR) AS category_name, 
  CAST(product_category_name_english AS VARCHAR) AS category_name_english,
  CAST(product_name_lenght AS INTEGER) AS name_char_qty,
  CAST(product_description_lenght AS INTEGER) AS description_char_qty, 
  CAST(product_photos_qty AS INTEGER) AS photos_qty, 
  CAST(product_weight_g AS FLOAT) AS weight_g, 
  CAST(product_length_cm AS FLOAT) AS length_cm, 
  CAST(product_height_cm AS FLOAT) AS height_cm, 
  CAST(product_width_cm AS FLOAT) AS width_cm
FROM silver_prd