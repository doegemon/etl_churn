WITH bronze_products AS (
  SELECT *
  FROM {{ source('dwolist', 'olist_products_dataset')}}
)
SELECT *
FROM bronze_products