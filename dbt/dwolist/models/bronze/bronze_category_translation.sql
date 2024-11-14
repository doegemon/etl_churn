WITH bronze_translation AS (
  SELECT *
  FROM {{ source('dwolist', 'product_category_name_translation')}}
)
SELECT *
FROM bronze_translation