WITH bronze_customers AS (
  SELECT *
  FROM {{ source('dwolist', 'olist_customers_dataset')}}
)
SELECT *
FROM bronze_customers