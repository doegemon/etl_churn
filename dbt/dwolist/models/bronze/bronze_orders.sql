WITH bronze_orders AS (
  SELECT *
  FROM {{ source('dwolist', 'olist_orders_dataset')}}
)
SELECT *
FROM bronze_orders