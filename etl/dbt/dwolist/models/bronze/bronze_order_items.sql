WITH bronze_items AS (
  SELECT *
  FROM {{ source('dwolist', 'olist_order_items_dataset')}}
)
SELECT *
FROM bronze_items