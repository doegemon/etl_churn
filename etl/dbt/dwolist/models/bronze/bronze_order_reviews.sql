WITH bronze_reviews AS (
  SELECT *
  FROM {{ source('dwolist', 'olist_order_reviews_dataset')}}
)
SELECT *
FROM bronze_reviews