WITH bronze_sellers AS (
  SELECT *
  FROM {{ source('dwolist', 'olist_sellers_dataset')}}
)
SELECT *
FROM bronze_sellers