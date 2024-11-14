WITH bronze_payments AS (
  SELECT *
  FROM {{ source('dwolist', 'olist_order_payments_dataset')}}
)
SELECT *
FROM bronze_payments