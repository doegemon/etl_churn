WITH bronze_geo AS (
  SELECT *
  FROM {{ source('dwolist', 'olist_geolocation_dataset')}}
)
SELECT *
FROM bronze_geo