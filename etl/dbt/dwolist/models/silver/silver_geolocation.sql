WITH silver_geo AS (
  SELECT geolocation_zip_code_prefix, 
    geolocation_lat,
    geolocation_lng, 
    geolocation_city, 
    geolocation_state
  FROM {{ ref( 'bronze_geolocation' ) }}
)
SELECT CAST(geolocation_city AS VARCHAR) AS city, 
  CAST(geolocation_state AS VARCHAR) AS state, 
  CAST(geolocation_zip_code_prefix AS VARCHAR) AS zip_code_prefix,
  CAST(geolocation_lat AS FLOAT) AS latitude, 
  CAST(geolocation_lng AS FLOAT) AS longitude
FROM silver_geo