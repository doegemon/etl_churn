WITH silver_rev AS (
  SELECT review_id, 
    order_id, 
    review_score, 
    review_comment_title,
    review_comment_message, 
    review_creation_date, 
    review_answer_timestamp
  FROM {{ ref( 'bronze_order_reviews' ) }}
)
SELECT CAST(review_id AS VARCHAR) AS review_id,
  CAST(order_id AS VARCHAR) AS order_id, 
  CAST(review_score AS INTEGER) AS score, 
  CAST(review_comment_title AS VARCHAR) AS comment_title, 
  CAST(review_comment_message AS VARCHAR) AS comment, 
  CAST(review_creation_date AS TIMESTAMP) AS survey_creation_ts, 
  CAST(review_answer_timestamp AS TIMESTAMP) AS survey_answer_ts
FROM silver_rev