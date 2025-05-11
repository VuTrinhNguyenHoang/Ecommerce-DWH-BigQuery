{{ config(materialized='table') }}

SELECT DISTINCT
  CAST(id AS STRING) AS comment_id, 
  CAST(product_id AS STRING) AS product_id,
  sentiment,
  content AS comment_content,
  COALESCE(rating, 0) AS comment_rating,
  created_at,
  purchased_at,
  days_to_review,
  rating_valid,
  CURRENT_DATE() AS load_date
FROM
  `{{ env_var('DATASET_NAME') }}.comments`
WHERE
  id IS NOT NULL
  AND product_id IS NOT NULL
  AND date = CURRENT_DATE()