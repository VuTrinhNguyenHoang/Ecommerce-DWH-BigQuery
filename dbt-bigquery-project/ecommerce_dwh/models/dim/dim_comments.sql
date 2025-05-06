{{ config(
    materialized='incremental',
    unique_key=['comment_id', 'effective_date'],
    partition_by={
      "field": "effective_date",
      "data_type": "date"
    },
    cluster_by=["product_id"]
) }}

WITH new_records AS (
  SELECT
    CAST(comment_id AS STRING) AS comment_id,
    CAST(product_id AS STRING) AS product_id,
    CAST(comment_content AS STRING) AS comment_content,
    CAST(comment_rating AS INT64) AS comment_rating,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(purchased_at AS TIMESTAMP) AS purchased_at,
    CAST(days_to_review AS INT64) AS days_to_review,
    CAST(rating_valid AS BOOLEAN) AS rating_valid,
    CAST(load_date AS DATE) AS effective_date,
    CAST(NULL AS DATE) AS end_date,
    CAST(TRUE AS BOOLEAN) AS is_active
  FROM
    {{ ref('stg_comments') }}
  {% if is_incremental %}
    WHERE load_date > (
      SELECT COALESCE(MAX(effective_date), '1900-01-01')
      FROM {{ this }}
    )
  {% endif %}
),

existing_records AS (
  SELECT
    CAST(comment_id AS STRING) AS comment_id,
    CAST(product_id AS STRING) AS product_id,
    CAST(comment_content AS STRING) AS comment_content,
    CAST(comment_rating AS INT64) AS comment_rating,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(purchased_at AS TIMESTAMP) AS purchased_at,
    CAST(days_to_review AS INT64) AS days_to_review,
    CAST(rating_valid AS BOOLEAN) AS rating_valid,
    CAST(effective_date AS DATE) AS effective_date,
    CAST(end_date AS DATE) AS end_date,
    CAST(is_active AS BOOLEAN) AS is_active
  FROM
    {{ this }}
  WHERE
    is_active = TRUE
    AND comment_id IN (SELECT comment_id FROM new_records)
),

updated_records AS (
  SELECT
    e.comment_id,
    e.product_id,
    e.comment_content,
    e.comment_rating,
    e.created_at,
    e.purchased_at,
    e.days_to_review,
    e.rating_valid,
    e.effective_date,
    CAST(n.effective_date AS DATE) AS end_date,
    FALSE AS is_active
  FROM
    existing_records e
  JOIN
    new_records n
  ON
    e.comment_id = n.comment_id
  WHERE
    e.comment_content != n.comment_content
    OR e.comment_rating != n.comment_rating
    OR e.days_to_review != n.days_to_review
    OR e.rating_valid != n.rating_valid
),

final AS (
  SELECT
    comment_id,
    product_id,
    comment_content,
    comment_rating,
    created_at,
    purchased_at,
    days_to_review,
    rating_valid,
    effective_date,
    end_date,
    is_active
  FROM new_records
  UNION ALL
  SELECT
    comment_id,
    product_id,
    comment_content,
    comment_rating,
    created_at,
    purchased_at,
    days_to_review,
    rating_valid,
    effective_date,
    end_date,
    is_active
  FROM updated_records
  UNION ALL
  SELECT
    comment_id,
    product_id,
    comment_content,
    comment_rating,
    created_at,
    purchased_at,
    days_to_review,
    rating_valid,
    effective_date,
    end_date,
    is_active
  FROM {{ this }}
  WHERE
    comment_id NOT IN (SELECT comment_id FROM new_records)
    OR (comment_id IN (SELECT comment_id FROM new_records) AND is_active = FALSE)
)

SELECT
  comment_id,
  product_id,
  comment_content,
  comment_rating,
  created_at,
  purchased_at,
  days_to_review,
  rating_valid,
  effective_date,
  end_date,
  is_active
FROM
  final