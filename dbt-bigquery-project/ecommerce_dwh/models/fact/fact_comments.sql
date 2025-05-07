{{ config(
    materialized='incremental',
    unique_key=['comment_id', 'snapshot_date'],
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=["product_id"]
) }}

SELECT DISTINCT
  c.comment_id,
  c.product_id,
  c.comment_rating,
  c.days_to_review,
  DATE(c.created_at) AS comment_date,
  CURRENT_DATE() AS snapshot_date
FROM {{ ref('stg_comments') }} c
JOIN {{ ref('dim_products') }} p
  ON c.product_id = p.product_id
  AND p.is_active = TRUE AND p.end_date IS NULL
  -- AND DATE(c.comment_date) BETWEEN p.effective_date AND COALESCE(p.end_date, DATE '9999-12-31')
{% if is_incremental %}
WHERE c.load_date > (SELECT COALESCE(MAX(snapshot_date), DATE '1900-12-31') FROM {{ this }})
{% endif %}
