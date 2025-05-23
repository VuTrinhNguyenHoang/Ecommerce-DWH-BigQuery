{{ config(
    materialized='incremental',
    unique_key=['product_id', 'effective_date'],
    partition_by={
      "field": "effective_date",
      "data_type": "date"
    },
    cluster_by=["product_id"]
) }}

WITH new_records AS (
  SELECT
    CAST(product_id AS STRING) AS product_id,
    CAST(product_name AS STRING) AS product_name,
    CAST(product_description AS STRING) AS product_description,
    CAST(product_inventory_status AS STRING) AS product_inventory_status,
    CAST(load_date AS DATE) AS source_effective_date,
    CAST(NULL AS DATE) AS end_date,
    CAST(TRUE AS BOOLEAN) AS is_active,
    load_date,
    ROW_NUMBER() OVER (PARTITION BY product_id, load_date ORDER BY load_date DESC) AS rn
  FROM
    {{ ref('stg_products') }} s
  {% if is_incremental %}
    WHERE load_date >= (
      SELECT COALESCE(MAX(t.effective_date), '1900-01-01')
      FROM {{ this }} t
      WHERE t.effective_date IS NOT NULL
    )
    AND NOT EXISTS (
      SELECT 1
      FROM {{ this }} t
      WHERE t.effective_date = s.load_date
      AND t.product_id = s.product_id
    )
  {% endif %}
),

filtered_new_records AS (
  SELECT
    product_id,
    product_name,
    product_description,
    product_inventory_status,
    source_effective_date,
    end_date,
    is_active
  FROM new_records
  WHERE rn = 1
),

existing_records AS (
  SELECT
    CAST(product_id AS STRING) AS product_id,
    CAST(product_name AS STRING) AS product_name,
    CAST(product_description AS STRING) AS product_description,
    CAST(product_inventory_status AS STRING) AS product_inventory_status,
    CAST(effective_date AS DATE) AS effective_date,
    CAST(end_date AS DATE) AS end_date,
    CAST(is_active AS BOOLEAN) AS is_active
  FROM
    {{ this }}
  WHERE
    is_active = TRUE
    AND product_id IN (SELECT product_id FROM filtered_new_records)
),

updated_records AS (
  SELECT
    e.product_id,
    e.product_name,
    e.product_description,
    e.product_inventory_status,
    e.effective_date,
    CAST(n.source_effective_date AS DATE) AS end_date,
    FALSE AS is_active
  FROM
    existing_records e
  JOIN
    filtered_new_records n
  ON
    e.product_id = n.product_id
  WHERE
    e.product_name != n.product_name
    OR e.product_description != n.product_description
    OR e.product_inventory_status != n.product_inventory_status
),

combined_records AS (
  SELECT
    product_id,
    product_name,
    product_description,
    product_inventory_status,
    source_effective_date AS effective_date,
    end_date,
    is_active
  FROM filtered_new_records
  UNION ALL
  SELECT
    product_id,
    product_name,
    product_description,
    product_inventory_status,
    effective_date,
    end_date,
    is_active
  FROM updated_records
  UNION ALL
  SELECT
    t.product_id,
    t.product_name,
    t.product_description,
    t.product_inventory_status,
    t.effective_date,
    t.end_date,
    t.is_active
  FROM {{ this }} t
  WHERE
    t.product_id NOT IN (SELECT product_id FROM filtered_new_records)
    OR (t.product_id IN (SELECT product_id FROM filtered_new_records) AND t.is_active = FALSE)
),

final AS (
  SELECT
    product_id,
    product_name,
    product_description,
    product_inventory_status,
    effective_date,
    end_date,
    is_active,
    ROW_NUMBER() OVER (PARTITION BY product_id, effective_date ORDER BY effective_date DESC) AS rn
  FROM combined_records
)

SELECT DISTINCT
  product_id,
  product_name,
  product_description,
  product_inventory_status,
  effective_date,
  end_date,
  is_active
FROM final
WHERE rn = 1