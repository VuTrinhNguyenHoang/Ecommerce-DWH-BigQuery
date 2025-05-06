{{ config(
    materialized='incremental',
    unique_key=['product_id', 'snapshot_date'],
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=["product_id"]
) }}

SELECT
  product_id,
  CURRENT_DATE() AS snapshot_date,
  product_all_time_quantity_sold,
  product_stock_item_qty,
  product_stock_item_max_sale_qty,
  product_price,
  product_list_price,
  product_discount,
  product_discount_rate
FROM {{ ref('stg_products') }}
{% if is_incremental %}
WHERE load_date > (SELECT COALESCE(MAX(snapshot_date), DATE '1900-12-31') FROM {{ this }})
{% endif %}
