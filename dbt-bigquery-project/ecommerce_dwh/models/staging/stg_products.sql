{{ config(materialized='table') }}

SELECT DISTINCT
  CAST(id AS STRING) AS product_id,
  name AS product_name,
  COALESCE(short_description, '') AS product_description,
  COALESCE(price, 0) AS product_price,
  COALESCE(list_price, 0) AS product_list_price,
  COALESCE(discount, 0) AS product_discount,
  COALESCE(discount_rate, 0)  AS product_discount_rate,
  COALESCE(all_time_quantity_sold, 0) AS product_all_time_quantity_sold,
  COALESCE(inventory_status, 'unknown') AS product_inventory_status,
  COALESCE(stock_item_qty, 0) AS product_stock_item_qty,
  COALESCE(stock_item_max_sale_qty, 0) AS product_stock_item_max_sale_qty,
  CURRENT_DATE() AS load_date
FROM
  `{{ env_var('DATASET_NAME') }}.products`
WHERE
  id IS NOT NULL
  AND date = CURRENT_DATE()
