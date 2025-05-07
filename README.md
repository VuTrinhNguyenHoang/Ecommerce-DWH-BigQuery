# Ecommerce-DWH-BigQuery

```
CREATE TABLE tiki_data.dim_products (
    product_id STRING,
    product_name STRING,
    product_description STRING,
    product_inventory_status STRING,
    effective_date DATE,
    end_date DATE,
    is_active BOOLEAN
  ) 
PARTITION BY effective_date;

CREATE TABLE `tiki_data.fact_products` (
  product_id STRING,
  snapshot_date DATE,
  product_all_time_quantity_sold INT64,
  product_stock_item_qty INT64,
  product_stock_item_max_sale_qty INT64,
  product_price FLOAT64,
  product_list_price FLOAT64,
  product_discount FLOAT64,
  product_discount_rate FLOAT64
)
PARTITION BY snapshot_date
CLUSTER BY product_id;

CREATE TABLE tiki_data.dim_comments (
    comment_id STRING,
    product_id STRING,
    sentiment STRING,
    comment_content STRING,
    comment_rating INT64,
    created_at TIMESTAMP,
    purchased_at TIMESTAMP,
    days_to_review INT64,
    rating_valid BOOLEAN,
    effective_date DATE,
    end_date DATE,
    is_active BOOLEAN
  ) PARTITION BY effective_date;

CREATE TABLE `tiki_data.fact_comments` (
  comment_id STRING NOT NULL,
  product_id STRING,
  days_to_review INT64,
  comment_rating INT64,
  comment_date DATE,
  snapshot_date DATE
)
PARTITION BY comment_date
CLUSTER BY product_id;


```
