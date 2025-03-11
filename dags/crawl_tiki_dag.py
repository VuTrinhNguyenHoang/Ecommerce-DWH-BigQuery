import io
import os
import time
import random
import pandas as pd
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='crawl_comment_inmemory_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def get_categories(**kwargs):
    """Read category IDs from a CSV file and return a DataFrame."""
    try:
        categories_df = pd.read_csv('/opt/airflow/dags/categories/categories_id.csv')
        if categories_df.empty:
            logger.warning("Categories file is empty!")
            return pd.DataFrame()
        logger.info(f"Loaded {len(categories_df)} category IDs.")
        return categories_df
    except FileNotFoundError:
        logger.error("Categories file not found!")
        raise
    except Exception as e:
        logger.error(f"Error reading categories file: {e}")
        raise

get_categories_task = PythonOperator(
    task_id='get_categories',
    python_callable=get_categories,
    provide_context=True,
    dag=dag
)

def crawl_product_by_category(**kwargs):
    """Crawl product IDs by category from Tiki API."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi,en;q=0.9,en-US;q=0.8',
        'Referer': 'https://tiki.vn/dien-thoai-may-tinh-bang/c1789',
        'x-guest-token': 'f1kXJZDbVQ8N0PFzH3E92yKUeg4Wa6wY',
        'Connection': 'keep-alive',
    }

    params = {
        'limit': '40',
        'include': 'advertisement',
        'aggregations': '2',
        'trackity_id': '563cc097-0278-3210-d9eb-2b08ef7b99a0',
        'category': '',
        'page': '1',
    }

    ti = kwargs['ti']
    categories_df = ti.xcom_pull(task_ids='get_categories')
    if categories_df is None or categories_df.empty:
        logger.error("No category IDs available!")
        return []

    product_id_list = []
    try:
        for cat in categories_df['category_id']:
            params['category'] = str(cat)
            for i in range(1, 2):  # Crawl only page 1
                params['page'] = str(i)
                response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings', headers=headers, params=params)
                logger.info(f"Fetching category {cat}, page {i}. Status: {response.status_code}")
                if response.status_code == 200:
                    data = response.json().get('data', [])
                    for record in data:
                        pid = record.get('id')
                        if pid:
                            product_id_list.append({'id': pid})
                else:
                    logger.warning(f"Failed to fetch category {cat}, page {i}. Status: {response.status_code}")
                time.sleep(random.uniform(1, 3))  # Polite delay
            break  # Only crawl first category
        logger.info(f"Crawled {len(product_id_list)} product IDs.")
        return product_id_list
    except Exception as e:
        logger.error(f"Error in crawl_product_by_category: {e}")
        return []

crawl_product_by_category_task = PythonOperator(
    task_id='crawl_product_by_category',
    python_callable=crawl_product_by_category,
    provide_context=True,
    dag=dag
)

def crawl_comment_product(**kwargs):
    """Crawl product comments from Tiki API."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi,en;q=0.9,en-US;q=0.8',
        'Referer': 'https://tiki.vn/ke-toan-via-he-thuc-hanh-bao-cao-tai-chinh-can-ban-tu-quay-ban-nuoc-chanh-p262977989.html?',
        'x-guest-token': 'pUYHub5GIMswKcLtmv6gzl9yDFo8Cd7E',
        'Connection': 'keep-alive',
        'TE': 'Trailers',
    }

    params = {
        'sort': 'score|desc,id|desc,stars|all',
        'page': '1',
        'limit': '10',
        'include': 'comments'
    }

    ti = kwargs['ti']
    product_ids = ti.xcom_pull(task_ids='crawl_product_by_category')
    if not product_ids:
        logger.error("No product IDs available!")
        return []

    p_ids = [item['id'] for item in product_ids][:10]  # Limit to 10 products
    result = []

    def comment_parser(json_item):
        try:
            created_by = json_item.get('created_by', {})
            return {
                'id': json_item.get('id'),
                'title': json_item.get('title'),
                'content': json_item.get('content'),
                'thank_count': json_item.get('thank_count'),
                'customer_id': json_item.get('customer_id'),
                'rating': json_item.get('rating'),
                'created_at': json_item.get('created_at'),
                'customer_name': created_by.get('full_name'),
                'purchased_at': created_by.get('purchased_at')
            }
        except Exception as e:
            logger.warning(f"Error parsing comment: {e}")
            return None

    try:
        for pid in p_ids:
            params['product_id'] = pid
            logger.info(f"Crawling comments for product {pid}")
            for i in range(1, 2):  # Only page 1
                params['page'] = str(i)
                try:
                    response = requests.get('https://tiki.vn/api/v2/reviews', headers=headers, params=params)
                    if response.status_code == 200:
                        data_comments = response.json().get('data', [])
                        for comment in data_comments:
                            parsed = comment_parser(comment)
                            if parsed:
                                result.append(parsed)
                    else:
                        logger.warning(f"Failed to fetch comments for product {pid}, page {i}. Status: {response.status_code}")
                    time.sleep(random.uniform(1, 3))
                except Exception as e:
                    logger.error(f"Error fetching comments for product {pid}, page {i}: {e}")
    except Exception as e:
        logger.error(f"Error in crawl_comment_product: {e}")

    logger.info(f"Crawled {len(result)} comments.")
    return result

crawl_comment_task = PythonOperator(
    task_id='crawl_comment_product',
    python_callable=crawl_comment_product,
    provide_context=True,
    dag=dag
)

def crawl_infor_product(**kwargs):
    """Crawl product information from Tiki API."""
    ti = kwargs['ti']
    product_ids = ti.xcom_pull(task_ids='crawl_product_by_category')
    if not product_ids:
        logger.error("No product IDs available!")
        return []

    p_ids = [item['id'] for item in product_ids][:10]  # Limit to 10 products
    result = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
        'Referer': 'https://tiki.vn/dien-thoai-samsung-galaxy-m31-128gb-6gb-hang-chinh-hang-p58259141.html?src=category-page-1789&2hi=0',
        'x-guest-token': '8jWSuIDBb2NGVzr6hsUZXpkP1FRin7lY',
        'Connection': 'keep-alive',
        'TE': 'Trailers',
    }

    params = {'platform': 'web', 'spid': '187266106'}

    def parser_product(json_item):
        try:
            stock_item = json_item.get('stock_item', {})
            brand = json_item.get('brand', {})
            return {
                'id': json_item.get('id'),
                'sku': json_item.get('sku'),
                'short_description': json_item.get('short_description'),
                'price': json_item.get('price'),
                'list_price': json_item.get('list_price'),
                'discount': json_item.get('discount'),
                'discount_rate': json_item.get('discount_rate'),
                'review_count': json_item.get('review_count'),
                'all_time_quantity_sold': json_item.get('all_time_quantity_sold'),
                'inventory_status': json_item.get('inventory_status'),
                'stock_item_qty': stock_item.get('qty'),
                'stock_item_max_sale_qty': stock_item.get('max_sale_qty'),
                'product_name': json_item.get('name'),
                'brand_id': brand.get('id'),
                'brand_name': brand.get('name')
            }
        except Exception as e:
            logger.warning(f"Error parsing product: {e}")
            return None

    try:
        for pid in p_ids:
            try:
                url = f'https://tiki.vn/api/v2/products/{pid}'
                response = requests.get(url, headers=headers, params=params)
                logger.info(f"Fetching product {pid}. Status: {response.status_code}")
                if response.status_code == 200:
                    product_info = parser_product(response.json())
                    if product_info:
                        result.append(product_info)
                else:
                    logger.warning(f"Failed to fetch product {pid}. Status: {response.status_code}")
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                logger.error(f"Error fetching product {pid}: {e}")
    except Exception as e:
        logger.error(f"Error in crawl_infor_product: {e}")

    logger.info(f"Crawled {len(result)} products.")
    return result

crawl_infor_product_task = PythonOperator(
    task_id='crawl_infor_product',
    python_callable=crawl_infor_product,
    provide_context=True,
    dag=dag
)

def load_to_hdfs(**kwargs):
    """Load crawled comments and product data to HDFS."""
    ti = kwargs['ti']
    comment_data = ti.xcom_pull(task_ids='crawl_comment_product')
    product_data = ti.xcom_pull(task_ids='crawl_infor_product')

    hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
    client = hook.get_conn()

    execution_date = kwargs.get('execution_date', datetime.now())
    date_str = execution_date.strftime("%Y%m%d_%H%M%S")

    def save_to_hdfs(data, name, remote_path):
        if not data:
            logger.warning(f"No {name} data to save!")
            return
        try:
            df = pd.DataFrame(data)
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            parquet_bytes = buffer.read()
            client.write(hdfs_path=remote_path, data=parquet_bytes, overwrite=True)
            logger.info(f"Uploaded {len(df)} {name} records to {remote_path}")
        except Exception as e:
            logger.error(f"Error uploading {name} to HDFS: {e}")
            raise

    # Save comments
    if comment_data:
        remote_comment_path = f"/user/airflow/comments/comment_inmemory_{date_str}.parquet"
        save_to_hdfs(comment_data, "comment", remote_comment_path)

    # Save products
    if product_data:
        remote_product_path = f"/user/airflow/details/product_inmemory_{date_str}.parquet"
        save_to_hdfs(product_data, "product", remote_product_path)

load_hdfs_task = PythonOperator(
    task_id='load_to_hdfs',
    python_callable=load_to_hdfs,
    provide_context=True,
    dag=dag
)

# Define task dependencies
get_categories_task >> crawl_product_by_category_task >> [crawl_comment_task, crawl_infor_product_task] >> load_hdfs_task