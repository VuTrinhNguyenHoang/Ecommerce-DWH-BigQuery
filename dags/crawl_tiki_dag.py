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
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import time, random
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
    'x-guest-token': 'acuqSeZsnKCWVgofFGjxJi7Ot4N8dPpX',
    'Connection': 'keep-alive'
}

def crawl_and_save_categories():
    try:
        response = requests.get("https://api.tiki.vn/raiden/v2/menu-config", headers=HEADERS)
        categories_id = []
        
        if response.status_code == 200:
            try:
                menu = response.json().get('menu_block').get('items')
                categories_id = [category['link'].split('/c')[-1] for category in menu]
            except Exception as parse_error:
                logging.error("Lỗi khi parse JSON từ API: %s", parse_error, exc_info=True)
        else:
            logging.error("Yêu cầu API không thành công với mã trạng thái: %s", response.status_code)
        
        # Tạm dừng một khoảng thời gian ngẫu nhiên từ 0.5 đến 2 giây để giảm tải và tránh bị block
        time.sleep(random.uniform(0.5, 2))
        
        # Xác định thư mục đầu ra và đường dẫn file CSV
        output_dir = '/opt/airflow/dags/categories'
        output_file = os.path.join(output_dir, 'categories_id.csv')
        
        # Tạo thư mục đầu ra nếu nó chưa tồn tại
        os.makedirs(output_dir, exist_ok=True)
        
        # Lưu Category ID thành file CSV
        df_categories = pd.DataFrame({'category_id': categories_id})
        df_categories.to_csv(output_file, index=False)

        logging.info("Đã lưu %s category_id vào file: %s", len(categories_id), output_file)
        
    except Exception as e:
        logging.error("Đã xảy ra lỗi trong khi thực hiện crawl Categories ID: %s", e, exc_info=True)
        raise

def crawl_and_save_productId(**context):
    try:
        categories_df = pd.read_csv('/opt/airflow/dags/categories/categories_id.csv')
        categories_id = categories_df.category_id.values
    except Exception as e:
        logging.error("Lỗi khi đọc file CSV categories_id: %s", e, exc_info=True)
        raise

    products_id = []
    for category_id in categories_id:
        try:
            response = requests.get(f'https://tiki.vn/api/personalish/v1/blocks/listings?category={category_id}', headers=HEADERS)
            
            if response.status_code == 200:
                try:
                    data = response.json().get('data', [])
                    for product in data:
                        product_id = product.get('id')
                        if product_id is not None:
                            products_id.append(product_id)
                        else:
                            logging.warning("Không tìm thấy 'id' cho sản phẩm trong category: %s", category_id)
                except Exception as parse_error:
                    logging.error("Lỗi khi phân tích dữ liệu JSON cho category %s: %s", category_id, parse_error, exc_info=True)
            else:
                logging.error("Yêu cầu API không thành công cho category %s với mã trạng thái: %s", category_id, response.status_code)
        except Exception as request_error:
            logging.error("Lỗi khi gửi yêu cầu cho category %s: %s", category_id, request_error, exc_info=True)
        
        # Tạm dừng ngẫu nhiên từ 0.5 đến 2 giây để tránh quá tải server và giảm nguy cơ bị block
        time.sleep(random.uniform(0.5, 2))
    
    try:
        # Xác định thư mục và đường dẫn file CSV để lưu product_id
        output_dir = '/opt/airflow/dags/products'
        output_file = os.path.join(output_dir, 'products_id.csv')
        
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(output_dir, exist_ok=True)
        
        # Lưu Product ID thành file CSV
        df_products = pd.DataFrame({'product_id': products_id})
        df_products.to_csv(output_file, index=False)

        logging.info("Đã lưu %s product_id vào file: %s", len(products_id), output_file)
    except Exception as e:
        logging.error("Lỗi khi lưu file CSV chứa product_id: %s", e, exc_info=True)
        raise

    return products_id

def comment_parser(json_data):
    dic = dict()
    dic['id'] = json_data.get('id', None)
    dic['product_id'] = json_data.get('product_id', None) 
    dic['title'] = json_data.get('title', None)
    dic['content'] = json_data.get('content', None)
    dic['rating'] = json_data.get('rating', None)
    dic['created_at'] = json_data.get('created_at', None)
    dic['purchased_at'] = json_data.get('created_by', {}).get('purchased_at', None)
    return dic

def crawl_and_save_comment(**context):
    ti = context['ti']
    products_id = ti.xcom_pull(task_ids='crawl_product_id')
    if not products_id:
        logging.error("Không có product_id từ XCom")
        raise ValueError("Không có product_id từ XCom")

    try:
        params = {
            'limit': 5,                                # Số lượng comment mỗi request (max=20)
            'page': 1,                                 # Trang hiện tại (có thể tùy chỉnh nếu cần duyệt nhiều trang)
            'include': 'comments',                     # Yêu cầu bao gồm thông tin comment
            'sort': 'score|desc,id|desc,stars|all'     # Thứ tự sắp xếp comment
        }

        comments = []
        for product_id in products_id[:1]:
            params['product_id'] = product_id
            
            try:
                response = requests.get('https://tiki.vn/api/v2/reviews', headers=HEADERS, params=params)
                
                if response.status_code == 200:
                    try:
                        data = response.json().get('data', [])
                        for comment in data:
                            comments.append(comment_parser(comment))
                    except Exception as parse_error:
                        logging.error("Lỗi khi parse JSON cho product %s: %s", product_id, parse_error, exc_info=True)
                else:
                    logging.error("Yêu cầu API cho product %s không thành công với mã trạng thái: %s", product_id, response.status_code)
            except Exception as request_error:
                logging.error("Lỗi khi gửi request cho product %s: %s", product_id, request_error, exc_info=True)
            
            # Tạm dừng ngẫu nhiên từ 0.5 đến 2 giây để tránh quá tải server và giảm nguy cơ bị block
            time.sleep(random.uniform(0.5, 2))
        
        logging.info("Tổng số comment thu thập được: %s", len(comments))

        try:
            # Xác định thư mục và đường dẫn file để lưu dữ liệu comment
            output_dir = '/opt/airflow/dags/comments'
            output_file = os.path.join(output_dir, 'comments.csv')
            
            # Tạo thư mục nếu chưa tồn tại
            os.makedirs(output_dir, exist_ok=True)
            
            # Lưu Comments thành file CSV
            df_comments = pd.DataFrame(comments)
            df_comments.to_csv(output_file, index=False)
            
            logging.info("Đã lưu %s comment vào file: %s", len(comments), output_file)
        except Exception as save_error:
            logging.error("Lỗi khi lưu file CSV chứa comment: %s", save_error, exc_info=True)
            raise
        
    except Exception as e:
        logging.error("Lỗi trong hàm crawl_and_save_comment: %s", e, exc_info=True)
        raise

def parser_product(json_data):
    dic = dict()
    dic['id'] = json_data.get('id', None)
    dic['name'] = json_data.get('name', None)
    dic['short_description'] = json_data.get('short_description', None)
    dic['price'] = json_data.get('price', None)
    dic['list_price'] = json_data.get('list_price', None)
    dic['discount'] = json_data.get('discount', None)
    dic['discount_rate'] = json_data.get('discount_rate', None)
    dic['all_time_quantity_sold'] = json_data.get('all_time_quantity_sold', None)
    dic['inventory_status'] = json_data.get('inventory_status', None)
    dic['stock_item_qty'] = json_data.get('stock_item', {}).get('qty', None)
    dic['stock_item_max_sale_qty'] = json_data.get('stock_item', {}).get('max_sale_qty', None)
    return dic

def crawl_and_save_product_detail(**context):
    ti = context['ti']
    products_id = ti.xcom_pull(task_ids='crawl_product_id')
    if not products_id:
        logging.error("Không có product_id từ XCom")
        raise ValueError("Không có product_id từ XCom")

    try:
        product_details = []
        for product_id in products_id[:1]:
            try:
                response = requests.get(f'https://tiki.vn/api/v2/products/{product_id}', headers=HEADERS)
                
                if response.status_code == 200:
                    try:
                        product_details.append(parser_product(response.json()))
                    except Exception as parse_error:
                        logging.error("Lỗi khi parse JSON cho product %s: %s", product_id, parse_error, exc_info=True)
                else:
                    logging.error("Yêu cầu API không thành công cho product %s với mã trạng thái: %s", product_id, response.status_code)
            except Exception as request_error:
                logging.error("Lỗi khi gửi request cho product %s: %s", product_id, request_error, exc_info=True)
            
            # Tạm dừng ngẫu nhiên từ 0.5 đến 2 giây để giảm tải server và tránh bị block
            time.sleep(random.uniform(0.5, 2))
        
        try:
            # Xác định thư mục và đường dẫn file để lưu dữ liệu sản phẩm
            output_dir = '/opt/airflow/dags/products'
            output_file = os.path.join(output_dir, 'product_details.csv')
            
            # Tạo thư mục nếu chưa tồn tại
            os.makedirs(output_dir, exist_ok=True)
            
            # Lưu DataFrame thành file CSV, không lưu cột index
            df_details = pd.DataFrame(product_details)
            df_details.to_csv(output_file, index=False)

            logging.info("Đã lưu %s product detail vào file: %s", len(product_details), output_file)
        except Exception as save_error:
            logging.error("Lỗi khi lưu file CSV chứa product details: %s", save_error, exc_info=True)
            raise
        
    except Exception as e:
        logging.error("Lỗi trong hàm crawl_and_save_product_detail: %s", e, exc_info=True)
        raise

def load_data_to_hdfs(**context):
    try:
        comments_csv = '/opt/airflow/dags/comments/comments.csv'
        product_csv = '/opt/airflow/dags/products/product_details.csv'
        comments_parquet = '/tmp/comments.parquet'
        product_parquet = '/tmp/product_details.parquet'
        
        df_comments = pd.read_csv(comments_csv)
        df_products = pd.read_csv(product_csv)
        
        df_comments.to_parquet(comments_parquet, index=False)
        df_products.to_parquet(product_parquet, index=False)
        
        from hdfs import InsecureClient
        hdfs_url = 'http://namenode:9870'
        client = InsecureClient(hdfs_url, user='hdfs')

        execution_date = context.get('execution_date', datetime.now())
        date_str = execution_date.strftime("%Y%m%d_%H%M%S")

        hdfs_comments_path = f"/user/airflow/comments/comment_inmemory_{date_str}.parquet"
        hdfs_products_path = f"/user/airflow/details/product_inmemory_{date_str}.parquet"
        
        client.upload(hdfs_comments_path, comments_parquet, overwrite=True)
        client.upload(hdfs_products_path, product_parquet, overwrite=True)
        
        logging.info("Đã tải file Parquet lên HDFS thành công: %s và %s", hdfs_comments_path, hdfs_products_path)
        
        os.remove(comments_parquet)
        os.remove(product_parquet)
    except Exception as e:
        logging.error("Lỗi khi lưu file Parquet lên HDFS: %s", e, exc_info=True)
        raise

with DAG(
    dag_id='crawl_tiki_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    crawl_categories = PythonOperator(
        task_id='crawl_categories',
        python_callable=crawl_and_save_categories,
        provide_context=True
    )

    crawl_product_id = PythonOperator(
        task_id='crawl_product_id',
        python_callable=crawl_and_save_productId,
        provide_context=True
    )

    crawl_comment = PythonOperator(
        task_id='crawl_comment',
        python_callable=crawl_and_save_comment,
        provide_context=True
    )

    crawl_product_detail = PythonOperator(
        task_id='crawl_product_detail',
        python_callable=crawl_and_save_product_detail,
        provide_context=True
    )

    load_to_hdfs = PythonOperator(
        task_id='load_data_to_hdfs',
        python_callable=load_data_to_hdfs,
        provide_context=True
    )

    crawl_categories >> crawl_product_id >> [crawl_comment, crawl_product_detail] >> load_to_hdfs