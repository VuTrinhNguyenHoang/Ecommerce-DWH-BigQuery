from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import time, random
import logging
print("JAVA_HOME =", os.environ.get("JAVA_HOME"))

SPARK_BIGQUERY_JARS  = os.getenv("SPARK_BIGQUERY_JARS")
GCP_PROJECT_ID       = os.getenv("GCP_PROJECT_ID")
ADC_PATH             = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

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
    dic['date'] = datetime.today().strftime('%Y-%m-%d')
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
        for product_id in products_id[:30]:
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
    dic['date'] = datetime.today().strftime('%Y-%m-%d')
    return dic

def crawl_and_save_product_detail(**context):
    ti = context['ti']
    products_id = ti.xcom_pull(task_ids='crawl_product_id')
    if not products_id:
        logging.error("Không có product_id từ XCom")
        raise ValueError("Không có product_id từ XCom")

    try:
        product_details = []
        for product_id in products_id[:100]:
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

        context['ti'].xcom_push(key='comments_path', value=hdfs_comments_path)
        context['ti'].xcom_push(key='products_path', value=hdfs_products_path)
        
        os.remove(comments_parquet)
        os.remove(product_parquet)

        return hdfs_products_path
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

    clean_product_details_and_load_to_bigquery_task = SparkSubmitOperator(
        task_id='spark_clean_product_details_and_load_to_bigquery',
        application='/opt/airflow/scripts/clean_product_details_spark.py',  
        conn_id='spark_default', 
        verbose=True,
        application_args=["{{ ti.xcom_pull(task_ids='load_data_to_hdfs', key='products_path') }}"],
        dag=dag,
        jars=SPARK_BIGQUERY_JARS,
        conf={
            'spark.hadoop.google.cloud.auth.service.account.json.keyfile': ADC_PATH,
            'spark.hadoop.fs.gs.project.id': GCP_PROJECT_ID,
        }
    )

    clean_product_comments_and_load_to_bigquery_task = SparkSubmitOperator(
        task_id='spark_clean_product_comments_and_load_to_bigquery',
        application='/opt/airflow/scripts/clean_product_comments_spark.py',  
        conn_id='spark_default', 
        application_args=["{{ ti.xcom_pull(task_ids='load_data_to_hdfs', key='comments_path') }}"],
        verbose=True,
        dag=dag,
        jars=SPARK_BIGQUERY_JARS,
        conf={
            'spark.hadoop.google.cloud.auth.service.account.json.keyfile': ADC_PATH,
            'spark.hadoop.fs.gs.project.id': GCP_PROJECT_ID,
        }
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt/dbt-bigquery-project/ecommerce_dwh && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/dbt/dbt-bigquery-project/ecommerce_dwh && dbt test',
    )

    crawl_categories >> crawl_product_id >> [crawl_comment, crawl_product_detail] >> load_to_hdfs >> [clean_product_details_and_load_to_bigquery_task, clean_product_comments_and_load_to_bigquery_task] >> dbt_run >> dbt_test