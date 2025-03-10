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
    'Connection': 'keep-alive',
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

def comment_parser(json):
    dic = dict()
    dic['id'] = json.get('id', None)
    dic['product_id'] = json.get('product_id', None) 
    dic['title'] = json.get('title', None)
    dic['content'] = json.get('content', None)
    dic['rating'] = json.get('rating', None)
    dic['created_at'] = json.get('created_at', None)
    dic['purchased_at'] = json.get('created_by', {}).get('purchased_at', None)
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
        for product_id in products_id:
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

def parser_product(json):
    dic = dict()
    dic['id'] = json.get('id', None)
    dic['name'] = json.get('name', None)
    dic['short_description'] = json.get('short_description', None)
    dic['price'] = json.get('price', None)
    dic['list_price'] = json.get('list_price', None)
    dic['discount'] = json.get('discount', None)
    dic['discount_rate'] = json.get('discount_rate', None)
    dic['all_time_quantity_sold'] = json.get('all_time_quantity_sold', None)
    dic['inventory_status'] = json.get('inventory_status', None)
    dic['stock_item_qty'] = json.get('stock_item', {}).get('qty', None)
    dic['stock_item_max_sale_qty'] = json.get('stock_item', {}).get('max_sale_qty', None)
    return dic

def crawl_and_save_product_detail(**context):
    ti = context['ti']
    products_id = ti.xcom_pull(task_ids='crawl_product_id')
    if not products_id:
        logging.error("Không có product_id từ XCom")
        raise ValueError("Không có product_id từ XCom")

    try:
        product_details = []
        for product_id in products_id:
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

    crawl_categories >> crawl_product_id >> [crawl_comment, crawl_product_detail]