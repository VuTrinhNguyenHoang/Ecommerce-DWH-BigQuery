from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def crawl_and_save_categories():
    """
    Hàm này sẽ gửi request đến trang Tiki, 
    lấy danh mục sản phẩm (category_id), và lưu vào file CSV.
    """
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
        'category': '1789',
        'page': '1',
    }

    response = requests.get("https://tiki.vn/", headers=headers, params=params)
    soup = BeautifulSoup(response.text, 'html.parser')

    div_container = soup.find('div', class_='styles__StyledListItem-sc-w7gnxl-0 cjqkgR')
    a_tags = div_container.find_all('a')

    categories_id = []
    for a in a_tags:
        href = a.get('href', '')
        match = re.search(r'/c(\d+)', href)
        if match:
            categories_id.append(match.group(1))

    df = pd.DataFrame({'category_id': categories_id})
    df.to_csv('/opt/airflow/dags/categories/categories_id.csv', index=False)
    print(f"Đã lưu {len(categories_id)} category_id vào file CSV!")

with DAG(
    dag_id='crawl_categories_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    crawl_task = PythonOperator(
        task_id='crawl_categories',
        python_callable=crawl_and_save_categories
    )

    crawl_task
