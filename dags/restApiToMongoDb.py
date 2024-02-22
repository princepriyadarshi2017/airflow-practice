from datetime import datetime
import json
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import logging
from pymongo import MongoClient

def save_posts(ti) -> None:
    try:
        posts = ti.xcom_pull(task_ids=['get_posts'])
        client = MongoClient('mongodb://localhost:27017/')
        db = client['webdev_minervadb']
        collection = db['test']
        collection.insert_one(posts[0])
        logging.info("Posts saved successfully to MongoDB.")
    except Exception as e:
        logging.error(f"Failed to save posts to MongoDB: {e}")



with DAG (
    dag_id='api_dag_mongo',
    schedule_interval='@daily',
    start_date=datetime(2024,2,19,2),
    catchup=False
) as dag:


    task_is_api_active = HttpSensor(
        task_id ='is_api_active',
        http_conn_id='api_posts',
        endpoint='posts/'
    )

    task_get_posts=SimpleHttpOperator(
        task_id='get_posts',
        http_conn_id='api_posts',
        endpoint='posts/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )


    task_save_posts = PythonOperator(
        task_id='save_posts',
        python_callable=save_posts,
        provide_context=True
    )


    task_is_api_active >> task_get_posts >> task_save_posts