'''
to be able pass parameter from one function which can be accessed
in another function we need to use something called xcoms stand for 
called xcoms that stands for 

CROSS COMMUNICATION

without this we will not able to pass onject from one function to another
reason being when we invoke these functions in the python operator
using python callable each of these function run in there own virtual envirnoment
the object of this python callable function is not vaialble to other function


'''




from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def extract_fn():
    print("Logic to Extract Data")
    return "10"

def transform_fn(ti):
    xcom_pull_obj = ti.xcom_pull(task_ids="EXTRACT")
    print("Value pulled from EXTRACT task:", xcom_pull_obj)
    return "11"

def load_fn(ti):
    xcom_pull_obj = ti.xcom_pull(task_ids="TRANSFORM")
    print("Value pulled from TRANSFORM task:", xcom_pull_obj)

def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2022, 6, 15)
}

with DAG("2ex_xcom_push_pull",
         default_args=def_args,
         catchup=False) as dag:

    extract_task = PythonOperator(
        task_id="EXTRACT",
        python_callable=extract_fn
    )

    transform_task = PythonOperator(
        task_id="TRANSFORM",
        python_callable=transform_fn,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="LOAD",
        python_callable=load_fn,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
