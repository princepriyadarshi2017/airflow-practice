from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime



def extr(a1, a2):
    print(f"{a1} and {a2} both are done")


with DAG(
    dag_id="op_kwargs",
    schedule_interval="@daily",
    start_date=datetime(2023,2,22,2)

) as dag:

    start = DummyOperator(
        task_id='start'
    )

    ex = PythonOperator(
        task_id='extra',
        python_callable= extr,
        op_kwargs={"a1":"first","a2":"second"}

    )

    start >> ex