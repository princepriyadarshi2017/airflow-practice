from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args ={
    'owner':'prince',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}


with DAG(
    dag_id='1first_dag_v2',
    default_args=default_args,
    description='This is our first dag',
    start_date=datetime(2024,2,19,2),
    schedule_interval='@daily'
) as dag:
    task1= BashOperator(
        task_id='first_task',
        bash_command="echo hello world,this is the first task!"
    )

    task2= BashOperator(
        task_id='second_task',
        bash_command="echo hey, here is second one!"
    )



    task3= BashOperator(
        task_id='third_task',
        bash_command="echo hey, here is third one!"
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # task1 >> task2
    # task1 >> task3


    task1 >> [task2,task3]

