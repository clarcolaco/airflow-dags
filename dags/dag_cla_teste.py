from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def task_hello_world():
    print('ola, isso é um testinho')


def task_hello_world2():
    print('ola, isso é um testinho2')


def task_hello_world3():
    print('ola, isso é um testinho2')


def task_hello_world4():
    print('ola, isso é um testinho2')


with DAG(
    dag_id='clari_dag_hello_world',
    description='Dag de teste',
    schedule_interval='0 17 * * *',
    tags=["testinho"],
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args={
        'owner': 'Clarissa C.',
        'retry_delay': timedelta(minutes=20),
        'depends_on_past': False,
        'retries': 3
    }
) as dag:

    hello_world = PythonOperator(
        task_id='task_hello_world',
        python_callable=task_hello_world,
    )

    hello_world2 = PythonOperator(
        task_id='task_hello_world2',
        python_callable=task_hello_world2,
    )

    hello_world3 = PythonOperator(
        task_id='task_hello_world3',
        python_callable=task_hello_world3,
    )

    hello_world4 = PythonOperator(
        task_id='task_hello_world4',
        python_callable=task_hello_world4,
    )

    hello_world >> [hello_world2, hello_world3] >> hello_world4
