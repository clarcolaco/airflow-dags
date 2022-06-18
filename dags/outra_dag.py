from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def task_tchau():
    print('ola, isso é um testinho')


def task_tchau2():
    print('ola, isso é um testinho2')


def task_tchau3():
    print('ola, isso é um testinho2')


def task_tchau4():
    print('ola, isso é um testinho2')


with DAG(
    dag_id='outra_dag',
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

    tchau = PythonOperator(
        task_id='task_tchau',
        python_callable=task_tchau,
    )

    tchau2 = PythonOperator(
        task_id='task_tchau2',
        python_callable=task_tchau2,
    )

    tchau3 = PythonOperator(
        task_id='task_tchau3',
        python_callable=task_tchau3,
    )

    tchau4 = PythonOperator(
        task_id='task_tchau4',
        python_callable=task_tchau4,
    )

    tchau5 = PythonOperator(
        task_id='task_tchau5',
        python_callable=task_tchau3,
    )

    tchau6 = PythonOperator(
        task_id='task_tchau6',
        python_callable=task_tchau3,
    )

    [tchau, tchau3, tchau2, tchau5, tchau6] >> tchau4
