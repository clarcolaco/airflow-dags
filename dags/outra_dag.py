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


queries = {
    "query1": "olá tudo bem",
    "query2": "ola 2",
    "query3": "clarissa olá",
    "query4": "oi"
}


def task_final():
    for query in queries.items():
        print(f"Nome da query: {query[0]}, Item da query: {query[1]}")


with DAG(
    dag_id='clarissa_dag_queries_teste',
    description='Dag de teste query',
    schedule_interval='0 20 * * *',
    tags=["testinho", "cla", "query"],
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

    tchau7 = PythonOperator(
        task_id='task_tchau7',
        python_callable=task_tchau3,
    )

    task_final = PythonOperator(
        task_id='task_final',
        python_callable=task_final,
    )

tchau7 >> [tchau, tchau3, tchau2, tchau5, tchau6] >> tchau4 >> task_final
