
import logging
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import time
import pendulum
from airflow.models.param import Param
import duckdb
import requests
import os
log = logging.getLogger(__name__)


# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
def print_kwargs(**kwargs):
    """This is a function that will run within the DAG execution"""
    print(kwargs)
    if kwargs['params']['email']:
        print('Email', kwargs['params']['email'])
    github_url = 'https://raw.githubusercontent.com/clarcolaco/kaggle_csv/master/kc_house_data.csv'
    response = requests.get(github_url)
    file_name = 'file_duckdb.csv'
    if response.status_code == 200:
        with open(file_name, 'wb') as file:
            file.write(response.content)
            print("Arquivo baixado com sucesso!")
    else:
        print("Erro ao baixar o arquivo:", response.status_code)
    
    for root, dirs, files in os.walk(r'/'):
        for name in files:
            if name == file_name:
                directory_file_name = os.path.abspath(os.path.join(root,name))
                print(directory_file_name)
    df = duckdb.read_csv(directory_file_name)
    read = duckdb.sql("SELECT * FROM df limit 2").show()
    print('Lendo retorno de query do duckdb...')
    print(read)
    read = duckdb.sql("SELECT count(*) FROM df").show()
    print('Lendo retorno de query do duckdb de counts...')
    print(read)
    print(f'Deletando temp_file, {file_name}')
    if os.path.exists(file_name):
        os.remove(file_name)
        print("Arquivo excluído com sucesso!")
    else:
        print("O arquivo não existe.")


with DAG(
    dag_id="dag_my_test_duckdb",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["kwargs,duckdb"],
    params={
        "email": Param(
            default="example@example.com",
            type="string",
            format="idn-email",
            minLength=5,
            maxLength=255,
        ),
    }
):

    # run_this = BashOperator(
    # task_id="run_bash",
    # bash_command="cd /tmp && ls")

    print_kwargs = PythonOperator(
        task_id=f"task_print_kwargs",
        python_callable=print_kwargs,
        # op_kwargs={"random_base": i / 10}
    )
    

# run_this >> 
print_kwargs
