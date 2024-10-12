from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import json
from datetime import datetime
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
import time 
import os
import json
import requests
from airflow.operators.python import PythonOperator

DBT_CLOUD_CONN_ID = "dbt-conn"
JOB_ID = "70403104214918"

# Função para obter o token da API do Airbyte
def get_new_token(**kwargs):
    url = "https://api.airbyte.com/v1/applications/token"  # URL correta para obter o token
    payload = {
        "client_id": os.getenv("AIRBYTE_CLIENT_ID"),
        "client_secret": os.getenv("AIRBYTE_CLIENT_SECRET")
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 200:
        # Captura o token da resposta
        token = response.json().get('access_token')
        # Armazena o token usando XCom para ser usado em outras tarefas
        kwargs['ti'].xcom_push(key='airbyte_token', value=token)
    else:
        raise Exception(f"Erro ao obter token: {response.status_code} - {response.text}")
    
    # Adiciona um tempo de espera antes de iniciar a sincronização
    time.sleep(5)  # Pausa de 30 segundos (ajuste conforme necessário)

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 11),
    'retries': 1,
}

# Definindo o DAG com o decorador
@dag(default_args=default_args, schedule_interval="@daily", catchup=False)
def running_airbyte_dbt():

    # Task para obter o token antes de qualquer operação
    get_token_task = PythonOperator(
        task_id='get_access_token',
        python_callable=get_new_token,
        provide_context=True  # Permite que a função acesse o contexto de execução (kwargs)
    )

    # Task para iniciar a sincronização no Airbyte
    start_airbyte_sync = SimpleHttpOperator(
    task_id='start_airbyte_sync',
    http_conn_id='airbyte_default',
    endpoint='/v1/jobs',
    method='POST',
    headers={
        "Content-Type": "application/json", 
        "Authorization": "Bearer {{ task_instance.xcom_pull(task_ids='get_access_token', key='airbyte_token') }}"
    },
    data=json.dumps({
        "connectionId": Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID"),
        "jobType": "sync"
    }),
    response_check=lambda response: response.json().get('status') == 'running'
)

    @task
    def esperar():
        time.sleep(180)

    trigger_job = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID,
        check_interval=60,
        timeout=360,
    )

    t1 = esperar()
    # Define task dependencies
    get_token_task >> start_airbyte_sync >> t1 >> trigger_job

dag_instance = running_airbyte_dbt()