from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import json
import requests
import os
from dotenv import load_dotenv
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Carregar variáveis do arquivo .env
load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# Função para obter o access token
def get_access_token():
    url = "https://api.airbyte.com/v1/applications/token"
    
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 200:
        token = response.json().get('access_token')  # Ajuste conforme a estrutura da resposta
        return token
    else:
        raise Exception(f"Error obtaining access token: {response.text}")

# DAG do Airflow
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, start_date=datetime(2024, 4, 18), schedule_interval="@daily", catchup=False)
def running_airbyte():
    # Tarefa para obter o access token
    get_token_task = PythonOperator(
        task_id='get_access_token',
        python_callable=get_access_token,
        do_xcom_push=True,  # Habilitar para que o token seja enviado para o XCom
    )

    # Recuperando o token do XCom
    def get_token(**kwargs):
        return kwargs['ti'].xcom_pull(task_ids='get_access_token')

    # Tarefa para iniciar a sincronização do Airbyte
    start_airbyte_sync = SimpleHttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte_default',
        endpoint='/v1/jobs',  # Endpoint correto para disparar a sincronização
        method='POST',
        headers={
            "Content-Type": "application/json", 
            "User-Agent": "fake-useragent", 
            "Accept": "application/json",
            "Authorization": "{{ task_instance.xcom_pull(task_ids='get_access_token') }}"  # Usando Jinja para puxar o token do XCom
        },
        data=json.dumps({
            "connectionId": Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID"),
            "jobType": "sync"
        }),  # Assegure que o connectionId está correto
        response_check=lambda response: response.json().get('status') == 'running'
    )

    get_token_task >> start_airbyte_sync  # Definindo a ordem de execução

# Instanciando a DAG
dag_instance = running_airbyte()
