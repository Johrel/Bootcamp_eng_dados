import os
import json
import requests
from dotenv import load_dotenv
from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime

# Carregar variáveis do .env
load_dotenv()

def get_new_token():
    # URL para obter o token
    url = "https://api.airbyte.com/auth/token"  # Substitua pela URL correta da API
    payload = {
        'client_id': os.getenv("AIRBYTE_CLIENT_ID"),
        'client_secret': os.getenv("AIRBYTE_CLIENT_SECRET")
    }
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        token = response.json().get('token')  # Ajuste isso com base na resposta da sua API
        return f'Bearer {token}'
    else:
        raise Exception(f"Erro ao obter token: {response.status_code} - {response.text}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

@dag(default_args=default_args, schedule_interval="@daily", catchup=False)
def running_airbyte():
    
    def task_using_token(**kwargs):
        # Captura um novo token a cada execução
        api_key = get_new_token()
        kwargs['ti'].xcom_push(key='api_key', value=api_key)
        print(f"Token: {api_key}")
        
    get_token_task = PythonOperator(
        task_id='get_access_token',
        python_callable=task_using_token,
        provide_context=True,  # Habilitar para que kwargs funcione
    )

    # Recuperando o token do XCom
    def get_token(**kwargs):
        return kwargs['ti'].xcom_pull(task_ids='get_access_token', key='api_key')

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
            "Authorization": "{{ task_instance.xcom_pull(task_ids='get_access_token', key='api_key') }}"  # Usando Jinja para puxar o token do XCom
        },
        data=json.dumps({
            "connectionId": Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID"),
            "jobType": "sync"
        }),  # Assegure que o connectionId está correto
        response_check=lambda response: response.json().get('status') == 'running'
    )

    # Definindo a ordem de execução
    get_token_task >> start_airbyte_sync  

# Instanciando a DAG
dag_instance = running_airbyte()
