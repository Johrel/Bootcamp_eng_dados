from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import json
import requests
import os
from dotenv import load_dotenv
from airflow import DAG
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
        # Armazenar o token em um lugar adequado (ex: variável global, banco de dados, etc.)
        return token
    else:
        raise Exception(f"Error obtaining access token: {response.text}")

# DAG do Airflow
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='DAG to use Airbyte API with dynamic token',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 12),
)

# Tarefa para obter o access token
get_token_task = PythonOperator(
    task_id='get_access_token',
    python_callable=get_access_token,
    dag=dag,
)

# Em sua DAG, sempre use a função get_jwt()
API_KEY = get_token_task
AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")

@dag(start_date=datetime(2024, 4, 18), schedule_interval="@daily", catchup=False)
def running_airbyte():

    start_airbyte_sync = SimpleHttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte_default',
        endpoint='/v1/jobs',  # api/v1/connections/sync Endpoint correto para disparar a sincronização
        method='POST',
        headers={
            "Content-Type": "application/json", 
            "User-Agent": "fake-useragent", 
            "Accept": "application/json",
            "Authorization": API_KEY
        },
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID, "jobType": "sync"}),  # Assegure que o connectionId está correto
        response_check=lambda response: response.json().get('status') == 'running'
    )

    start_airbyte_sync

running_airbyte()
