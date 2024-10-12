import os
import json
import requests
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from datetime import datetime

AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")

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

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

# Definindo o DAG com o decorador
@dag(default_args=default_args, schedule_interval="@daily", catchup=False)
def airbyte_sync_dag():

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
        endpoint='/v1/applications/token',  # Endpoint correto para iniciar a sincronização
        method='POST',
        headers={
            "Content-Type": "application/json", 
            # Pega o token diretamente do XCom
            "Authorization": "{{ task_instance.xcom_pull(task_ids='get_access_token', key='airbyte_token') }}"
        },
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID, "jobType":"sync"}),  # Assegure que o connectionId está correto
        response_check=lambda response: response.json()['status'] == 'running'
    )
    

    # Define a sequência de execução das tasks
    get_token_task >> start_airbyte_sync

# Instancia a DAG
dag_instance = airbyte_sync_dag()
