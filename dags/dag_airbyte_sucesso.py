from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
from datetime import datetime
import requests
import os
import time
from airflow.models import Variable
from dotenv import load_dotenv


# Carrega as variáveis do arquivo .env
load_dotenv()

# Função para obter um novo token JWT
def get_new_jwt():
    client_id = os.getenv("AIRBYTE_CLIENT_ID")
    client_secret = os.getenv("AIRBYTE_CLIENT_SECRET")

    response = requests.post('https://api.airbyte.com/api/v1/applications/token', 
                             json={"client_id": client_id, "client_secret": client_secret},
                             headers={"Content-Type": "application/json"})
    
    if response.status_code == 200:
        token_info = response.json()
        return token_info['token'], token_info.get('expires_in', 3600)  # Obtenha o tempo de expiração
    else:
        raise Exception("Erro ao obter novo token JWT")

# Função para obter o token JWT, gerando um novo se necessário
def get_jwt():
    token_data = Variable.get("AIRBYTE_API_TOKEN", deserialize_json=True)

    # Verifica se o token está presente e se está expirado
    if token_data and 'token' in token_data and 'expiration' in token_data:
        if token_data['expiration'] > time.time():
            return token_data['token']  # Retorna o token se ainda for válido

    # Caso contrário, gera um novo token
    token, expires_in = get_new_jwt()
    expiration = time.time() + expires_in
    Variable.set("AIRBYTE_API_TOKEN", {"token": token, "expiration": expiration}, serialize_json=True)
    return token

# Em sua DAG, sempre use a função get_jwt()
API_KEY = get_jwt()
AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")


@dag(start_date=datetime(2024, 4, 18), schedule_interval="@daily", catchup=False)
def running_airbyte():

    start_airbyte_sync = SimpleHttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte_default',
        endpoint=f'/v1/jobs',  # api/v1/connections/sync Endpoint correto para disparar a sincronização
        method='POST',
        headers={"Content-Type": "application/json", 
                 "User-Agent":"fake-useragent", 
                 "Accept":"application/json",
                 "Authorization": API_KEY},
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID, "jobType":"sync"}),  # Assegure que o connectionId está correto
        response_check=lambda response: response.json()['status'] == 'running'
    )

    start_airbyte_sync


running_airbyte()