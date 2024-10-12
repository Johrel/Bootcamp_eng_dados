from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
import requests
from datetime import datetime
import os
from dotenv import load_dotenv

AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
load_dotenv()

def get_new_jwt():
# Carregar variáveis de ambiente do arquivo .env
    client_id = os.getenv("AIRBYTE_CLIENT_ID")
    client_secret = os.getenv("AIRBYTE_CLIENT_SECRET")

    # Implemente a lógica para obter um novo token JWT aqui
    response = requests.post('https://api.airbyte.com/api/v1/applications/token', 
                             data=json.dumps({"client_id": client_id,
                                               "client_secret": client_secret}),
                                                headers={"Content-Type": "application/json"})
    if response.status_code == 200:
        return response.json().get('token')  # Supondo que o token é retornado assim
    else:
        raise Exception("Erro ao obter novo token JWT")

# Use a função para obter um novo token antes de fazer a chamada que falhou
API_KEY = get_new_jwt()


@dag(start_date=datetime(2024, 10, 11), schedule_interval="@daily", catchup=False)
def running_airbyte():

    start_airbyte_sync = SimpleHttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte_default',
        endpoint='api/v1/connections/sync',  # api/v1/connections/sync Endpoint correto para disparar a sincronização
        method='POST',
        headers={"Content-Type": "application/json", 
                    "User-Agent":"fake-useragent", 
                    "Accept":"application/json",
                    "Authorization": API_KEY},
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID, "jobType":"sync"}),  # Assegure que o connectionId está correto
         response_check=lambda response: response.json().get('status') == 'running'
    )

    start_airbyte_sync


running_airbyte()