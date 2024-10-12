from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
from datetime import datetime

AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f'Bearer {Variable.get("AIRBYTE_API_TOKEN")}'


@dag(start_date=datetime(2024, 10, 11), schedule_interval="@daily", catchup=False)
def running_airbyte():

    start_airbyte_sync = SimpleHttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte_default',
        endpoint='/api/v1/connections/sync',  # api/v1/connections/sync Endpoint correto para disparar a sincronização
        method='POST',
        headers={"Content-Type": "application/json", 
                    "User-Agent":"fake-useragent", 
                    "Accept":"application/json",
                    "client_id": "942195ea-999e-4f17-8d52-277fb5453949",
                    "client_secret": "hCGX2rYkLaD9vKH9csFYbR75wXIlGJkQ",
                    "Authorization": API_KEY},
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID, "jobType":"sync"}),  # Assegure que o connectionId está correto
         response_check=lambda response: response.json().get('status') == 'running'
    )

    start_airbyte_sync


running_airbyte()