from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models import Variable
from datetime import datetime

# Variável com o Airbyte Connection ID
AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")

@dag(start_date=datetime(2024, 4, 18), schedule_interval="@daily", catchup=False)
def airbyte_google_sheets_to_postgres():
    
    # Dispara a sincronização no Airbyte
    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_google_sheets_postgres',
        airbyte_conn_id='airbyte_default',  # Certifique-se que a conexão Airbyte está configurada no Airflow
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=False,  # Aguarda a conclusão da sincronização
        timeout=3600,  # Tempo máximo para a sincronização
        wait_seconds=30  # Intervalo entre as verificações do status
    )

    airbyte_sync

airbyte_google_sheets_to_postgres()
