from time import sleep
from airflow.decorators import dag, task
from datetime import datetime

@dag(
        dag_id="DAG_Airflow",
        description="minha etl braba",
        schedule="0 12 * 1-12 *", ##crontab.guru
        start_date=datetime(2024,10,11),
        catchup=False #backfill
)
def DAG_Airflow():

    @task
    def primeira_atividade():
        print("minha primeira atividade! - Hello World")
        sleep(2)

    @task
    def segunda_atividade():
        print("minha segunda atividade! - Hello World")
        sleep(2)
    
    @task
    def terceira_atividade():
        print("minha terceira atividade - Hello World")
        sleep(2)

    @task
    def quarta_atividade():
        print("pipeline finalizou")   

    t1 = primeira_atividade()
    t2 = segunda_atividade()
    t3 = terceira_atividade()
    t4 = quarta_atividade()

    t1 >> t2 >> t3 >> t4

DAG_Airflow()