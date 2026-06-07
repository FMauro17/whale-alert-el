# IMPORTAR LIBRERIAS NECESARIAS:

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# DEFINIR DAG DE AIRFLOW:
with DAG(
    dag_id="master_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id="start")

    trigger_test_run = TriggerDagRunOperator(
        task_id="trigger_test_run",
        trigger_dag_id="test_minio_connection",
        wait_for_completion=True,
    )

    trigger_minio = TriggerDagRunOperator(
        task_id="trigger_minio",
        trigger_dag_id="test_minio_connection",
        wait_for_completion=True,
    )

    trigger_whale_alert = TriggerDagRunOperator(
        task_id="trigger_whale_alert",
        trigger_dag_id="whale_el_pipeline",
        wait_for_completion=True,
    )

    end = EmptyOperator(task_id="end")

    start >> trigger_test_run >> trigger_minio >> trigger_whale_alert >> end 
