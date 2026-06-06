# IMPORTAR LIBRERÍAS NECESARIAS
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError, EndpointConnectionError 
from airflow import DAG
from airflow.operators.python import PythonOperator

# DEFINIR FUNCION QUE CREA UN CLIENTE S3 APUNTANDO A MINIO Y LOSTA BUCKETS DISPONIBLES: 
def check_s3_connection():
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
            config=boto3.session.Config(signature_version="s3v4"),
        )
        buckets = s3.list_buckets()
        print("Conexion exitosa a MinIO")
        print("Buckets disponibles:", buckets.get("Buckets", []))
    except NoCredentialsError:
        raise Exception("Error: credenciales invalidas")
    except EndpointConnectionError:
        raise Exception("Error: no se puede conectar a MinIO") 
    except Exception as e:
        raise Exception(f"Error inesperado: {e}") 

# DEFINIR DAG DE AIRFLOW:
with DAG(
    dag_id="test_minio_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["MinIO", "test"],
) as dag:
    check_connection = PythonOperator(
        task_id="check_connection",
        python_callable=check_s3_connection,
    )