# IMPORTAR LIBRERIAS NECESARIAS:
import boto3
from botocore.exceptions import ClientError

# CONFIGURACION DE CONEXION A MINIO:
ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "desafio-bucket"

def crear_cliente_s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=boto3.session.Config(signature_version="s3v4"),
    )

def bucket_existe(s3, nombre_bucket):
    try:
        s3.head_bucket(Bucket=nombre_bucket)
        return True
    except ClientError:
        return False
    
def crear_bucket_si_no_existe(s3, nombre_bucket):
    if bucket_existe(s3, nombre_bucket):
        print(f"El bucket '{nombre_bucket}' ya existe.")
    else:
        s3.create_bucket(Bucket=nombre_bucket)
        print(f"Bucket '{nombre_bucket}' creado exitosamente.")

if __name__ == "__main__":
    s3 = crear_cliente_s3()
    crear_bucket_si_no_existe(s3, BUCKET_NAME)

    buckets = s3.list_buckets()
    print("\nBuckets disponibles en MinIO:")
    for bucket in buckets['Buckets']:
        print(f" - {bucket['Name']}")