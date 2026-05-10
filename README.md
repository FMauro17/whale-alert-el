# Whale Alert EL Pipeline 🐋

Pipeline de datos **Extract-Load (EL)** que extrae información de [whale-alert.io](https://whale-alert.io/whales.html) y la persiste en un bucket de **MinIO**, orquestado por **Apache Airflow** y ejecutado íntegramente en **Docker**.

---

## Arquitectura

```
whale-alert-el/
├── dags/
│   └── whale_el_pipeline.py    # DAG de Airflow (extract → load)
├── src/
│   ├── clients/
│   │   ├── whale_client.py     # Scraper HTTP con dataclass WhaleRecord
│   │   └── storage_client.py   # Cliente MinIO (boto3)
│   └── utils/
│       └── logger.py           # Logging centralizado
├── .env.example                # Plantilla de variables de entorno
├── .gitignore
├── docker-compose.yml          # Stack completo con healthchecks
├── Dockerfile                  # Imagen Airflow + dependencias del proyecto
└── requirements.txt
```

### Flujo del Pipeline

```
whale-alert.io
     │
     │  HTTP GET
     ▼
[WhaleClient.extract()]
     │ List[WhaleRecord]  (dataclass tipada)
     │
     ▼  XCom
[pd.DataFrame → CSV string]
     │
     ▼
[StorageClient.upload_csv()]
     │
     ▼
MinIO  ──►  s3://whale-alert/whale_data_YYYYMMDD_HHMM.csv
```

---

## Inicio Rápido

### 1. Clonar y configurar el entorno

```bash
git clone <repo-url>
cd whale-alert-el
cp .env.example .env
```

Edita `.env` y reemplaza `YOUR_FERNET_KEY_HERE` con una clave generada:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Levantar el stack

```bash
docker compose up --build -d
```

El servicio `airflow-init` corre una sola vez para migrar la DB y crear el usuario admin.

### 3. Acceder a las UIs

| Servicio          | URL                    | Credenciales |
|-------------------|------------------------|--------------|
| Airflow Webserver | http://localhost:8080  | admin / admin |
| MinIO Console     | http://localhost:9001  | minioadmin / minioadmin |

### 4. Activar el DAG

En la UI de Airflow, activa el DAG **`whale_el_pipeline`**. Se ejecutará automáticamente cada **30 minutos**.

---

## Configuración

Todas las variables se gestionan en `.env` (nunca commitear al repositorio):

| Variable                          | Descripción                          |
|-----------------------------------|--------------------------------------|
| `MINIO_ENDPOINT`                  | URL de la API de MinIO               |
| `MINIO_ACCESS_KEY`                | Access key de MinIO                  |
| `MINIO_SECRET_KEY`                | Secret key de MinIO                  |
| `MINIO_BUCKET_NAME`               | Nombre del bucket destino            |
| `WHALE_URL`                       | URL de whale-alert.io                |
| `AIRFLOW__CORE__FERNET_KEY`       | Clave de cifrado de Airflow          |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Conexión a la DB de metadatos    |

---

## Stack de Tecnologías

- **Apache Airflow 2.9.1** — Orquestación
- **MinIO** — Object storage S3-compatible
- **PostgreSQL 15** — Metadata DB de Airflow
- **boto3** — Cliente S3/MinIO
- **BeautifulSoup4 + requests** — Web scraping
- **Pandas** — Exportación a CSV
- **Docker Compose** — Infraestructura local

---

## Detener el Stack

```bash
docker compose down           # Conserva los volúmenes (datos)
docker compose down -v        # Elimina también los volúmenes
```
