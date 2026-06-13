# Whale Alert EL Pipeline

Pipeline de datos **Extract-Load (EL)** que extrae movimientos de criptomonedas de [whale-alert.io](https://whale-alert.io/whales.html), los modela en un dataclass tipado y los persiste como CSV en un bucket de **MinIO** (S3-compatible). Orquestado por **Apache Airflow** y ejecutado íntegramente en **Docker**.

---

## Arquitectura

```
whale-alert-el/
├── dags/
│   └── whale_el_pipeline.py    # DAG de Airflow: extract → load
├── src/
│   ├── clients/
│   │   ├── whale_client.py     # Scraper HTTP + dataclass WhaleRecord
│   │   └── storage_client.py   # Cliente MinIO/S3 con boto3
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
     │  HTTP GET (requests.Session con User-Agent)
     ▼
[WhaleClient.extract()]
     │  List[WhaleRecord]  — dataclass tipada con datetime_utc, crypto, known, unknown
     │
     ▼  XCom (serialización a List[dict])
[pd.DataFrame → CSV string]
     │
     ▼
[StorageClient.upload_csv()]
     │  ensure_bucket_exists() → crea el bucket si no existe
     ▼
MinIO  ──►  s3://whale-alert/whale_data_YYYYMMDD_HHMM.csv
```

---

## Requisitos previos

Antes de ejecutar el proyecto, asegurate de tener instalado:

- **Docker Desktop** (incluye Docker Compose v2) — [docker.com/get-started](https://www.docker.com/get-started/)
- **Python 3.10+** — solo si querés correr linters o tests localmente
- **Git** — para clonar el repositorio

Verificá las instalaciones con:

```bash
docker --version
docker compose version
```

> No es necesario instalar Python, Airflow ni ninguna dependencia localmente. Todo corre dentro de Docker.

---

## Stack de servicios

El `docker-compose.yml` orquesta **5 contenedores** con healthchecks y dependencias declaradas:

| Contenedor | Imagen | Puerto | Rol |
|---|---|---|---|
| `whale-postgres` | `postgres:15-alpine` | — | Metadata DB de Airflow |
| `whale-minio` | `minio/minio:latest` | `9000` / `9001` | Object storage S3-compatible |
| `whale-airflow-init` | build local | — | Migración de DB + creación de usuario admin (corre una sola vez) |
| `whale-airflow-webserver` | build local | `8080` | UI web de Airflow |
| `whale-airflow-scheduler` | build local | — | Scheduler de Airflow |

Los servicios de Airflow esperan a que `postgres` y `minio` estén **healthy** antes de arrancar, y el webserver/scheduler esperan además a que `airflow-init` haya **completado exitosamente**.

---

## Inicio rápido

### 1. Clonar y configurar el entorno

```bash
git clone https://github.com/FMauro17/whale-alert-el.git
cd whale-alert-el
cp .env.example .env
```

Editá `.env` y reemplazá `YOUR_FERNET_KEY_HERE` con una clave generada:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Levantar el stack

```bash
docker compose up --build -d
```

El servicio `airflow-init` corre una sola vez para migrar la DB y crear el usuario admin. Podés verificar que todo esté sano con:

```bash
docker compose ps
```

### 3. Acceder a las UIs

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow Webserver | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

### 4. Activar el DAG

En la UI de Airflow, activá el DAG **`whale_el_pipeline`**. Se ejecutará automáticamente cada **30 minutos** (`schedule_interval="*/30 * * * *"`).

---

## Configuración

Todas las variables se gestionan en `.env` (nunca commitear al repositorio):

| Variable | Valor por defecto | Descripción |
|---|---|---|
| `MINIO_ENDPOINT` | `http://minio:9000` | URL de la API de MinIO |
| `MINIO_ACCESS_KEY` | `minioadmin` | Access key de MinIO |
| `MINIO_SECRET_KEY` | `minioadmin` | Secret key de MinIO |
| `MINIO_BUCKET_NAME` | `whale-alert` | Bucket destino |
| `WHALE_URL` | `https://whale-alert.io/whales.html` | URL a scrapear |
| `AIRFLOW__CORE__FERNET_KEY` | — | Clave de cifrado de Airflow (**requerida**) |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Ejecutor de Airflow |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://...` | Conexión a la metadata DB |
| `POSTGRES_USER` | `airflow` | Usuario de PostgreSQL |
| `POSTGRES_PASSWORD` | `airflow` | Contraseña de PostgreSQL |
| `POSTGRES_DB` | `airflow` | Base de datos de PostgreSQL |

---

## DAG: `whale_el_pipeline`

El DAG tiene **2 tasks** en secuencia con retries automáticos:

```
extract  ──►  load
```

| Task | Función | Descripción |
|---|---|---|
| `extract` | `task_extract()` | Instancia `WhaleClient`, llama a `extract()` y serializa los `WhaleRecord` a `List[dict]` para XCom |
| `load` | `task_load()` | Lee los registros de XCom, construye un `DataFrame`, genera el CSV y lo sube a MinIO |

**Manejo de errores:**
- Cada task tiene `retries=3` con un `retry_delay` de 60 segundos.
- Si `extract` no devuelve registros, `load` lanza `ValueError` antes de intentar la subida.
- `StorageClient.ensure_bucket_exists()` crea el bucket automáticamente si no existe.

---

## Datos extraídos

El `WhaleClient` parsea la tabla de whale-alert.io y genera instancias de `WhaleRecord`:

| Campo | Tipo | Descripción |
|---|---|---|
| `datetime_utc` | `datetime` | Momento exacto del scraping en UTC |
| `crypto` | `str` | Símbolo de la criptomoneda (ej: `BTC`, `ETH`) |
| `known` | `str` | Volumen de wallets conocidas |
| `unknown` | `str` | Volumen de wallets desconocidas |

El archivo resultante en MinIO sigue el patrón: `whale_data_YYYYMMDD_HHMM.csv`

---

## Ejemplo de output

**Logs de Airflow (tarea `extract`):**
```
[2026-06-12 14:00:01] INFO - === TASK: extract — START ===
[2026-06-12 14:00:02] INFO - Starting extraction from https://whale-alert.io/whales.html
[2026-06-12 14:00:03] INFO - Extraction successful — 15 records retrieved
[2026-06-12 14:00:03] INFO - === TASK: extract — 15 records extracted ===
```

**Logs de Airflow (tarea `load`):**
```
[2026-06-12 14:00:04] INFO - === TASK: load — START ===
[2026-06-12 14:00:04] INFO - Target object key: whale_data_20260612_1400.csv
[2026-06-12 14:00:04] INFO - StorageClient initialised — endpoint=http://minio:9000, bucket=whale-alert
[2026-06-12 14:00:04] INFO - Bucket 'whale-alert' already exists
[2026-06-12 14:00:04] INFO - Uploading 'whale_data_20260612_1400.csv' to bucket 'whale-alert' (847 bytes)
[2026-06-12 14:00:04] INFO - Upload complete — s3://whale-alert/whale_data_20260612_1400.csv (847 bytes transferred)
[2026-06-12 14:00:04] INFO - === TASK: load — COMPLETE | file=whale_data_20260612_1400.csv | bytes=847 ===
```

**CSV resultante (`whale_data_20260612_1400.csv`):**
```csv
datetime_utc,crypto,known,unknown
2026-06-12T14:00:01.423167+00:00,BTC,1,234.5M,890.1M
2026-06-12T14:00:01.423167+00:00,ETH,567.8M,123.4M
2026-06-12T14:00:01.423167+00:00,USDT,2,345.6M,456.7M
...
```

---

## Stack de tecnologías

| Tecnología | Versión | Rol |
|---|---|---|
| Apache Airflow | 2.9.1 | Orquestación del pipeline |
| MinIO | latest | Object storage S3-compatible |
| PostgreSQL | 15-alpine | Metadata DB de Airflow |
| boto3 | — | Cliente S3/MinIO |
| BeautifulSoup4 | — | Parsing del HTML |
| requests | — | Solicitudes HTTP con Session |
| Pandas | — | Construcción del DataFrame y exportación CSV |
| Docker Compose | v2 | Infraestructura local completa |

---

## Detener el stack

```bash
docker compose down        # Conserva los volúmenes (datos en MinIO y Airflow DB)
docker compose down -v     # Elimina también los volúmenes
```

---

## Autor

Mauro Filani — Data Engineering — Mayo 2026
