# =============================================================================
# Whale Alert EL Pipeline — Custom Airflow Image
# =============================================================================
# Base: official Apache Airflow 2.9.1 (Python 3.11)
# Extends the base image by installing project dependencies and copying
# application source code into the container.
# =============================================================================

FROM apache/airflow:2.9.1-python3.11

# Switch to root to install OS-level dependencies (if ever needed)
USER root

# Keep the image lean — remove apt lists after install
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for pip installs (security best practice)
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# Copy application source into PYTHONPATH
COPY src/ /opt/airflow/src/

# DAGs are mounted via volume in docker-compose (not baked into the image)
# This keeps the image reusable and lets you update DAGs without rebuilding.
