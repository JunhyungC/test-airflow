FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    netcat-openbsd \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# airflow 사용자를 위해 /app 디렉토리를 생성하고 권한을 설정합니다.
RUN mkdir -p /app && \
    chown -R airflow /app

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# src 폴더 복사
COPY src/ /opt/airflow/
COPY entrypoint.sh /entrypoint.sh

WORKDIR /opt/airflow

ENTRYPOINT ["/entrypoint.sh"] 