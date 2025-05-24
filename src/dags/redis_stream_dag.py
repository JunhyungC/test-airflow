from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# 플러그인을 통해 등록된 Deferrable 센서를 임포트합니다.
# PYTHONPATH에 추가된 plugins 폴더를 기준으로 경로를 지정합니다.
from utils.sensors.redis_stream_sensor_async import RedisStreamSensorAsync
import os

# --- Module-level constants for environment variables ---
STREAM_NAME = os.getenv('STREAM_NAME', 'data_event_stream')
GROUP_NAME = os.getenv('GROUP_NAME', 'data_sync_consumer_group')
REDIS_CONN_ID = os.getenv('REDIS_CONN_ID', 'redis_default')
TRIGGERED_DAG_ID = os.getenv('TRIGGERED_DAG_ID', 'redis_stream_processor_dag')

# --- DAG 정의: Deferrable 센서 DAG ---
with DAG(
    dag_id='redis_stream_sensor_dag_async',
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(seconds=15),
    max_active_runs=2,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['redis', 'sensor', 'trigger', 'deferrable'],
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        # 'execution_timeout': timedelta(hours=1)  # 선택사항: 필요한 경우 전체 태스크 타임아웃 설정
    }
) as dag:

    wait_for_stream_data = RedisStreamSensorAsync(
        task_id='wait_for_stream_data_async',
        stream_name=STREAM_NAME,
        group_name=GROUP_NAME,
        redis_conn_id=REDIS_CONN_ID,
    )

    trigger_processor_dag = TriggerDagRunOperator(
        task_id='trigger_processor_dag',
        trigger_dag_id=TRIGGERED_DAG_ID,
    )

    wait_for_stream_data >> trigger_processor_dag 