from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.elasticsearch.hooks.elasticsearch import (
    ElasticsearchHook
)
from datetime import datetime, timedelta
import logging
import os
# 데이터 모델 임포트 (개별 파일에서)
from utils.entities.data import Data

# Sunrise API Hook 임포트
from utils.hooks.sunrise_api_hook import SunriseApiHook

# Elasticsearch client for type hinting
from elasticsearch import Elasticsearch as ElasticsearchClient
from elasticsearch.helpers import bulk as es_bulk
import concurrent.futures
import time

# --- Module-level constants ---
SUNRISE_API_CONN_ID = os.getenv('SUNRISE_API_CONN_ID', 'sunrise_api_default')
REDIS_CONN_ID = os.getenv('REDIS_CONN_ID', 'redis_default')
ELASTICSEARCH_CONN_ID = os.getenv('ELASTICSEARCH_CONN_ID', 'elasticsearch_default')
DATA_INDEX_NAME = os.getenv('DATA_INDEX_NAME', 'superb_platform_data_index')
STREAM_NAME = os.getenv('STREAM_NAME', 'data_event_stream')
GROUP_NAME = os.getenv('GROUP_NAME', 'data_sync_consumer_group')
CONSUMER_NAME_PROCESSOR = os.getenv('CONSUMER_NAME_PROCESSOR', 'airflow_processor_consumer')
REDIS_DATA_SYNC_DLQ_STREAM = os.getenv('REDIS_DATA_SYNC_DLQ_STREAM', 'data_sync_dlq_stream')
MAX_WORKERS_FOR_MESSAGE_PROCESSING = int(os.getenv('MAX_WORKERS_FOR_MESSAGE_PROCESSING', '2'))
BLOCK_TIMEOUT_MS = int(os.getenv('BLOCK_TIMEOUT_MS', '600000'))
PROCESS_BATCH_SIZE = int(os.getenv('PROCESS_BATCH_SIZE', '100'))
ES_BULK_MAX_WAIT_SECONDS = int(os.getenv('ES_BULK_MAX_WAIT_SECONDS', '10'))
ES_BULK_ACTIONS_THRESHOLD = int(os.getenv('ES_BULK_ACTIONS_THRESHOLD', '50'))


DELETE_ERROR_MESSAGES = [
    "not found",
    "Failed to data",
]


def send_to_dlq(
    redis_client, original_message_id: str, original_stream: str,
    original_group: str, dataset_id: str, data_id: str,
    job_list_str: str, error_info, log: logging.Logger
):
    dlq_stream_name = REDIS_DATA_SYNC_DLQ_STREAM
    dlq_message = {
        "datasetId": dataset_id,
        "dataId": data_id,
        "jobList": job_list_str,
        "originalStream": original_stream,
        "originalMessageId": original_message_id,
        "error": str(error_info)[:1000]  # Truncate error message
    }
    try:
        redis_client.xadd(dlq_stream_name, dlq_message)
        log.info(
            f"Message for data {data_id} (original ID: {original_message_id}) "
            f"sent to DLQ stream '{dlq_stream_name}'."
        )

        if original_stream and original_group and original_message_id:
            acked_count = redis_client.xack(
                original_stream, original_group, original_message_id
            )
            if acked_count == 1:
                log.info(
                    f"Original message {original_message_id} acknowledged "
                    f"after sending to DLQ."
                )
            else:
                log.warning(
                    f"Failed to ack original message {original_message_id} "
                    f"after DLQ (acked_count: {acked_count})."
                )
        else:
            log.warning(
                f"Could not ack original message due to missing "
                f"stream/group/id info for data {data_id}"
            )

    except Exception as e:
        log.error(
            f"Failed to send message for data {data_id} to DLQ stream "
            f"'{dlq_stream_name}': {e}"
        )


# --- 스레드 내에서 단일 메시지 처리를 위한 헬퍼 함수 ---
def _process_single_message_in_thread(
    message_info: dict,
    data_index_name: str,
    current_stream_key: str,
    group_name: str,
    log: logging.Logger
) -> tuple:
    """
    단일 Redis 메시지를 처리합니다 (Sunrise API 호출, ES 액션 준비 등).

    결과로 (ES 액션 dict 또는 None, ACK할 메시지 ID 또는 None,
             DLQ 정보 dict 또는 None)를 반환합니다.
    DLQ 정보 dict 예시: {
        'original_message_id': "msg_id_str",
        'original_stream': "stream_name_str",
        'original_group': "group_name_str",
        'dataset_id': "dataset_id_str",
        'data_id': "data_id_str",
        'job_list_str': "job_list_str",
        'error_info': "Exception object or error string"
    }
    """
    msg_id = message_info['id']
    msg_data = message_info['data']

    dataset_id = msg_data.get('datasetId')
    data_id = msg_data.get('dataId')
    job_list_str = msg_data.get('jobList', '')

    if not dataset_id or not data_id:
        log.warning(
            f"Message {msg_id} missing 'datasetId' or 'dataId'. "
            f"Skipping for ES, but will be ACKed. Data: {msg_data}"
        )
        return None, msg_id, None

    log.info(
        f"Thread processing message {msg_id} for data_id: {data_id}, "
        f"dataset_id: {dataset_id}"
    )

    es_action = None
    dlq_payload = None
    ack_id = msg_id  # 기본적으로 성공/실패 관계없이 ACK 시도

    try:
        sunrise_hook_local = SunriseApiHook(http_conn_id=SUNRISE_API_CONN_ID)
        is_data_delete = False
        data_dict_from_api = None  # Initialize data_dict_from_api

        try:
            log.info(
                f"Thread fetching data via GQL: dataset_id={dataset_id}, "
                f"data_id={data_id} for msg_id {msg_id}"
            )
            data_dict_from_api = sunrise_hook_local.get_data(
                dataset_id=dataset_id, data_id=data_id
            )
            log.debug(
                f"Data {data_id} (msg {msg_id}) fetched from API: "
                f"{str(data_dict_from_api)[:200]}..."
            )
        except Exception as api_e:
            log.warning(
                f"Error retrieving data {data_id} (msg {msg_id}) from API: {str(api_e)}"
            )
            if any(err_msg.lower() in str(api_e).lower()
                   for err_msg in DELETE_ERROR_MESSAGES):
                is_data_delete = True
                log.info(
                    f"Data {data_id} (msg {msg_id}) flagged for deletion "
                    f"based on API error."
                )
            else:
                dlq_payload = {
                    'original_message_id': msg_id,
                    'original_stream': current_stream_key,
                    'original_group': group_name,
                    'dataset_id': dataset_id,
                    'data_id': data_id,
                    'job_list_str': job_list_str,
                    'error_info': api_e
                }
                return None, ack_id, dlq_payload

        if is_data_delete:
            es_action = {
                "_op_type": "delete",
                "_index": data_index_name,
                "_id": data_id
            }
            log.info(
                f"Prepared ES delete action for data {data_id} (msg {msg_id})."
            )
        elif data_dict_from_api is not None:  # Check if data_dict_from_api is populated
            annotation_data = data_dict_from_api.pop('annotation', None)
            annotation_obj = None
            if annotation_data:
                try:
                    from utils.entities.annotation import Annotation as AnnotationModel # noqa
                    annotation_obj = AnnotationModel.model_validate(
                        annotation_data
                    )
                except Exception as e_anno:
                    log.warning(
                        f"Failed to parse annotation data for {data_id}: {e_anno}"
                    )

            try:
                data_payload = {
                    **data_dict_from_api,
                    "id": data_id,
                    "datasetId": dataset_id,
                    "annotation": annotation_obj
                }
                data_obj = Data.model_validate(data_payload)

                es_action = {
                    "_op_type": "update",
                    "_index": data_index_name,
                    "_id": data_obj.id,
                    "doc": data_obj.to_es_document(),
                    "doc_as_upsert": True
                }
                log.info(
                    f"Prepared ES update/upsert for data {data_obj.id} "
                    f"(msg {msg_id})."
                )
            except Exception as pydantic_e:
                log.error(
                    f"Pydantic validation/Data object creation failed for "
                    f"data_id {data_id} (msg {msg_id}): {pydantic_e}",
                    exc_info=True
                )
                dlq_payload = {
                    'original_message_id': msg_id,
                    'original_stream': current_stream_key,
                    'original_group': group_name,
                    'dataset_id': dataset_id,
                    'data_id': data_id,
                    'job_list_str': job_list_str,
                    'error_info': (
                        f"Pydantic Data model creation/validation error: "
                        f"{pydantic_e}"
                    )
                }
                return None, ack_id, dlq_payload
        else:  # data_dict_from_api is None and not is_data_delete
            log.warning(
                f"No data from API for data_id {data_id} (msg {msg_id}) "
                f"and not flagged for deletion. Cannot process."
            )
            dlq_payload = {
                'original_message_id': msg_id,
                'original_stream': current_stream_key,
                'original_group': group_name,
                'dataset_id': dataset_id,
                'data_id': data_id,
                'job_list_str': job_list_str,
                'error_info': "No data from API and not a delete operation."
            }
            return None, ack_id, dlq_payload

        return es_action, ack_id, None  # 성공 시 ES 액션, ACK ID 반환

    except Exception as processing_e:
        log.error(
            f"Error in _process_single_message for data_id {data_id} (msg {msg_id}): {processing_e}", 
            exc_info=True
        )
        dlq_payload = {
            'original_message_id': msg_id,
            'original_stream': current_stream_key,
            'original_group': group_name,
            'dataset_id': dataset_id,
            'data_id': data_id,
            'job_list_str': job_list_str,
            'error_info': processing_e
        }
        return None, ack_id, dlq_payload


# --- 처리를 위한 파이썬 함수 ---
def process_redis_stream(**context):
    log = logging.getLogger(__name__)
    log.info(
        "Starting process_redis_stream task with "
        "multithreading and batch ES."
    )

    dag_run_start_time = datetime.now()

    dag = context['dag']
    default_args = getattr(dag, 'default_args', {})
    exec_timeout_delta = default_args.get(
        'execution_timeout', timedelta(minutes=30)
    )
    dag_execution_timeout_seconds = exec_timeout_delta.total_seconds()

    polling_buffer_seconds = 180
    polling_duration_seconds = max(
        0, dag_execution_timeout_seconds - polling_buffer_seconds
    )

    if polling_duration_seconds <= 0:
        log.warning(
            f"Calculated polling duration ({polling_duration_seconds}s) "
            f"is zero or negative. Setting to fallback 10s."
        )
        polling_duration_seconds = 10

    log.info(
        f"DAG execution_timeout: {dag_execution_timeout_seconds}s. "
        f"Polling for approx {polling_duration_seconds}s."
    )

    redis_hook = RedisHook(
        redis_conn_id=REDIS_CONN_ID
    )
    redis_conn = redis_hook.get_conn()

    try:
        es_hook = ElasticsearchHook(
            elasticsearch_conn_id=ELASTICSEARCH_CONN_ID
        )
        es_client: ElasticsearchClient = es_hook.get_conn().es
    except Exception as es_hook_init_e:
        log.error(
            f"Failed to initialize ElasticsearchHook or get client: "
            f"{es_hook_init_e}", exc_info=True
        )
        raise

    data_index_name = DATA_INDEX_NAME
    if not data_index_name:
        log.error("Elasticsearch data index name not configured. Aborting.")
        raise ValueError("Elasticsearch data index name is not configured.")

    dag_run = context.get('dag_run')
    triggering_dag_run_id = dag_run.conf.get(
        'triggering_dag_run_id', 'N/A'
    ) if dag_run else 'N/A'
    log.info(f"Processor DAG run triggered by: {triggering_dag_run_id}")

    log.info(
        f"Checking stream '{STREAM_NAME}' / "
        f"group '{GROUP_NAME}' "
        f"with consumer '{CONSUMER_NAME_PROCESSOR}'"
    )

    stream_to_read = {STREAM_NAME: '>'}
    messages_processed_total = 0
    accumulated_es_actions = []
    accumulated_message_ids_to_ack = []
    last_bulk_operation_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_WORKERS_FOR_MESSAGE_PROCESSING
    ) as executor:
        try:
            while True:
                time_since_dag_start = (
                    datetime.now() - dag_run_start_time
                ).total_seconds()
                if time_since_dag_start >= polling_duration_seconds:
                    log.info("Polling duration reached. Exiting loop.")
                    break

                rem_poll_time_s = polling_duration_seconds - time_since_dag_start
                curr_block_ms = int(
                    min(max(0, rem_poll_time_s) * 1000, BLOCK_TIMEOUT_MS, 5000)
                )

                if curr_block_ms <= 0 and rem_poll_time_s <= 0:
                    log.info(
                        "Polling time exhausted before xreadgroup. Exiting."
                    )
                    break
                if rem_poll_time_s > 0 and curr_block_ms == 0:
                    curr_block_ms = 100  # Busy-loop 방지 최소 블록

                log.debug(
                    f"Polling: Rem {rem_poll_time_s:.2f}s, "
                    f"xreadgroup block_ms {curr_block_ms}"
                )

                results = redis_conn.xreadgroup(
                    groupname=GROUP_NAME,
                    consumername=CONSUMER_NAME_PROCESSOR,
                    streams=stream_to_read,
                    count=PROCESS_BATCH_SIZE,
                    block=curr_block_ms
                )

                if not results or not results[0][1]:  # 메시지 없음
                    log.debug(
                        f"No messages from xreadgroup "
                        f"(blocked for {curr_block_ms}ms)."
                    )
                    es_wait_exceeded = (
                        time.time() - last_bulk_operation_time
                    ) >= ES_BULK_MAX_WAIT_SECONDS
                    if accumulated_es_actions and es_wait_exceeded:
                        log.info(
                            f"Max wait time ({ES_BULK_MAX_WAIT_SECONDS}s) for ES "
                            f"bulk with {len(accumulated_es_actions)} actions. "
                            f"Processing."
                        )
                        try:
                            s_count, errs = es_bulk(
                                client=es_client,
                                actions=accumulated_es_actions,
                                raise_on_error=False,
                                raise_on_exception=False
                            )
                            log.info(
                                f"Timed ES Bulk: {s_count} successful, "
                                f"{len(errs)} errors."
                            )
                            if errs:
                                for i, err_info in enumerate(errs):
                                    act_type = list(err_info.keys())[0]
                                    f_action = err_info[act_type]
                                    f_doc_id = f_action.get('_id', 'N/A')
                                    err_details = f_action.get('error', {})
                                    err_reason = str(
                                        err_details.get('reason', str(err_details))
                                    )
                                    orig_idx = f_action.get('_index_in_bulk', i)
                                    log.error(
                                        f"Timed ES Bulk error for doc_id "
                                        f"{f_doc_id} (action: {act_type}, "
                                        f"original_idx: {orig_idx}): "
                                        f"{err_reason}."
                                    )
                        except Exception as bulk_e:
                            log.error(
                                f"Error during timed ES bulk: {bulk_e}",
                                exc_info=True
                            )
                        finally:
                            if accumulated_message_ids_to_ack:
                                acked_c = redis_conn.xack(
                                    STREAM_NAME,
                                    GROUP_NAME,
                                    *accumulated_message_ids_to_ack
                                )
                                log.info(
                                    f"Acked {acked_c} msgs after timed ES bulk "
                                    f"for stream {STREAM_NAME}."
                                )
                                messages_processed_total += acked_c
                            accumulated_es_actions.clear()
                            accumulated_message_ids_to_ack.clear()
                            last_bulk_operation_time = time.time()
                    continue

                current_stream_key_bytes, messages_in_batch_list = results[0]
                current_stream_key = current_stream_key_bytes.decode('utf-8')

                decoded_messages = [
                    {
                        'id': mid.decode('utf-8'),
                        'data': {
                            k.decode('utf-8'): v.decode('utf-8')
                            for k, v in mdata.items()
                        }
                    }
                    for mid, mdata in messages_in_batch_list
                ]
                log.info(
                    f"Received {len(decoded_messages)} messages from "
                    f"stream '{current_stream_key}'."
                )

                future_to_msg_id = {}
                for msg_info in decoded_messages:
                    future = executor.submit(
                        _process_single_message_in_thread,
                        msg_info,
                        data_index_name,
                        current_stream_key,
                        GROUP_NAME,
                        log
                    )
                    future_to_msg_id[future] = msg_info['id']

                for future in concurrent.futures.as_completed(future_to_msg_id):
                    msg_id_processed = future_to_msg_id[future]
                    try:
                        es_action, ack_id, dlq_info = future.result()

                        if es_action:
                            accumulated_es_actions.append(es_action)
                        if ack_id:  # ack_id는 에러 유무와 관계없이 반환됨
                            accumulated_message_ids_to_ack.append(ack_id)
                        if dlq_info:
                            send_to_dlq(
                                redis_conn,
                                dlq_info['original_message_id'],
                                dlq_info['original_stream'],
                                dlq_info['original_group'],
                                dlq_info['dataset_id'],
                                dlq_info['data_id'],
                                dlq_info['job_list_str'],
                                dlq_info['error_info'],
                                log
                            )
                    except Exception as exc:
                        log.error(
                            f"Message {msg_id_processed} generated an "
                            f"exception in thread: {exc}",
                            exc_info=True
                        )

                force_bulk = (
                    time.time() - last_bulk_operation_time
                ) >= ES_BULK_MAX_WAIT_SECONDS
                if accumulated_es_actions and (
                    len(accumulated_es_actions) >= ES_BULK_ACTIONS_THRESHOLD or
                    force_bulk
                ):
                    log.info(
                        f"Processing ES bulk actions. "
                        f"Count: {len(accumulated_es_actions)}, "
                        f"Force by time: {force_bulk}"
                    )
                    try:
                        s_count, errs = es_bulk(
                            client=es_client,
                            actions=accumulated_es_actions,
                            raise_on_error=False,
                            raise_on_exception=False
                        )
                        log.info(
                            f"ES Bulk: {s_count} successful, {len(errs)} errors."
                        )
                        if errs:
                            for i, err_info in enumerate(errs):
                                act_type = list(err_info.keys())[0]
                                f_action = err_info[act_type]
                                f_doc_id = f_action.get('_id', 'N/A')
                                err_details = f_action.get('error', {})
                                err_reason = str(
                                    err_details.get('reason', str(err_details))
                                )
                                orig_idx = f_action.get('_index_in_bulk', i)
                                log.error(
                                    f"ES Bulk error for doc_id {f_doc_id} "
                                    f"(action: {act_type}, original_idx: "
                                    f"{orig_idx}): {err_reason}."
                                )
                    except Exception as bulk_e:
                        log.error(
                            f"Error during ES bulk operation: {bulk_e}",
                            exc_info=True
                        )
                    finally:  # 벌크 성공/실패 여부와 관계없이 ACK는 시도
                        if accumulated_message_ids_to_ack:
                            acked_c = redis_conn.xack(
                                current_stream_key,
                                GROUP_NAME,
                                *accumulated_message_ids_to_ack
                            )
                            log.info(
                                f"Acked {acked_c} msgs for stream "
                                f"{current_stream_key}."
                            )
                            messages_processed_total += acked_c
                        accumulated_es_actions.clear()
                        accumulated_message_ids_to_ack.clear()
                        last_bulk_operation_time = time.time()
            
            # 루프 종료 후 남은 작업 처리
            if accumulated_es_actions:
                log.info(
                    f"Processing remaining {len(accumulated_es_actions)} "
                    f"ES actions after loop."
                )
                try:
                    s_count, errs = es_bulk(
                        client=es_client,
                        actions=accumulated_es_actions,
                        raise_on_error=False,
                        raise_on_exception=False
                    )
                    log.info(
                        f"Final ES Bulk: {s_count} successful, {len(errs)} errors."
                    )
                    if errs:  # 에러 로깅
                        for i, err_info in enumerate(errs):
                            act_type = list(err_info.keys())[0]
                            f_action = err_info[act_type]
                            f_doc_id = f_action.get('_id', 'N/A')
                            err_details = f_action.get('error', {})
                            err_reason = str(
                                err_details.get('reason', str(err_details))
                            )
                            orig_idx = f_action.get('_index_in_bulk', i)
                            log.error(
                                f"Final ES Bulk error for doc_id {f_doc_id} "
                                f"(action: {act_type}, original_idx: "
                                f"{orig_idx}): {err_reason}."
                            )
                except Exception as bulk_e:
                    log.error(
                        f"Error during final ES bulk operation: {bulk_e}",
                        exc_info=True
                    )
                finally:
                    if accumulated_message_ids_to_ack:
                        acked_c = redis_conn.xack(
                            STREAM_NAME,
                            GROUP_NAME,
                            *accumulated_message_ids_to_ack
                        )
                        log.info(
                            f"Acked {acked_c} remaining msgs "
                            f"for stream {STREAM_NAME}."
                        )
                        messages_processed_total += acked_c
                    accumulated_es_actions.clear()
                    accumulated_message_ids_to_ack.clear()

            log.info(
                f"Polling loop finished. "
                f"Total messages processed: {messages_processed_total}. "
                f"Total active polling time: "
                f"{(datetime.now() - dag_run_start_time).total_seconds():.2f}s."
            )

        except Exception as e:
            log.error(
                f"General error in process_redis_stream: {e}", exc_info=True
            )
            raise
        finally:
            log.info("Process_redis_stream task finished.")


# --- DAG 정의: 프로세서 DAG ---
with DAG(
    dag_id='redis_stream_processor_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=2,
    is_paused_upon_creation=True,
    tags=['redis', 'processor', 'elasticsearch', 'gql', 'refactored'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'depends_on_past': False,
        'execution_timeout': timedelta(minutes=30)
    }
) as dag:

    process_stream_data = PythonOperator(
        task_id='process_and_index_stream_data_refactored',
        python_callable=process_redis_stream,
    )
