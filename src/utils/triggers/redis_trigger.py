import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Tuple, Optional

import redis.asyncio as redis
from redis.exceptions import RedisError, ResponseError

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

# 로거 초기화
log = logging.getLogger(__name__)


class RedisStreamTrigger(BaseTrigger):
    """
    XINFO 명령을 사용하여 스트림의 마지막 생성 ID와 그룹의 마지막 전달 ID를 비교하여
    메시지 소비 없이 Redis 스트림 그룹에 데이터가 나타나기를 기다리는 트리거입니다.

    :param redis_conn_id: 사용할 Redis 연결 ID.
    :param stream_name: Redis 스트림의 이름.
    :param group_name: 소비자 그룹의 이름.
    :param check_interval_sec: 확인 사이에 대기할 시간(초).
    :param previously_fired_lgid: 이전에 감지된 메시지 ID (상태 복원용).
    """

    def __init__(
        self,
        redis_conn_id: str,
        stream_name: str,
        group_name: str,
        check_interval_sec: int = 5,  # 체크 사이에 기본 5초 대기
        previously_fired_lgid: Optional[str] = None,
        **kwargs  # 잠재적인 미래 BaseTrigger 인자를 위해 유지
    ):
        super().__init__(**kwargs)
        self.redis_conn_id = redis_conn_id
        self.stream_name = stream_name
        self.group_name = group_name
        self.check_interval_sec = check_interval_sec
        self.previously_fired_lgid = previously_fired_lgid

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """트리거 인자와 클래스 경로를 직렬화합니다."""
        return (
            "utils.triggers.redis_trigger.RedisStreamTrigger",
            {
                "redis_conn_id": self.redis_conn_id,
                "stream_name": self.stream_name,
                "group_name": self.group_name,
                "check_interval_sec": self.check_interval_sec,
                "previously_fired_lgid": self.previously_fired_lgid,
            },
        )

    async def _get_redis_client(self) -> redis.Redis:
        """비동기 Redis 연결을 설정합니다."""
        redis_hook = RedisHook(redis_conn_id=self.redis_conn_id)
        conn_params = redis_hook.get_connection(self.redis_conn_id)
        host = conn_params.host or 'localhost'
        port = conn_params.port or 6379
        db = conn_params.extra_dejson.get('db', 0)
        log.info(
            f"Trigger interpreted Redis connection '{self.redis_conn_id}' as: "
            f"host={host}, port={port}, db={db}"
        )

        redis_kwargs = {
            'host': host,
            'port': port,
            'db': db,
            'decode_responses': True  # ID와 정보 결과를 쉽게 처리하기 위해 응답 디코딩
        }
        if conn_params.password:
            redis_kwargs['password'] = conn_params.password

        redis_client = redis.Redis(**redis_kwargs)
        await redis_client.ping()  # 연결 확인
        log.info("Successfully connected to Redis.")
        return redis_client

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        주요 로직: Redis에 연결하고 XINFO STREAM과 XINFO GROUPS를 사용하여
        주기적으로 새 데이터를 확인합니다.
        """
        redis_client = None
        try:
            redis_client = await self._get_redis_client()
            while True:
                # 1. 스트림 길이 (XLEN) 확인
                try:
                    current_stream_length = await redis_client.xlen(self.stream_name)
                except ResponseError as e:
                    # 스트림이 존재하지 않는 경우 xlen은 오류를 발생시킴
                    if "no such key" in str(e).lower():
                        log.warning(
                            f"Stream '{self.stream_name}' does not exist "
                            f"(checked by XLEN). Waiting "
                            f"{self.check_interval_sec}s..."
                        )
                        await asyncio.sleep(self.check_interval_sec)
                        continue
                    else:
                        # 다른 Redis 오류는 예외 발생
                        raise

                if current_stream_length == 0:
                    log.debug(
                        f"Stream '{self.stream_name}' is currently empty (XLEN=0). "
                        f"Waiting {self.check_interval_sec}s..."
                    )
                    await asyncio.sleep(self.check_interval_sec)
                    continue  # 스트림에 메시지가 없으면 다음 체크까지 대기

                # 스트림에 메시지가 있는 경우에만 다음 로직 수행
                log.debug(
                    f"Stream '{self.stream_name}' has {current_stream_length} messages. Checking for group '{self.group_name}'. "
                    f"Previously fired LGID: {self.previously_fired_lgid}"
                )
                try:
                    # 2. 스트림 정보 확인 (기존 로직)
                    stream_info = await redis_client.xinfo_stream(self.stream_name)
                    last_generated_id = stream_info.get('last-generated-id')

                    if not last_generated_id:
                        log.warning(
                            f"Stream '{self.stream_name}' exists (XLEN={current_stream_length}) but couldn't get "
                            f"last-generated-id. Retrying in {self.check_interval_sec}s..."
                        )
                        await asyncio.sleep(self.check_interval_sec)
                        continue

                    # 3. 그룹 정보 확인 (기존 로직)
                    group_info_list = await redis_client.xinfo_groups(self.stream_name)
                    target_group_info = next(
                        (g for g in group_info_list
                         if g.get('name') == self.group_name),
                        None
                    )

                    if not target_group_info:
                        log.warning(
                            f"Could not find consumer group '{self.group_name}' "
                            f"for stream '{self.stream_name}'. "
                            f"Assuming no new data. "
                            f"Retrying in {self.check_interval_sec} seconds..."
                        )
                        # 프로세서 DAG가 그룹을 생성해야 합니다. 대기 후 재시도.
                        await asyncio.sleep(self.check_interval_sec)
                        continue

                    last_delivered_id = target_group_info.get('last-delivered-id', '0-0')

                    # 4. ID 비교
                    # Redis ID는 사전적/시간적으로 비교 가능합니다
                    group_has_new_data_to_process = last_generated_id > last_delivered_id

                    # Redis ID 비교를 위한 함수 (타임스탬프 부분과 시퀀스 부분으로 분리하여 비교)
                    def parse_redis_id(redis_id: str) -> Tuple[int, int]:
                        parts = redis_id.split('-')
                        return int(parts[0]), int(parts[1])

                    if self.previously_fired_lgid is None:
                        # 이전에 발생한 LGID가 없으면, 그룹에 처리할 데이터가 있는 경우 항상 새로운 것으로 간주
                        is_newer_than_previously_fired = True
                    else:
                        # 이전에 발생한 LGID가 있으면, 현재 LGID와 비교
                        is_newer_than_previously_fired = parse_redis_id(last_generated_id) > parse_redis_id(self.previously_fired_lgid)

                    if group_has_new_data_to_process and is_newer_than_previously_fired:
                        log.info(
                            f"New data detected. LGID: {last_generated_id} (previously fired: {self.previously_fired_lgid}), "
                            f"Group LDID: {last_delivered_id}. Firing event."
                        )
                        yield TriggerEvent({
                            "status": "success",
                            "message": f"New data {last_generated_id} available for group {self.group_name}",
                            "last_generated_id": last_generated_id
                        })
                        return
                    else:
                        log_msg_parts = []
                        if not group_has_new_data_to_process:
                            log_msg_parts.append(
                                f"No new data for group (LGID: {last_generated_id} <= LDID: {last_delivered_id})."
                            )
                        if not is_newer_than_previously_fired and group_has_new_data_to_process:
                            log_msg_parts.append(
                                f"Data {last_generated_id} already processed by sensor or not newer than {self.previously_fired_lgid}."
                            )
                        if not log_msg_parts:
                            log_msg_parts.append(
                                f"No new data for sensor (LGID: {last_generated_id}, Prev_LGID: {self.previously_fired_lgid}, LDID: {last_delivered_id})."
                            )

                        log.debug(f"{' '.join(log_msg_parts)} Waiting {self.check_interval_sec}s...")
                        await asyncio.sleep(self.check_interval_sec)

                except ResponseError as e:
                    # 스트림이 존재하지 않는지 특정 검사 (NOGROUP 오류는 XREADGROUP용)
                    # XINFO STREAM은 ResponseError: 'ERR no such key'를 발생시킵니다
                    if "no such key" in str(e).lower():
                        log.warning(
                            f"Stream '{self.stream_name}' does not exist yet. "
                            f"Waiting {self.check_interval_sec} seconds..."
                        )
                        await asyncio.sleep(self.check_interval_sec)
                        continue  # 스트림이 생성될 때까지 대기
                    else:
                        # 예상치 못한 ResponseError 재발생
                        raise
                except RedisError as e:
                    log.error(f"Redis error checking stream info: {e}")
                    yield TriggerEvent({"status": "error", "message": str(e)})
                    return  # 예상치 못한 Redis 오류 시 반복 중지

        except Exception as e:
            log.exception(f"Exception in RedisStreamTrigger: {e}")
            yield TriggerEvent({"status": "error", "message": str(e)})
        finally:
            if redis_client:
                await redis_client.close()
                log.info("Redis client connection closed.")
