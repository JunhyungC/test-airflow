from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context

# 플러그인 디렉토리 구조에 따라 트리거 경로가 올바른지 확인하세요
from utils.triggers.redis_trigger import RedisStreamTrigger


class RedisStreamSensorAsync(BaseSensorOperator):
    """
    지연 가능 모드를 사용하여 Redis 스트림 그룹에 데이터가 나타나기를 기다립니다.
    메시지 소비를 피하기 위해 트리거에서 XINFO 확인을 사용합니다.

    :param redis_conn_id: 사용할 Redis 연결 ID.
    :param stream_name: Redis 스트림의 이름.
    :param group_name: 소비자 그룹의 이름.
    :param trigger_check_interval_sec: 트리거가 확인 사이에 대기할 시간(초).
    """

    # 사용할 트리거 정의
    deferrable = True

    @apply_defaults
    def __init__(
        self,
        redis_conn_id: str,
        stream_name: str,
        group_name: str,
        trigger_check_interval_sec: int = 5,
        **kwargs,
    ):
        # kwargs에 있는 경우 센서별 폴링 매개변수 제거
        kwargs.pop("poke_interval", None)
        kwargs.pop("timeout", None)
        kwargs.pop("mode", None)
        # 트리거에서 더 이상 사용되지 않는 매개변수 제거
        kwargs.pop("consumer_name", None)  # kwargs를 통해 전달된 경우
        kwargs.pop("trigger_block_msec", None)  # kwargs를 통해 전달된 경우
        super().__init__(**kwargs)

        self.redis_conn_id = redis_conn_id
        self.stream_name = stream_name
        self.group_name = group_name
        self.trigger_check_interval_sec = trigger_check_interval_sec
        # 고유 XCom 키 생성
        self.xcom_key = (
            f"last_fired_lgid_{self.dag_id}_{self.task_id}"
        )
        # 잠재적 호환성을 위해 유지되었지만 사용되지 않는 속성들
        # self.consumer_name = consumer_name
        # self.trigger_block_msec = trigger_block_msec

    def execute(self, context: Context) -> None:
        """트리거로 실행을 전달합니다."""
        ti = context["ti"]
        previously_fired_lgid = ti.xcom_pull(
            task_ids=self.task_id, key=self.xcom_key
        )
        
        self.log.info(
            f"Attempting to defer using Redis conn ID: "
            f"'{self.redis_conn_id}'. "
            f"Previously fired LGID from XCom: {previously_fired_lgid}"
        )
        self.defer(
            trigger=RedisStreamTrigger(
                redis_conn_id=self.redis_conn_id,
                stream_name=self.stream_name,
                group_name=self.group_name,
                check_interval_sec=self.trigger_check_interval_sec,
                previously_fired_lgid=previously_fired_lgid,  # XCom 값 전달
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: Context, event: Optional[Dict[str, Any]] = None
    ) -> None:
        """트리거가 발생할 때의 콜백입니다."""
        if event is None:
            # 제대로 구현된 트리거에서는 이런 일이 발생하지 않아야 하지만,
            # 방어적으로 처리합니다.
            raise AirflowException("Trigger returned without an event.")

        status = event.get("status")
        message = event.get("message")

        if status == "success":
            self.log.info(
                f"Successfully detected data in stream "
                f"'{self.stream_name}': {message}"
            )
            newly_fired_lgid = event.get("last_generated_id")
            if newly_fired_lgid:
                ti = context["ti"]
                ti.xcom_push(key=self.xcom_key, value=newly_fired_lgid)
                self.log.info(
                    f"Pushed newly_fired_lgid='{newly_fired_lgid}' to XCom "
                    f"with key '{self.xcom_key}'."
                )
            else:
                self.log.warning(
                    "Trigger event SUCCESS but did not contain "
                    "'last_generated_id'. XCom not updated."
                )
            return
        elif status == "error":
            raise AirflowException(f"Error checking Redis stream: {message}")
        else:
            # 예상치 못한 상태 처리
            raise AirflowException(
                f"Received unexpected status from trigger: {status}"
            ) 