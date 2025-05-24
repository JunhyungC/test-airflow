#!/bin/bash
set -e

# 데이터베이스 연결 확인 함수 (nc 사용)
wait_for_db() {
  echo "데이터베이스 연결 확인 중..."
  local max_attempts=30
  local wait_seconds=10
  local attempt=0
  
  # nc 명령어로 포트 연결 확인
  while ! nc -z db 15432; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
      echo "데이터베이스 연결 실패: 최대 시도 횟수 초과"
      exit 1
    fi
    echo "데이터베이스 연결 대기 중... ($attempt/$max_attempts)"
    sleep $wait_seconds
  done
  
  echo "데이터베이스 연결 성공. 추가 대기..."
  # 데이터베이스가 실제로 쿼리를 처리할 준비가 될 때까지 추가 대기
  sleep 10
  echo "데이터베이스 연결 준비 완료"
}

# 디렉토리 생성 및 권한 설정
echo "디렉토리 권한 설정 중..."
mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins
chmod -R 777 /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# 1. 데이터베이스 연결 확인
wait_for_db

# 2. 데이터베이스 초기화 시도
echo "데이터베이스 초기화 중..."
airflow db init

# 3. 데이터베이스 상태 확인
if airflow db check; then
  echo "데이터베이스 초기화 성공"
else
  echo "데이터베이스 초기화 실패. 5초 후 재시도..."
  sleep 5
  
  echo "데이터베이스 재초기화 시도..."
  airflow db init
  
  if ! airflow db check; then
    echo "데이터베이스 초기화 실패"
    exit 1
  fi
fi

# 4. Admin 사용자 생성
echo "Admin 사용자 확인 중..."
if ! airflow users list 2>/dev/null | grep -q "admin"; then
  echo "Admin 사용자 생성 중..."
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
fi

# 5. 원래 Airflow 명령 실행
echo "Airflow 시작: $@"
exec airflow "$@" 