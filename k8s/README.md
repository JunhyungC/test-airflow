# Airflow Kubernetes 마이그레이션 가이드

이 가이드는 Docker Compose 환경의 Airflow를 Docker Desktop의 Kubernetes 클러스터로 마이그레이션하는 방법을 설명합니다.

## 🎯 마이그레이션 개요

### Docker Compose → Kubernetes 변경사항

| 항목 | Docker Compose | Kubernetes |
|------|----------------|------------|
| **Executor** | LocalExecutor | CeleryExecutor |
| **Database** | External PostgreSQL | Internal PostgreSQL (Helm Chart) |
| **Redis** | External (host.docker.internal) | Internal Redis (Helm Chart) |
| **DAGs** | Volume Mount | Git Sync (Private Repository) |
| **Utils** | Volume Mount | Git Sync (포함됨) |
| **이미지** | Custom Build | Bitnami Official (2.10.5-debian-12-r11) |

## 📋 사전 요구사항

1. **Docker Desktop**의 Kubernetes 활성화
2. **Helm** 설치 ([설치 가이드](https://helm.sh/docs/intro/install/))
3. **kubectl** 설치 및 Docker Desktop 클러스터 연결
4. **Private Git Repository** 접근을 위한 SSH Key

### 환경 확인

```bash
# Kubernetes 클러스터 연결 확인
kubectl cluster-info

# Helm 설치 확인
helm version

# Docker Desktop 컨텍스트 확인
kubectl config current-context
# 출력 예: docker-desktop
```

## 🚀 배포 과정

### 1단계: Git Repository 설정

#### Git Repository URL 설정
`values.yaml` 파일에서 다음 값을 수정하세요:

```yaml
dags:
  git:
    repository: "git@github.com:your-username/your-repository.git"  # 실제 repository URL로 변경
```

#### SSH Key 설정
Private repository 접근을 위한 SSH credentials 설정:

```bash
# Git credentials 설정 헬퍼 스크립트 실행
./setup-git-credentials.sh
```

### 2단계: Airflow 배포

```bash
# 자동 배포 스크립트 실행
./deploy.sh
```

또는 수동으로:

```bash
# Namespace 생성
kubectl create namespace airflow

# Bitnami Helm repository 추가
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Airflow 배포
helm upgrade --install airflow bitnami/airflow \
    --namespace airflow \
    --values values.yaml \
    --wait \
    --timeout 10m
```

### 3단계: 배포 확인

```bash
# Pod 상태 확인
kubectl get pods -n airflow

# 서비스 상태 확인
kubectl get svc -n airflow

# 로그 확인
kubectl logs -n airflow -l app.kubernetes.io/component=web -f
```

### 4단계: Web UI 접근

```bash
# Port forwarding
kubectl port-forward -n airflow svc/airflow 8080:8080
```

브라우저에서 `http://localhost:8080` 접속
- **Username**: admin
- **Password**: admin

## 📁 파일 구조

```
k8s/
├── values.yaml                 # Bitnami Airflow Helm Chart 설정
├── deploy.sh                   # 자동 배포 스크립트
├── setup-git-credentials.sh    # Git credentials 설정 헬퍼
├── cleanup.sh                  # 배포 정리 스크립트
└── README.md                   # 이 파일
```

## ⚙️ 주요 설정 사항

### CeleryExecutor 설정
- **Executor**: CeleryExecutor (확장성 향상)
- **Worker Replicas**: 2개 (조정 가능)
- **Redis**: 내장 Redis 사용

### Database 설정
- **PostgreSQL**: Helm Chart 내장 사용
- **Persistence**: 8Gi 볼륨
- **자동 백업**: 미설정 (필요시 추가 구성)

### Git Sync 설정
- **Repository**: Private repository 지원
- **Sync Interval**: 60초
- **SubPath**: `src` (dags, utils 포함)
- **Authentication**: SSH Key 기반

### 리소스 설정
```yaml
# 각 컴포넌트별 리소스 할당
web:        500m CPU, 512Mi Memory (요청) / 1000m CPU, 1Gi Memory (제한)
scheduler:  500m CPU, 512Mi Memory (요청) / 1000m CPU, 1Gi Memory (제한)  
worker:     500m CPU, 512Mi Memory (요청) / 1000m CPU, 1Gi Memory (제한)
triggerer:  250m CPU, 256Mi Memory (요청) / 500m CPU, 512Mi Memory (제한)
```

## 🔧 커스터마이징

### 환경변수 추가
`values.yaml`의 `extraEnvVars` 섹션에 추가:

```yaml
extraEnvVars:
  - name: YOUR_CUSTOM_VAR
    value: "your_value"
```

### Python 패키지 추가
`values.yaml`의 `extraPackages` 섹션에 추가:

```yaml
extraPackages: |
  your-package==1.0.0
  another-package
```

### Worker 수 조정
```yaml
worker:
  replicaCount: 3  # 원하는 worker 수로 변경
```

## 🛠️ 트러블슈팅

### Git Sync 문제
```bash
# Git sync 컨테이너 로그 확인
kubectl logs -n airflow -l app.kubernetes.io/component=git-sync -f

# SSH key 권한 확인
kubectl get secret git-credentials -n airflow -o yaml
```

### Database 연결 문제
```bash
# PostgreSQL 상태 확인
kubectl get pods -n airflow -l app.kubernetes.io/component=postgresql

# Database 연결 테스트
kubectl exec -it -n airflow deployment/airflow-postgresql -- psql -U airflow -d airflow -c "SELECT 1;"
```

### Worker 문제
```bash
# Worker 로그 확인
kubectl logs -n airflow -l app.kubernetes.io/component=worker -f

# Redis 연결 확인
kubectl exec -it -n airflow deployment/airflow-redis-master -- redis-cli ping
```

## 🗑️ 정리

전체 배포를 제거하려면:

```bash
./cleanup.sh
```

## 📚 추가 자료

- [Bitnami Airflow Chart 문서](https://github.com/bitnami/charts/tree/main/bitnami/airflow)
- [Apache Airflow 공식 문서](https://airflow.apache.org/docs/)
- [Kubernetes 공식 문서](https://kubernetes.io/docs/)
- [Helm 공식 문서](https://helm.sh/docs/)

## ⚠️ 주의사항

1. **데이터 백업**: 중요한 데이터는 배포 전에 백업하세요
2. **리소스 모니터링**: 클러스터 리소스 사용량을 모니터링하세요
3. **보안**: Production 환경에서는 적절한 보안 설정을 추가하세요
4. **Git Repository**: Private repository의 SSH key는 읽기 전용 권한만 부여하세요 