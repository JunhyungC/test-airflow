#!/bin/bash

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 함수 정의
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 변수 설정
NAMESPACE="airflow"
RELEASE_NAME="airflow"
CHART_REPO="oci://registry-1.docker.io/bitnamicharts"
CHART_NAME="airflow"

log_info "Airflow Kubernetes 배포를 시작합니다..."

# 1. Docker Desktop Kubernetes 클러스터 확인
log_info "Kubernetes 클러스터 연결 확인 중..."
if ! kubectl cluster-info > /dev/null 2>&1; then
    log_error "Kubernetes 클러스터에 연결할 수 없습니다. Docker Desktop의 Kubernetes가 활성화되어 있는지 확인해주세요."
    exit 1
fi

CURRENT_CONTEXT=$(kubectl config current-context)
log_info "현재 Kubernetes 컨텍스트: $CURRENT_CONTEXT"

# 2. Namespace 생성
log_info "Namespace '$NAMESPACE' 생성 중..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# 3. Helm 설치 확인
log_info "Helm 설치 확인 중..."
if ! command -v helm &> /dev/null; then
    log_error "Helm이 설치되어 있지 않습니다. https://helm.sh/docs/intro/install/ 에서 설치해주세요."
    exit 1
fi

# 4. Bitnami Helm repository 추가
log_info "Bitnami Helm repository 추가 중..."
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# 5. Git credentials secret 생성 확인
log_warn "Git credentials secret 설정이 필요합니다."
log_warn "Private repository 접근을 위해 다음 명령어를 실행해주세요:"
log_warn "kubectl create secret generic git-credentials -n $NAMESPACE --from-file=ssh-private-key=/path/to/your/private/key"
log_warn ""
read -p "Git credentials secret을 이미 생성했나요? (y/N): " confirm
if [[ $confirm != [yY] ]]; then
    log_warn "Git credentials secret을 먼저 생성한 후 다시 실행해주세요."
    exit 1
fi

# 6. values.yaml 파일에서 repository URL 확인
log_warn "values.yaml 파일에서 Git repository URL을 설정했는지 확인해주세요."
read -p "Git repository URL을 설정했나요? (y/N): " confirm_repo
if [[ $confirm_repo != [yY] ]]; then
    log_warn "values.yaml 파일에서 dags.git.repository 값을 설정한 후 다시 실행해주세요."
    exit 1
fi

# 7. Airflow 배포
log_info "Airflow 배포 중..."
helm upgrade --install $RELEASE_NAME bitnami/airflow \
    --namespace $NAMESPACE \
    --values values.yaml \
    --wait \
    --timeout 10m

# 8. 배포 상태 확인
log_info "배포 상태 확인 중..."
kubectl get pods -n $NAMESPACE

# 9. 서비스 상태 확인
log_info "서비스 상태 확인 중..."
kubectl get svc -n $NAMESPACE

# 10. Airflow Web UI 접근 안내
log_info "배포가 완료되었습니다!"
log_info ""
log_info "Airflow Web UI에 접근하려면 다음 명령어를 실행하세요:"
log_info "kubectl port-forward -n $NAMESPACE svc/airflow 8080:8080"
log_info ""
log_info "그 후 브라우저에서 http://localhost:8080 으로 접속하세요."
log_info "로그인 정보: admin / admin"
log_info ""
log_info "로그 확인:"
log_info "kubectl logs -n $NAMESPACE -l app.kubernetes.io/component=web -f"

log_info "배포 스크립트가 완료되었습니다." 