#!/bin/bash

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

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

log_warn "Airflow Kubernetes 배포를 정리합니다."
log_warn "이 작업은 모든 Airflow 리소스와 데이터를 삭제합니다."
echo

read -p "정말로 삭제하시겠습니까? (y/N): " confirm
if [[ $confirm != [yY] ]]; then
    log_info "작업이 취소되었습니다."
    exit 0
fi

# 1. Helm release 삭제
log_info "Helm release '$RELEASE_NAME' 삭제 중..."
if helm list -n $NAMESPACE | grep -q $RELEASE_NAME; then
    helm uninstall $RELEASE_NAME -n $NAMESPACE
    log_info "Helm release가 삭제되었습니다."
else
    log_warn "Helm release '$RELEASE_NAME'을 찾을 수 없습니다."
fi

# 2. PVC 삭제 (데이터 완전 삭제)
log_warn "Persistent Volume Claims를 삭제하시겠습니까? (데이터가 완전히 삭제됩니다)"
read -p "PVC 삭제? (y/N): " confirm_pvc
if [[ $confirm_pvc == [yY] ]]; then
    log_info "PVC 삭제 중..."
    kubectl delete pvc --all -n $NAMESPACE 2>/dev/null || true
    log_info "PVC가 삭제되었습니다."
fi

# 3. Secrets 삭제
log_info "Git credentials secret 삭제 중..."
kubectl delete secret git-credentials -n $NAMESPACE 2>/dev/null || log_warn "git-credentials secret을 찾을 수 없습니다."

# 4. Namespace 삭제 여부 확인
log_warn "Namespace '$NAMESPACE'를 삭제하시겠습니까?"
read -p "Namespace 삭제? (y/N): " confirm_ns
if [[ $confirm_ns == [yY] ]]; then
    log_info "Namespace '$NAMESPACE' 삭제 중..."
    kubectl delete namespace $NAMESPACE --timeout=300s
    log_info "Namespace가 삭제되었습니다."
else
    log_info "Namespace는 유지됩니다."
fi

# 5. 정리 상태 확인
log_info "정리 작업이 완료되었습니다."
log_info ""
log_info "남은 리소스 확인:"
if kubectl get namespace $NAMESPACE > /dev/null 2>&1; then
    kubectl get all -n $NAMESPACE 2>/dev/null || log_info "Namespace '$NAMESPACE'에 리소스가 없습니다."
else
    log_info "Namespace '$NAMESPACE'가 삭제되었습니다."
fi 