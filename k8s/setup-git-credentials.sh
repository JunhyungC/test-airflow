#!/bin/bash

set -e

# 색상 정의
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
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

NAMESPACE="airflow"

log_info "Git credentials 설정을 도와드리겠습니다."
echo

# 1. SSH Key 경로 입력
log_info "Private repository 접근을 위한 SSH private key 경로를 입력해주세요."
log_warn "예: ~/.ssh/id_rsa 또는 ~/.ssh/id_ed25519"
read -p "SSH private key 경로: " SSH_KEY_PATH

# 경로 확장 (~ 처리)
SSH_KEY_PATH="${SSH_KEY_PATH/#\~/$HOME}"

# 2. SSH Key 파일 존재 확인
if [ ! -f "$SSH_KEY_PATH" ]; then
    log_error "SSH key 파일을 찾을 수 없습니다: $SSH_KEY_PATH"
    exit 1
fi

log_info "SSH key 파일을 찾았습니다: $SSH_KEY_PATH"

# 3. Namespace 생성 확인
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f - > /dev/null 2>&1

# 4. 기존 secret 삭제 (있다면)
if kubectl get secret git-credentials -n $NAMESPACE > /dev/null 2>&1; then
    log_warn "기존 git-credentials secret을 삭제합니다."
    kubectl delete secret git-credentials -n $NAMESPACE
fi

# 5. Git credentials secret 생성
log_info "Git credentials secret을 생성합니다..."
kubectl create secret generic git-credentials \
    -n $NAMESPACE \
    --from-file=ssh-private-key="$SSH_KEY_PATH"

if [ $? -eq 0 ]; then
    log_info "Git credentials secret이 성공적으로 생성되었습니다!"
else
    log_error "Git credentials secret 생성에 실패했습니다."
    exit 1
fi

# 6. Secret 확인
log_info "생성된 secret 확인:"
kubectl get secret git-credentials -n $NAMESPACE

echo
log_info "다음 단계:"
log_info "1. k8s/values.yaml 파일에서 dags.git.repository 값을 실제 Git repository URL로 설정하세요."
log_info "   예: git@github.com:username/repository.git"
log_info "2. ./k8s/deploy.sh 스크립트를 실행하여 Airflow를 배포하세요."
echo
log_warn "주의: SSH key는 해당 Git repository에 대한 읽기 권한이 있어야 합니다." 