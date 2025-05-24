import requests

# 업로드할 파일 경로
file_path = 'example.txt'

# Presigned URL
presigned_url = "http://minio:9000/default-workspace/01JTMPQT9CPYRP9MH66S10PTDH?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=superb_platform_user%2F20250507%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250507T062543Z&X-Amz-Expires=3600&X-Amz-Signature=f1a68fee782dc1b0fee2beba5686102647184703f02df8aed1dba28320bb9cc5&X-Amz-SignedHeaders=host"

# 파일을 읽어서 업로드 요청 전송
with open(file_path, 'rb') as f:
    response = requests.put(presigned_url, data=f)

# 결과 확인
if response.status_code == 200:
    print("✅ 업로드 성공")
else:
    print(f"❌ 업로드 실패: {response.status_code} - {response.text}")
