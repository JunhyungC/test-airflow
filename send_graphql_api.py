import requests

# GraphQL endpoint URL
url = "http://app-api:8080/system/graphql"

# GraphQL 쿼리
query = """
query AutoCuration {
  autoCuration(
    datasetId: "01JKF2Z8GESGS8H2TR7YHQV0ND"
    id: "01JM206DKT7YG98ZNXBCE1ZF13"
  ) {
    id
    type
    method
    clusterScatterContents {
      annotationClusterContents {
        id
      }
    }
  }
}
"""

# 요청 payload
payload = {
    "query": query
}

# 필요 시 인증 토큰 추가
headers = {
    "Content-Type": "application/json",
    # "Authorization": "Bearer YOUR_ACCESS_TOKEN"
}

# POST 요청
response = requests.post(url, json=payload, headers=headers)

# 응답 출력
if response.status_code == 200:
    print("응답 데이터:")
    print(response.json())
else:
    print(f"요청 실패: {response.status_code}")
    print(response.text)
