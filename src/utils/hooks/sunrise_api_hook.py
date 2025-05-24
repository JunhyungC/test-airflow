import requests
import random
import json
from io import BytesIO
from typing import Optional, Dict, Any, List
import time

from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class RetryWithJitter(Retry):
    """
    Custom Retry class that adds jitter to the backoff time.
    Inherits from urllib3's Retry class.
    """
    def __init__(self, *args, **kwargs):
        """
        Initialize RetryWithJitter with the same parameters as Retry.
        Add retry_count attribute to track the current retry count.
        """
        super().__init__(*args, **kwargs)
        self.retry_count = 0

    def increment(self, *args, **kwargs):
        """
        Override increment method to update retry_count when a retry occurs.
        """
        result = super().increment(*args, **kwargs)
        if result:
            if hasattr(result, 'retry_count'):
                self.retry_count = result.retry_count
            else:
                self.retry_count = 0
        return result

    def get_backoff_time(self):
        """
        Calculates backoff time with jitter.
        Uses the base backoff time and adds a random jitter component.
        """
        # 부모 클래스의 backoff time 계산 
        backoff = super().get_backoff_time()
        
        if backoff == 0:
            return 0
            
        # 추가 jitter 적용
        jitter = backoff * random.uniform(0.5, 1.5)
        backoff_with_jitter = (
            min(jitter, self.BACKOFF_MAX)
            if hasattr(self, 'BACKOFF_MAX')
            else jitter
        )
        return backoff_with_jitter


class SunriseApiHook(BaseHook):
    """
    Interacts with the Sunrise GraphQL API.

    :param http_conn_id: The Airflow HTTP connection ID for API requests.
                         The connection's 'host' should be API endpoint URL.
    """
    conn_name_attr = 'http_conn_id'
    default_conn_name = 'sunrise_api_default'
    conn_type = 'http'
    hook_name = 'Sunrise API'

    def __init__(
        self,
        http_conn_id: str = default_conn_name,
        retry_params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.base_url = self._get_base_url()
        
        default_retry_params = {
            'retries': 5,  # HTTP level retries
            'backoff_factor': 0.5,  # HTTP level backoff factor for urllib3
            'status_forcelist': (
                403, 404, 500, 502, 503, 504
            ), # 404 포함
            'allowed_methods': frozenset([
                'GET', 'POST', 'PUT', 'DELETE', 'OPTIONS',
                'HEAD', 'PATCH', 'TRACE', 'CONNECT'
            ]),
            'gql_content_retries': 3,  # Retries for specific GQL content errors on HTTP 200
            'gql_content_initial_delay_seconds': 1.0  # Initial delay for GQL content retries
        }
        
        # 사용자 정의 retry_params가 제공되면 기본값을 업데이트합니다.
        if retry_params:
            default_retry_params.update(retry_params)
        self.retry_params = default_retry_params
        
        self.session: Optional[requests.Session] = None

    def _get_base_url(self) -> str:
        """
        Retrieves the base URL from the HTTP connection.
        Connection URI typically: 'http://user:pass@host:port/path'
        We need to extract just the scheme and host part.
        """
        conn = self.get_connection(self.http_conn_id)
        
        # 기본 스키마 정의
        scheme = 'http'
        
        # 호스트 정보 추출
        host = conn.host or ''
        port = conn.port
        
        # conn.host가 URL 형식인 경우 (예: http://:@app-api:8080/system/graphql)
        if '://' in host:
            # URL에서 스키마 부분 추출
            parts = host.split('://', 1)
            scheme = parts[0]
            
            # 나머지 부분에서 호스트 이름 추출 (사용자 정보 및 경로 제외)
            remainder = parts[1]
            
            # @가 있으면 사용자 정보 부분 제거
            if '@' in remainder:
                remainder = remainder.split('@', 1)[1]
            
            # 경로 부분 제거
            if '/' in remainder:
                host = remainder.split('/', 1)[0]
            else:
                host = remainder
        
        # 포트가 호스트에 이미 포함되어 있지 않고, conn.port에 설정된 경우
        if port and ':' not in host:
            host = f"{host}:{port}"
        
        base_url = f"{scheme}://{host}"
        self.log.debug("Constructed base URL: %s", base_url)
        return base_url

    def get_conn(self) -> requests.Session:
        """Returns a requests session with retry logic."""
        if self.session:
            return self.session

        session = requests.Session()
        conn = self.get_connection(self.http_conn_id)

        headers = {}
        if conn.extra:
            try:
                extra_options = conn.extra_dejson
                if 'headers' in extra_options and \
                   isinstance(extra_options['headers'], dict):
                    headers.update(extra_options['headers'])
            except json.JSONDecodeError:
                self.log.warning(
                    "Could not decode extra_dejson for connection %s",
                    self.http_conn_id
                )

        headers.setdefault('Accept', 'application/json')
        headers.setdefault('Content-Type', 'application/json')
        session.headers.update(headers)

        if conn.login:
            session.auth = (conn.login, conn.password or '')

        retry_strategy = RetryWithJitter(
            total=self.retry_params['retries'],
            read=self.retry_params['retries'],
            connect=self.retry_params['retries'],
            backoff_factor=self.retry_params['backoff_factor'],
            status_forcelist=frozenset(self.retry_params['status_forcelist']),
            allowed_methods=self.retry_params['allowed_methods'],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_maxsize=32,
            pool_block=True
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        self.session = session
        return self.session

    def run_gql(
        self,
        query_name: str,
        query: str,
        variables: dict,
        **request_kwargs: Any
    ) -> Dict[str, Any]:
        session = self.get_conn()
        payload = {
            "query": query,
            "variables": variables
        }
        self.log.info(
            "Executing GraphQL query: %s with variables: %s",
            query_name,
            # 변수 로깅 시 민감 정보나 크기가 큰 내용을 주의하여 요약 로깅 고려
            json.dumps(variables, indent=2)
        )

        graphql_endpoint = "/system/graphql"
        api_url = f"{self.base_url}{graphql_endpoint}"
        self.log.debug("API URL: %s", api_url)

        max_content_retries = self.retry_params.get('gql_content_retries', 3)
        initial_delay_seconds = self.retry_params.get(
            'gql_content_initial_delay_seconds', 1.0
        )
        last_graphql_errors_for_retry_path = None

        for attempt in range(max_content_retries + 1):
            try:
                response = session.post(api_url, json=payload, **request_kwargs)
                response.raise_for_status()  # HTTP 오류 발생 시 (세션 재시도 후) 예외 발생
            except requests.exceptions.RequestException as e:
                self.log.error(
                    "GraphQL request for '%s' failed due to HTTP error (after session retries): %s",
                    query_name, str(e)
                )
                raise AirflowException(f"API request for '{query_name}' failed: {e}") from e

            try:
                result = response.json()
            except json.JSONDecodeError as e:
                current_response_text = response.text if response else "No response text available"
                self.log.error(
                    "Failed to decode JSON response for '%s': %s. Response text: %s",
                    query_name, str(e), current_response_text[:500]  # 첫 500자만 로깅
                )
                raise AirflowException(
                    f"Failed to decode JSON response for '{query_name}': {e}"
                ) from e

            graphql_errors = result.get('errors', [])
            last_graphql_errors_for_retry_path = graphql_errors # 마지막 시도의 에러 저장

            if response.status_code == 200 and graphql_errors:
                should_retry_gql_content_error = False
                for err in graphql_errors:
                    err_message = err.get("message", "")
                    gql_err_code_top = err.get("code")
                    gql_err_code_ext = err.get("extensions", {}).get("code")

                    is_not_found_error = (
                        (isinstance(err_message, str) and
                         "Data not found" in err_message) or
                        gql_err_code_top == "NOT_FOUND" or
                        gql_err_code_ext == "NOT_FOUND"
                    )

                    if is_not_found_error:
                        self.log.warning(
                            f"GraphQL query '{query_name}' (HTTP 200) returned 'NOT_FOUND' type error. "
                            f"Details: {json.dumps(err)}. Attempt {attempt + 1} of {max_content_retries + 1}."
                        )
                        should_retry_gql_content_error = True
                        break 
                
                if should_retry_gql_content_error:
                    if attempt < max_content_retries:
                        delay = initial_delay_seconds * (2 ** attempt)  # Exponential backoff
                        self.log.info(
                            f"Retrying '{query_name}' for GQL content error after {delay:.2f} seconds..."
                        )
                        time.sleep(delay)
                        continue  # 현재 run_gql 함수의 재시도 루프 계속
                    else:
                        # 내용 기반 재시도 모두 소진
                        self.log.error(
                            f"Max GQL content retries ({max_content_retries}) reached for '{query_name}' "
                            f"due to persistent 'NOT_FOUND' type errors on HTTP 200."
                        )
                        # 에러는 아래의 `if graphql_errors:` 블록에서 처리됨
            
            # 정상 처리되었거나, 재시도 대상이 아닌 GraphQL 에러가 있거나, 내용 기반 재시도가 모두 소진된 경우
            if graphql_errors:
                error_message = json.dumps(graphql_errors)
                self.log.error(
                    f"GraphQL query '{query_name}' resulted in errors (or content retries exhausted): {error_message}"
                )
                raise AirflowException(f"GraphQL query '{query_name}' failed: {error_message}")

            # GraphQL 에러 없이 성공한 경우
            gql_response_main_data_field = result.get('data')

            if gql_response_main_data_field is None:
                # 응답이 `{"data": null}` 이고 GraphQL 에러가 없는 경우
                if query_name == "data": # get_data 와 같은 경우, 실제 데이터 엔티티가 null 이면 문제
                    self.log.error(
                        f"GraphQL query '{query_name}' (expected to return data entity) "
                        f"returned null in 'data' field without any GraphQL errors. Treating as data not found."
                    )
                    raise AirflowException(
                        f"Data for query '{query_name}' is null in response without GraphQL errors, indicating data not found or unexpected response."
                    )
                # 다른 뮤테이션 등에서는 'data': null 이 정상일 수 있음
                return None 

            if not isinstance(gql_response_main_data_field, dict):
                err_msg = (
                    f"The 'data' field in GraphQL response is not a dictionary. "
                    f"Type: {type(gql_response_main_data_field)}. "
                    f"Value: {str(gql_response_main_data_field)[:200]}"
                )
                self.log.error(err_msg)
                raise AirflowException(err_msg)

            # gql_response_main_data_field는 이제 딕셔너리임 (예: `{"data": {"id": ...}}` 에서 `{"id": ...}` 부분)
            actual_data_entity = gql_response_main_data_field.get(query_name)

            if actual_data_entity is None and query_name == "data":
                # 예: `{"data": {"data": null}}` 이고 GraphQL 에러가 없는 경우
                # `get_data` 호출 시, 이는 실제 데이터가 없음을 의미
                log_msg = (
                    f"GraphQL query '{query_name}' for data entity resolved to null "
                    f"within the 'data' field, without GraphQL errors. "
                    f"Treating as data not found."
                )
                self.log.error(log_msg)
                raise AirflowException(
                    f"Data for query '{query_name}' resolved to null, without GraphQL errors."
                )
            
            # `get_data` 와 같은 경우, `actual_data_entity` (즉, `response.json()['data']['data']`)는 딕셔너리여야 함
            if query_name == "data" and not isinstance(actual_data_entity, dict) and actual_data_entity is not None:
                 self.log.error(
                    f"Data for query '{query_name}' is not a dictionary as expected for data entity. "
                    f"Type: {type(actual_data_entity)}. Value: {str(actual_data_entity)[:200]}"
                 )
                 raise AirflowException(
                     f"Data for query '{query_name}' is not a dictionary: {type(actual_data_entity)}"
                 )

            return actual_data_entity

        # 루프가 모든 재시도 후 (내용 기반) 실패하여 여기까지 도달한 경우
        # (이론적으로는 루프 내에서 예외가 발생해야 함)
        final_error_message = f"GraphQL query '{query_name}' ultimately failed after {max_content_retries + 1} attempts for content issues."
        if last_graphql_errors_for_retry_path:
             final_error_message += (
                 f" Last GraphQL errors: {json.dumps(last_graphql_errors_for_retry_path)}"
             )
        self.log.error(final_error_message)  # 추가 로깅
        raise AirflowException(final_error_message)

    # --- Methods based on user-provided GraphQL queries ---

    def get_data(
        self,
        dataset_id: str,
        data_id: str
    ) -> Dict[str, Any]:
        """Get data by id using GraphQL."""
        query = '''
            query GetData($dataset_id: String!, $id: ID!) {
                data(datasetId: $dataset_id, id: $id) {
                    id
                    datasetId
                    sliceIds
                    key
                    type
                    scene {
                        id
                        type
                        content {
                            id
                        }
                        meta
                    }
                    thumbnail {
                        id
                    }
                    annotation {
                        meta
                        versions {
                            content {
                                id
                            }
                            id
                            meta
                        }
                    }
                    predictions {
                        setId
                        meta
                        content {
                            id
                        }
                    }
                    meta {
                        key
                        type
                        value
                    }
                    systemMeta {
                        key
                        type
                        value
                    }
                    createdAt
                    createdBy
                    updatedAt
                    updatedBy
                }
            }
        '''
        variables = {
            "dataset_id": dataset_id,
            "id": data_id,
        }
        self.log.info(f"Executing get_data for data_id: {data_id} in dataset: {dataset_id}")
        return self.run_gql("data", query, variables)

    def generate_content_download_url(self, content_id: str) -> Optional[str]:
        """Generate Content Download URL using GraphQL."""
        query = '''
            mutation GenerateContentDownloadURL($id: ID!) {
                generateContentDownloadURL(id: $id)
            }
        '''
        variables = {"id": content_id}
        self.log.info(f"Executing generate_content_download_url for content_id: {content_id}")
        response_data = self.run_gql("generateContentDownloadURL", query, variables)
        if isinstance(response_data, str):
            return response_data
        self.log.warning(
            f"generateContentDownloadURL returned type {type(response_data)}. Expected str. Response: {str(response_data)[:200]}"
        )
        return response_data if isinstance(response_data, str) else None

    def update_data_meta(
        self,
        dataset_id: str,
        data_id: str,
        meta: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Update data meta (without stream) using GraphQL."""
        query = '''
            mutation UpdateDataWithoutStream(
                $dataset_id: ID!,
                $id: ID!,
                $meta: [DataMetaInput!]
            ) {
                updateDataWithoutStream(
                    datasetId: $dataset_id,
                    id: $id,
                    meta: $meta
                ) {
                    datasetId
                    id
                    key
                    meta {
                        key
                        type
                        value
                    }
                    systemMeta {
                        key
                        type
                        value
                    }
                }
            }
        '''
        variables = {
            "dataset_id": dataset_id,
            "id": data_id,
            "meta": meta,
        }
        self.log.info(f"Executing update_data_meta for data_id: {data_id}")
        return self.run_gql("updateDataWithoutStream", query, variables)

    def delete_data(
        self,
        dataset_id: str,
        data_id: str,
    ) -> Optional[bool]:
        """Delete data using GraphQL."""
        query = '''
            mutation DeleteData($datasetId: ID!, $deleteDataId: ID!) {
                deleteData(datasetId: $datasetId, id: $deleteDataId)
            }
        '''
        params = {
            "datasetId": dataset_id,
            "deleteDataId": data_id,
        }
        self.log.info(f"Executing delete_data for data_id: {data_id} in dataset: {dataset_id}")
        result = self.run_gql("deleteData", query, params)
        if isinstance(result, bool):
            return result
        return True if result is not None else False

    # --- New methods for specific GraphQL queries ---

    def create_content(self, key: str) -> Dict[str, Any]:
        """
        Corresponds to ContentService.create_content GraphQL mutation.
        Creates a content entry and returns its ID and upload URL.
        """
        query = '''
            mutation CreateContent($key: String!) {
                createContent(key: $key) {
                    content {
                        id
                        key
                    }
                    uploadURL
                }
            }
        '''
        variables = {"key": key}
        return self.run_gql("createContent", query, variables)

    def create_data(
        self,
        dataset_id: str,
        key: str,
        type: str,
        meta: Optional[List[dict]] = None,
        system_meta: Optional[List[dict]] = None,
        scene: Optional[List[dict]] = None,
        annotation: Optional[Dict[str, Any]] = None,
        thumbnail: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Corresponds to DataService.create_data GraphQL mutation.
        Creates a data record.
        """
        query = '''
            mutation CreateData(
                $dataset_id: ID!,
                $key: String!,
                $type: DataType!,
                $meta: [DataMetaInput!],
                $system_meta: [DataMetaInput!],
                $scene: [SceneInput!],
                $annotation: AnnotationInput,
                $thumbnail: ContentBaseInput
            ) {
                createData(
                    datasetId: $dataset_id,
                    key: $key,
                    type: $type,
                    meta: $meta,
                    systemMeta: $system_meta,
                    scene: $scene,
                    annotation: $annotation,
                    thumbnail: $thumbnail
                ) {
                    id
                    key
                }
            }
        '''
        variables: Dict[str, Any] = {
            "dataset_id": dataset_id,
            "key": key,
            "type": type,
            "meta": meta or [],
            "system_meta": system_meta or [],
        }
        if scene:
            variables["scene"] = scene
        if annotation:
            variables["annotation"] = annotation
        if thumbnail:
            variables["thumbnail"] = thumbnail

        return self.run_gql("createData", query, variables)

    def get_datasets(
        self,
        name_contains_filter: Optional[str] = None,
        length: int = 100,
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Corresponds to DatasetService.get_dataset_list GraphQL query.
        Retrieves a list of datasets.
        """
        query = '''
            query datasets($length: Int, $filter: DatasetFilter, $cursor: String) {
                datasets(length: $length, filter: $filter, cursor: $cursor) {
                    datasets {
                        id
                        name
                    }
                    next
                    totalCount
                }
            }
        '''
        variables: Dict[str, Any] = {"length": length}
        if name_contains_filter:
            variables["filter"] = {
                "must": {"nameContains": name_contains_filter}
            }
        if cursor:
            variables["cursor"] = cursor

        return self.run_gql("datasets", query, variables)

    def create_dataset(
        self,
        name: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Corresponds to DatasetService.create_dataset GraphQL mutation.
        Creates a new dataset.
        """
        query = '''
            mutation CreateDataset($name: String!, $description: String) {
                createDataset(name: $name, description: $description) {
                    id
                    name
                }
            }
        '''
        variables: Dict[str, Any] = {"name": name}
        if description:
            variables["description"] = description
        return self.run_gql("createDataset", query, variables)

    def upload_file(
        self,
        upload_presigned_url: str,
        file_data: BytesIO,
        content_type: str,
        **request_kwargs: Any
    ) -> bool:
        """
        Uploads file data to a pre-signed URL using PUT.

        :param upload_presigned_url: The URL to upload the file to.
        :param file_data: A BytesIO object containing the file data.
        :param content_type: The MIME type of the file.
        :param request_kwargs: Additional keyword arguments for requests put.
        :return: True if upload is successful (HTTP 200), False otherwise.
        :raises AirflowException: If the upload request fails.
        """
        session = self.get_conn()
        file_data.seek(0)
        data_bytes = file_data.getvalue()
        headers = {'Content-Type': content_type}

        self.log.info(
            "Uploading file to %s (Content-Type: %s, Size: %d bytes)",
            upload_presigned_url, content_type, len(data_bytes)
        )

        # 직접 재시도 로직 제거됨
        try:
            response = session.put(
                url=upload_presigned_url,
                data=data_bytes,
                headers=headers,
                **request_kwargs
            )
            response.raise_for_status()
            self.log.info(
                "Upload successful (Status Code: %d)", response.status_code
            )
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            # 에러에 대한 상세 정보 추출
            error_details = self._extract_error_details(e)
            self.log.error(
                "Failed to upload file to %s after session retries: %s\n"
                "Error details: %s", 
                upload_presigned_url, e, error_details
            )
            raise AirflowException(f"File upload failed after session retries: {e}") from e
                
    def upload_json(
        self,
        upload_presigned_url: str,
        data: dict,
        **request_kwargs: Any
    ) -> bool:
        """
        Uploads JSON data to a pre-signed URL using PUT.

        :param upload_presigned_url: The URL to upload the JSON to.
        :param data: A dictionary representing the JSON data.
        :param request_kwargs: Additional keyword arguments for requests put.
        :return: True if upload is successful (HTTP 200), False otherwise.
        :raises AirflowException: If the upload request fails.
        """
        session = self.get_conn()
        headers = {'Content-Type': 'application/json'}

        self.log.info("Uploading JSON to %s", upload_presigned_url)

        # 직접 재시도 로직 제거됨
        try:
            response = session.put(
                url=upload_presigned_url,
                json=data,
                headers=headers,
                **request_kwargs
            )
            response.raise_for_status()
            self.log.info(
                "JSON upload successful (Status Code: %d)",
                response.status_code
            )
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            # 에러에 대한 상세 정보 추출
            error_details = self._extract_error_details(e)
            self.log.error(
                "Failed to upload JSON to %s after session retries: %s\n"
                "Error details: %s", 
                upload_presigned_url, e, error_details
            )
            raise AirflowException(f"JSON upload failed after session retries: {e}") from e
                
    def _extract_error_details(self, error: requests.exceptions.RequestException) -> str:
        """
        에러 객체에서 상세 정보를 추출합니다.
        
        :param error: 발생한 요청 예외
        :return: 포맷팅된 에러 상세 정보 문자열
        """
        details = []
        
        # 응답이 있는 경우 (HTTPError 등)
        response = getattr(error, 'response', None)
        if response:
            details.append(f"Status: {response.status_code}")
            
            # 응답 헤더 중요 정보
            headers = response.headers
            important_headers = [
                'content-type', 'x-amz-error-code', 
                'x-amz-error-message', 'server', 
                'x-amz-request-id', 'date'
            ]
            header_info = {
                k: headers.get(k) for k in important_headers 
                if k in headers
            }
            if header_info:
                details.append(f"Headers: {header_info}")
            
            # 응답 본문 (있으면)
            try:
                content = response.text[:500]  # 너무 길면 자르기
                if content:
                    details.append(f"Response: {content}")
            except Exception:
                pass
        
        # URL 파싱 분석 (만료된 토큰 등 체크)
        url = getattr(error, 'url', None) or getattr(error, 'request', {}).url
        if url and 'X-Amz-Date' in url and 'X-Amz-Expires' in url:
            try:
                import re
                import datetime
                # dateutil 모듈은 사용하지 않으므로 제거
                
                # X-Amz-Date 추출
                date_match = re.search(r'X-Amz-Date=([^&]+)', url)
                if date_match:
                    amz_date = date_match.group(1)
                    details.append(f"X-Amz-Date: {amz_date}")
                    
                    # X-Amz-Expires 추출
                    expires_match = re.search(r'X-Amz-Expires=(\d+)', url)
                    if expires_match:
                        expires_seconds = int(expires_match.group(1))
                        details.append(f"X-Amz-Expires: {expires_seconds}s")
                        
                        # 만료 여부 체크
                        try:
                            date_format = "%Y%m%dT%H%M%SZ"
                            request_time = datetime.datetime.strptime(
                                amz_date, date_format
                            ).replace(tzinfo=datetime.timezone.utc)
                            expires_time = request_time + datetime.timedelta(
                                seconds=expires_seconds
                            )
                            now = datetime.datetime.now(datetime.timezone.utc)
                            
                            if now > expires_time:
                                details.append(
                                    f"URL EXPIRED: {now} > {expires_time}"
                                )
                            else:
                                details.append(
                                    f"URL valid until: {expires_time}"
                                )
                        except Exception as date_err:
                            details.append(
                                f"Error parsing expiration: {date_err}"
                            )
            except Exception as parse_err:
                details.append(f"Error analyzing URL: {parse_err}")
                
        # 기본 오류 원인
        cause = getattr(error, '__cause__', None)
        if cause:
            details.append(f"Cause: {cause}")
            
        # ConnectionError 추가 정보
        if isinstance(error, requests.exceptions.ConnectionError):
            details.append("Connection error - check network/hostname")
            
        # Timeout 추가 정보
        if isinstance(error, requests.exceptions.Timeout):
            details.append("Request timed out - server may be overloaded")
            
        # SSL 오류 추가 정보
        if isinstance(error, requests.exceptions.SSLError):
            details.append("SSL verification failed")
        
        return " | ".join(details)

    def download_file(
        self,
        download_presigned_url: str,
        **request_kwargs: Any
    ) -> Any:
        """
        Downloads a file from a pre-signed URL using GET.
        Determines response type based on Content-Type header.

        :param download_presigned_url: The URL to download from.
        :param request_kwargs: Additional keyword arguments for requests get.
        :return: Dictionary if content is JSON, BytesIO otherwise.
        :raises AirflowException: If download request fails or content unknown.
        """
        session = self.get_conn()
        self.log.info("Downloading file from %s", download_presigned_url)
        try:
            response = session.get(download_presigned_url, **request_kwargs)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '').lower()
            self.log.debug("Received Content-Type: %s", content_type)

            if 'application/json' in content_type:
                self.log.info("Detected JSON content, parsing.")
                return response.json()
            elif content_type:
                self.log.info(
                    "Detected non-JSON content (%s), returning as BytesIO.",
                    content_type
                )
                return BytesIO(response.content)
            else:
                self.log.warning(
                    "Content-Type header missing or empty, "
                    "returning as BytesIO."
                )
                return BytesIO(response.content)

        except requests.exceptions.RequestException as e:
            self.log.error(
                "Failed to download file from %s: %s",
                download_presigned_url, e
            )
            raise AirflowException(f"File download failed: {e}") from e
        except json.JSONDecodeError as e:
            self.log.error("Failed to parse downloaded JSON content: %s", e)
            raise AirflowException(
                f"Failed to parse JSON response: {e}"
            ) from e
