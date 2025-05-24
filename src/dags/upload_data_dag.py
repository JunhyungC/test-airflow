import os
import json
import logging
from io import BytesIO
from PIL import Image
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

from airflow.decorators import task
from airflow.models import DAG

from utils.hooks.sunrise_api_hook import SunriseApiHook

# --- Module-level constants for environment variables ---
SUNRISE_API_CONN_ID = os.getenv("SUNRISE_API_CONN_ID", "sunrise_api_default")
UPLOAD_DATA_ROOT_PATH = os.getenv("UPLOAD_DATA_ROOT_PATH", "/app/input")

# --- 헬퍼 함수 ---
def create_thumbnail(
    image_bytes: bytes,  # BytesIO 대신 bytes를 받도록 변경 고려
    thumbnail_size=(128, 128)
) -> Optional[BytesIO]:
    """주어진 이미지 바이트에서 썸네일 이미지를 생성합니다."""
    try:
        image = BytesIO(image_bytes)
        with Image.open(image) as img:
            img.thumbnail(thumbnail_size, Image.Resampling.LANCZOS)
            thumb = Image.new('RGB', thumbnail_size, (255, 255, 255))
            left = (thumbnail_size[0] - img.width) // 2
            top = (thumbnail_size[1] - img.height) // 2
            thumb.paste(img, (left, top))
            thumbnail_io = BytesIO()
            thumb.save(thumbnail_io, format='JPEG')
            thumbnail_io.seek(0)
            return thumbnail_io
    except Exception as e:  # pylint: disable=broad-except
        logging.error(f"Error creating thumbnail: {e}")
        return None

# get_all_files_with_relative_paths 함수는 이제 직접 사용되지 않을 수 있습니다.

# --- DAG 정의 (컨텍스트 매니저 스타일) ---

dag_args = {
    'owner': 'airflow',
    'retries': 1,  # 배치 처리 태스크에서 내부적으로 재시도 관리 고려
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="upload_data_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["upload", "batch-optimized"],  # 태그 업데이트
    default_args=dag_args,
    doc_md='''
    ### 데이터 업로드 DAG (단일 데이터셋 중심 - 배치 최적화)

    배치 처리를 사용하여 특정 데이터셋 디렉토리의 내용을 업로드합니다.

    **사용법:**
    트리거 시 `dataset_name` 매개변수를 제공하세요.
    DAG는 `/app/input/{dataset_name}/`에서 파일을 처리합니다.

    **`/app/input/{dataset_name}/` 내의 예상 폴더 구조:**
    - `{dataset_name}/`: 이미지 파일 포함 (예: `split_train_0.jpeg`).
    - `meta/{dataset_name}/`: 메타데이터 JSON 파일 포함 (예: `split_train_0.jpeg.json`).
    - `labels/`: 주석 JSON 파일 포함 (예: `UUID.json`).
    - `project.json` 및 `.DS_Store` 파일은 무시됩니다.

    **워크플로우:**
    1. 데이터셋 경로를 검증합니다.
    2. API에서 대상 데이터셋을 가져오거나 생성합니다.
    3. `meta/{dataset_name}/` 디렉토리를 스캔하여 메타 파일 경로를 기반으로 배치를 정의합니다.
    4. 각 배치 명세에 대해:
        a. 지정된 메타 파일에서 항목 세부 정보(이미지, 주석, 메타)를 구성합니다.
        b. 이미지 업로드, 썸네일 생성, 썸네일 업로드, 주석 업로드를 합니다.
        c. 성공적으로 업로드된 항목에 대한 데이터 레코드를 생성합니다.
    ''',
    params={
        "dataset_name": "",
        "batch_size": 500  # 배치 크기 파라미터 추가 (조정 가능)
    },
) as dag:

    log = logging.getLogger(__name__)

    @task
    def validate_dataset_path(data_root: str, dataset_name: str) -> Optional[str]:
        """
        데이터셋 경로가 존재하는지 검증합니다.
        존재하는 경우 전체 경로를 반환하고, 그렇지 않으면 None을 반환합니다.
        """
        task_log = logging.getLogger(__name__)

        if not dataset_name:
            task_log.error("No dataset_name provided. Please specify a dataset_name.")
            return None  # 기존 방식 유지: None 반환하여 downstream 건너뛰기

        dataset_path = os.path.join(data_root, dataset_name)

        if not os.path.isdir(dataset_path):
            task_log.error(f"Dataset path '{dataset_path}' does not exist or is not a directory.")
            return None

        # 추가 검증: 필요한 하위 디렉토리 존재 여부 확인
        # meta 디렉토리 경로 (tree_result.txt 기준)
        meta_dir_path = os.path.join(dataset_path, "meta", dataset_name)
        # 이미지 디렉토리 경로 (tree_result.txt 기준)
        image_dir_path = os.path.join(dataset_path, dataset_name)
        labels_dir_path = os.path.join(dataset_path, "labels")

        if not os.path.isdir(meta_dir_path):
            task_log.warning(f"Meta directory not found: {meta_dir_path}")
            # 메타 파일이 없으면 진행 불가하므로 None 반환
            return None
        if not os.path.isdir(image_dir_path):
            task_log.warning(f"Image directory not found: {image_dir_path}")
            # 이미지가 없으면 진행 불가
            return None
        if not os.path.isdir(labels_dir_path):
            task_log.warning(f"Labels directory not found: {labels_dir_path}")
            # 레이블이 없을 수 있으므로 경고만 출력하고 진행 가능하게 둘 수 있음
            # 또는 엄격하게 체크하려면 None 반환

        task_log.info(f"Validated dataset path: {dataset_path} and required subdirectories.")
        return dataset_path

    @task
    def define_batch_specifications(  # 이름 변경 및 로직 수정
        dataset_root_path: str,
        dataset_name: str,
        batch_size: Any
    ) -> List[Dict[str, List[str]]]:  # 반환 타입 변경: 배치 명세 리스트
        """
        메타 파일을 스캔하고 "배치 명세" 목록을 반환합니다.
        각 명세에는 배치에 대한 메타 파일 절대 경로 목록이 포함됩니다.
        params에서 batch_size의 int 변환을 처리합니다.
        """
        task_log = logging.getLogger(__name__)
        # batch_size를 int로 변환 시도 (Airflow Jinja 템플릿 결과는 문자열일 수 있음)
        try:
            processed_batch_size = int(batch_size)
            if processed_batch_size <= 0:
                task_log.warning(f"Invalid batch_size value: {processed_batch_size}. Using default 500.")
                processed_batch_size = 500
        except (ValueError, TypeError):
            task_log.error(f"Invalid batch_size parameter type: {type(batch_size)}, value: {batch_size}. Using default 500.")
            processed_batch_size = 500

        task_log.info(f"Starting to define batch specifications from: {dataset_root_path} with batch_size: {processed_batch_size}")

        # tree_result.txt 구조 기반 경로 설정
        meta_dir_path = os.path.join(dataset_root_path, "meta", dataset_name)
        # image_base_dir = os.path.join(dataset_root_path, dataset_name) # 이미지 파일 기본 경로 -> upload_batch_data_items 로 이동
        # labels_base_dir = os.path.join(dataset_root_path, "labels")     # 레이블 파일 기본 경로 -> upload_batch_data_items 로 이동

        if not os.path.isdir(meta_dir_path):
            task_log.error(f"Meta directory not found: {meta_dir_path}. Cannot define batches.")
            # 메타 디렉토리가 없으면 진행 불가, 빈 iterator 반환
            return [] 

        all_meta_file_paths = []  # 메타파일 절대경로만 수집
        # current_batch = [] -> 배치 명세로 변경
        # items_generated = 0 -> 메타파일 수 또는 배치 명세 수로 변경
        image_extensions = {".jpeg", ".jpg", ".png"}  # 메타 파일 확장자 검사 시 사용

        for root, _, files in os.walk(meta_dir_path):
            for meta_filename in files:
                # 메타 파일 확장자 확인 (예: .jpeg.json)
                if not any(meta_filename.lower().endswith(ext + ".json") for ext in image_extensions):
                    task_log.debug(f"Skipping non-image-meta JSON: {meta_filename}")
                    continue

                meta_abs_path = os.path.join(root, meta_filename)
                all_meta_file_paths.append(meta_abs_path)  # 절대 경로 추가
        
        if not all_meta_file_paths:
            task_log.warning(f"No valid meta files found to process in {meta_dir_path}.")
            return []

        # 일관된 처리 순서를 위한 정렬
        all_meta_file_paths.sort()
        
        all_batch_specifications = []  # 최종 반환될 배치 명세 리스트
        current_meta_file_batch = []  # 현재 구성 중인 메타 파일 경로 배치
        
        for meta_path in all_meta_file_paths:
            current_meta_file_batch.append(meta_path)
            if len(current_meta_file_batch) >= processed_batch_size:
                all_batch_specifications.append({"meta_files_for_this_batch": current_meta_file_batch})
                task_log.info(f"Defined a batch specification with {len(current_meta_file_batch)} meta files.")
                current_meta_file_batch = []  # 새 배치 시작
        
        if current_meta_file_batch:  # 마지막 남은 메타 파일들 처리
            all_batch_specifications.append({"meta_files_for_this_batch": current_meta_file_batch})
            task_log.info(f"Defined the final batch specification with {len(current_meta_file_batch)} meta files.")

        if not all_batch_specifications:  # all_meta_file_paths는 있었지만 배치가 안 만들어진 경우 (이론상 processed_batch_size > 0 이면 발생 안함)
            task_log.warning("No batch specifications were generated despite finding meta files.")
        
        task_log.info(f"Finished defining batch specifications. Total specifications: {len(all_batch_specifications)}, Total meta files processed: {len(all_meta_file_paths)}")

        return all_batch_specifications  # 배치 명세 리스트 반환

    @task
    def upload_batch_data_items(
        dataset_api_id: str,
        batch_specification: Dict[str, List[str]],  # 입력 파라미터 변경
        dataset_root_path: str,                     # Caller passes this
        dataset_name: str,                          # Caller passes this
    ):
        """
        'batch_specification'에 의해 정의된 배치 항목을 처리합니다.
        각 항목의 세부 정보는 이 태스크 내에서 메타 파일 경로에서 구성됩니다.
        API에 데이터를 업로드하고 결과를 보고합니다.
        콘텐츠/데이터 생성을 위한 공식 배치 API가 존재하지 않는다고 가정합니다.
        기본 멱등성 검사(개념적)를 포함합니다.
        """
        task_log = logging.getLogger(__name__)
        hook = SunriseApiHook(http_conn_id=SUNRISE_API_CONN_ID)  # Directly use the DAG-level variable
        
        meta_files_for_this_batch = batch_specification.get(
            "meta_files_for_this_batch", []
        )
        if not meta_files_for_this_batch:
            task_log.warning(f"Empty or invalid batch specification received for dataset_id {dataset_api_id}. Skipping.")
            # log_final_summary가 오류 없이 처리할 수 있도록 빈 결과 반환
            return {
                "batch_total_items": 0,
                "successful_items": 0,
                "failed_items_count": 0,
                "failed_item_details": [],
                "new_data_record_ids": []
            }

        # --- 아이템 정보 재구성 로직 (기존 generate_item_batches에서 이관/수정) ---
        actual_batch_of_items = []  # 이 태스크 인스턴스가 처리할 실제 아이템들
        image_base_dir = os.path.join(dataset_root_path, dataset_name)
        # labels_base_dir = os.path.join(dataset_root_path, "labels")  # 필요시 사용

        task_log.info(f"Constructing items for dataset_id {dataset_api_id} from {len(meta_files_for_this_batch)} meta file paths.")

        for meta_abs_path in meta_files_for_this_batch:
            meta_rel_path = os.path.relpath(meta_abs_path, dataset_root_path)
            # 아이템 구성 실패 시 사용할 기본 키 (실패 로깅용)
            item_construction_key_for_log = os.path.basename(meta_abs_path) 
            try:
                with open(meta_abs_path, 'r', encoding='utf-8') as f:
                    meta_content = json.load(f)

                data_key = meta_content.get("data_key")  # 예: "split_train_632.jpeg"
                label_path_list = meta_content.get("label_path")  # 예: ["labels/... .json"]

                if not data_key:
                    task_log.warning(f"Meta file {meta_rel_path} (abs: {meta_abs_path}) lacks 'data_key'. Skipping item construction.")
                    # 실패 처리는 바깥 루프에서 failed_item_keys_in_batch에 추가
                    # 실제로는 이 아이템은 actual_batch_of_items에 안들어가므로, 아래에서 처리되지 않음
                    # 하지만 전체 배치 카운트에서는 고려해야 할 수 있음 - log_final_summary에서 조정
                    continue  # 다음 메타 파일로
                    
                item_construction_key_for_log = data_key  # data_key가 있으면 로깅 시 사용

                # 레이블 경로는 없을 수도 있으므로, 없으면 None으로 처리
                annotation_rel_path = None
                annotation_abs_path = None
                if label_path_list and isinstance(label_path_list, list) and label_path_list[0].startswith("labels/"):
                    annotation_rel_path = label_path_list[0]  # "labels/UUID.json" 형태
                    annotation_abs_path = os.path.join(dataset_root_path, annotation_rel_path)
                    if not os.path.exists(annotation_abs_path):
                        task_log.warning(f"Annotation file specified in meta {meta_rel_path} not found: {annotation_abs_path}. Proceeding without annotation for item {data_key}.")
                        annotation_rel_path = None
                        annotation_abs_path = None
                elif label_path_list:  # label_path가 있지만 형식이 잘못된 경우
                    task_log.warning(f"Meta file {meta_rel_path} has invalid 'label_path' format: {label_path_list} for item {data_key}. Proceeding without annotation.")

                # 이미지 경로 구성 (tree_result.txt 구조 기반)
                image_abs_path_constructed = os.path.join(image_base_dir, data_key)
                image_rel_path_constructed = os.path.relpath(image_abs_path_constructed, dataset_root_path)

                # 이미지 파일 존재 확인 (필수)
                if not os.path.exists(image_abs_path_constructed):
                    task_log.warning(f"Image file not found for meta {meta_rel_path} (item {data_key}): {image_abs_path_constructed}. Skipping item construction.")
                    continue  # 다음 메타 파일로

                # 아이템 키 생성 (이미지 파일명 기반 - 확장자 제외)
                image_base_key, _ = os.path.splitext(data_key)

                # API 전송용 메타데이터 파싱 (기존 로직 활용)
                api_meta_list = []
                if isinstance(meta_content.get("tags"), list):
                    for tag_obj in meta_content["tags"]:
                        if isinstance(tag_obj, dict) and "name" in tag_obj:
                            api_meta_list.append({"key": "tag", "value": tag_obj["name"], "type": "String"})
                if isinstance(meta_content.get("image_info"), dict):
                    img_info = meta_content["image_info"]
                    if "width" in img_info: api_meta_list.append({"key": "image_width", "value": str(img_info["width"]), "type": "Number"})
                    if "height" in img_info: api_meta_list.append({"key": "image_height", "value": str(img_info["height"]), "type": "Number"})

                # 배치에 추가할 아이템 딕셔너리 생성
                item_dict = {
                    "key": image_base_key,  # 업로드 및 데이터레코드 생성 시 사용될 key
                    "_dataset_name_on_disk": dataset_name,  # 디스크 상의 데이터셋 이름
                    "image_rel_path": image_rel_path_constructed,
                    "image_abs_path": image_abs_path_constructed,
                    "annotation_rel_path": annotation_rel_path,  # None일 수 있음
                    "annotation_abs_path": annotation_abs_path,  # None일 수 있음
                    "meta_rel_path": meta_rel_path,
                    "meta_abs_path": meta_abs_path,  # 원본 메타파일 경로 (로깅/디버깅용)
                    "meta_parsed_for_api": api_meta_list
                }
                actual_batch_of_items.append(item_dict)
                task_log.debug(f"Successfully constructed item '{image_base_key}' from meta {meta_rel_path}.")

            except json.JSONDecodeError:
                task_log.error(f"Error decoding meta JSON: {meta_abs_path} (item key for log: {item_construction_key_for_log}). Skipping item construction.")
            except Exception as e:
                task_log.error(f"Error processing meta file {meta_rel_path} (item key for log: {item_construction_key_for_log}) to build item details: {e}. Skipping item construction.")
        # --- 아이템 정보 재구성 로직 종료 ---
        
        successful_item_uploads_in_batch = 0
        failed_item_keys_in_batch = [] 
        data_record_ids_created = []

        # 만약 actual_batch_of_items가 비어있다면 (모든 메타파일 파싱/아이템 구성 실패 등)
        # meta_files_for_this_batch는 있었지만, actual_batch_of_items가 0개일 수 있음
        if not actual_batch_of_items:
            task_log.warning(f"No valid items could be constructed from the {len(meta_files_for_this_batch)} meta files in this batch specification for dataset_id {dataset_api_id}.")
            # 실패 아이템 리스트 생성
            failed_details_for_empty_batch = []
            for mfp in meta_files_for_this_batch:
                failed_details_for_empty_batch.append({"key": os.path.basename(mfp), "reason": "Failed to construct item details from meta file (e.g., missing data_key, image_not_found, JSON error)"})

            return {
                "batch_total_items": 0,  # 실제 처리 시도한 아이템이 없으므로 0
                "successful_items": 0,
                "failed_items_count": len(meta_files_for_this_batch),  # 명세에 있던 메타 파일 수만큼 실패
                "failed_item_details": failed_details_for_empty_batch,
                "new_data_record_ids": []
            }

        task_log.info(f"Starting API processing for a batch of {len(actual_batch_of_items)} constructed items for dataset_id {dataset_api_id} (derived from {len(meta_files_for_this_batch)} meta file specs).")

        # --- 아이템별 순차 처리 (기존 로직과 거의 동일, actual_batch_of_items 사용) ---
        for data_item in actual_batch_of_items:
            key = data_item.get("key")
            item_processed_successfully = False
            error_message = ""

            try:
                task_log.debug(f"Processing item: {key}")
                image_abs_path = data_item.get("image_abs_path")
                annotation_abs_path = data_item.get("annotation_abs_path")

                image_content_bytes = None
                thumbnail_io = None
                annotation_content = None
                image_content_id = None
                thumbnail_content_id = None
                annotation_api_content_id = None
                
                # --- 1. 이미지 및 썸네일 처리 ---
                if image_abs_path and os.path.exists(image_abs_path):
                    try:
                        with open(image_abs_path, 'rb') as f:
                            image_content_bytes = f.read()
                        thumbnail_io = create_thumbnail(image_content_bytes)
                    except Exception as e:
                        task_log.warning(f"Error reading/processing image {image_abs_path} for key {key}: {e}")
                        image_content_bytes = None
                        thumbnail_io = None
                else:
                    task_log.warning(f"Image path not found or not provided: {image_abs_path} for key {key}")

                # --- 2. 어노테이션 처리 ---
                if annotation_abs_path and os.path.exists(annotation_abs_path):
                    try:
                        with open(annotation_abs_path, 'r', encoding='utf-8') as f:
                            annotation_content = json.load(f)
                    except Exception as e:
                        task_log.warning(f"Error reading annotation {annotation_abs_path} for key {key}: {e}")
                        annotation_content = None
                else:
                    task_log.warning(f"Annotation path not found or not provided: {annotation_abs_path} for key {key}")

                # --- 3. API 업로드 (Content 생성 및 파일 업로드) ---
                upload_results = {"image": False, "thumbnail": False, "annotation": False}
                
                # 3.1 이미지 업로드 (멱등성 고려)
                image_content_key = f"{dataset_api_id}_{key}_image"
                # Conceptual: Check if content already exists
                # existing_image_content = hook.get_content_by_key(image_content_key)
                # if existing_image_content:
                #     image_content_id = existing_image_content['id']
                #     upload_results["image"] = True # 이미 존재하면 성공으로 간주
                #     task_log.info(f"Image content already exists for key {key}, id: {image_content_id}")
                if image_content_bytes:  # and not existing_image_content:  # 이미 존재하지 않을 때만 업로드
                    try:
                        response = hook.create_content(key=image_content_key)
                        image_content_id = response["content"]["id"]
                        upload_url = response["uploadURL"]
                        image_io = BytesIO(image_content_bytes)
                        upload_success = hook.upload_file(
                            upload_presigned_url=upload_url,
                            file_data=image_io,
                            content_type="image/jpeg"
                        )
                        if upload_success:
                            upload_results["image"] = True
                            task_log.debug(f"Uploaded image for key {key}, id: {image_content_id}")
                        else:
                            task_log.warning(f"Image upload via presigned URL failed for key {key}")
                    except Exception as e:
                        task_log.error(f"Failed to create/upload image content for key {key}: {e}")

                # 3.2 썸네일 업로드 (멱등성 고려)
                thumbnail_content_key = f"{dataset_api_id}_{key}_thumbnail"
                # Conceptual: Check if content already exists
                # existing_thumbnail_content = hook.get_content_by_key(thumbnail_content_key)
                # if existing_thumbnail_content:
                #     thumbnail_content_id = existing_thumbnail_content['id']
                #     upload_results["thumbnail"] = True
                #     task_log.info(f"Thumbnail content already exists for key {key}, id: {thumbnail_content_id}")
                if thumbnail_io:  # and not existing_thumbnail_content:
                    try:
                        response = hook.create_content(key=thumbnail_content_key)
                        thumbnail_content_id = response["content"]["id"]
                        upload_url = response["uploadURL"]
                        upload_success = hook.upload_file(
                            upload_presigned_url=upload_url,
                            file_data=thumbnail_io,
                            content_type="image/jpeg"
                        )
                        if upload_success:
                            upload_results["thumbnail"] = True
                            task_log.debug(f"Uploaded thumbnail for key {key}, id: {thumbnail_content_id}")
                        else:
                            task_log.warning(f"Thumbnail upload via presigned URL failed for key {key}")
                    except Exception as e:
                        task_log.error(f"Failed to create/upload thumbnail content for key {key}: {e}")
                        
                # 3.3 어노테이션 업로드 (멱등성 고려)
                annotation_content_key = f"{dataset_api_id}_{key}_annotation"
                # Conceptual: Check if content already exists
                # existing_annotation_content = hook.get_content_by_key(annotation_content_key)
                # if existing_annotation_content:
                #    annotation_api_content_id = existing_annotation_content['id']
                #    upload_results["annotation"] = True
                #    task_log.info(f"Annotation content already exists for key {key}, id: {annotation_api_content_id}")

                converted_annotations_for_api = None
                if annotation_content and isinstance(annotation_content.get("objects"), list):
                    converted_annotations_for_api = {"objects": []}
                    for obj in annotation_content["objects"]:
                        anno_type = obj.get("annotation_type")
                        anno_class = obj.get("class_name")
                        anno_value_dict = obj.get("annotation", {}).get("coord")
                        if (anno_type in ["box", "polygon"] and anno_class and anno_value_dict):
                            converted_annotations_for_api["objects"].append({
                                "class_name": anno_class,
                                "annotation_type": anno_type,
                                "annotation": {"coord": anno_value_dict}
                            })
                        else:
                            task_log.warning(f"Skipping invalid annotation object for key {key}: {obj}")
                    if not converted_annotations_for_api["objects"]:
                        converted_annotations_for_api = None  # 유효한 object 없으면 None 처리

                if converted_annotations_for_api:  # and not existing_annotation_content:
                    try:
                        response = hook.create_content(key=annotation_content_key)
                        annotation_api_content_id = response["content"]["id"]
                        upload_url = response["uploadURL"]
                        upload_success = hook.upload_json(
                            upload_presigned_url=upload_url,
                            data=converted_annotations_for_api
                        )
                        if upload_success:
                            upload_results["annotation"] = True
                            task_log.debug(f"Uploaded annotation for key {key}, id: {annotation_api_content_id}")
                        else:
                            task_log.warning(f"Annotation upload via presigned URL failed for key {key}")
                    except Exception as e:
                        task_log.error(f"Failed to create/upload annotation content for key {key}: {e}")
                elif annotation_content:  # annotation_content는 있었으나 변환 결과가 없을 때
                    task_log.warning(f"No valid annotation objects to upload for key {key}")

                # --- 4. Data Record 생성 (멱등성 고려) ---
                any_upload_success = any(upload_results.values())
                data_record_id = None
                
                # Conceptual: Check if data record already exists
                # existing_data_record = hook.get_data_by_key(dataset_api_id, key)
                # if existing_data_record:
                #     data_record_id = existing_data_record['id']
                #     task_log.info(f"Data record already exists for key {key}, id: {data_record_id}")
                
                if any_upload_success:  # and not existing_data_record:  # 업로드 성공한 것이 있고, 데이터 레코드가 없을 때만 생성 시도
                    try:
                        system_meta_list = []
                        if img_rel_path := data_item.get("image_rel_path"):
                            system_meta_list.append({"key": "original_image_path", "value": img_rel_path, "type": "String"})
                        if anno_rel_path := data_item.get("annotation_rel_path"):
                            system_meta_list.append({"key": "original_annotation_path", "value": anno_rel_path, "type": "String"})
                        if meta_rel_path := data_item.get("meta_rel_path"):
                            system_meta_list.append({"key": "original_meta_path", "value": meta_rel_path, "type": "String"})

                        create_data_vars = {
                            "dataset_id": dataset_api_id,
                            "key": key,
                            "type": "SUPERB_IMAGE",
                            "meta": data_item.get("meta_parsed_for_api", []),
                            "system_meta": system_meta_list
                        }
                        # 성공적으로 업로드된 Content ID만 추가
                        if image_content_id and upload_results["image"]:
                            create_data_vars["scene"] = [{"type": "IMAGE", "content": {"id": image_content_id}, "meta": {}}]
                        if annotation_api_content_id and upload_results["annotation"]:
                            create_data_vars["annotation"] = {"versions": {"content": {"id": annotation_api_content_id}, "meta": {}}, "meta": {}}
                        if thumbnail_content_id and upload_results["thumbnail"]:
                            create_data_vars["thumbnail"] = {"id": thumbnail_content_id}
                        
                        # Data Record 생성 API 호출
                        data_record_response = hook.create_data(**create_data_vars)
                        if data_record_response and data_record_response.get("id"):
                            data_record_id = data_record_response["id"]
                            task_log.debug(f"Created data record for key {key}: {data_record_id}")
                            data_record_ids_created.append(data_record_id)
                            item_processed_successfully = True  # 최종 성공
                        else:
                            task_log.error(f"Data record creation API call returned invalid response for key {key}: {data_record_response}")
                            error_message = "Data record creation failed (API response invalid)"

                    except Exception as e:
                        task_log.error(f"Failed to create data record for key {key}: {e}")
                        error_message = f"Data record creation failed: {e}"
                elif not any_upload_success:
                    task_log.warning(f"Skipping data record creation for key {key} as all content uploads failed or were skipped.")
                    error_message = "All content uploads failed or were skipped"
                # else: # existing_data_record 가 있는 경우
                #     item_processed_successfully = True # 이미 존재하므로 성공 처리
                
            except Exception as e:  # 아이템 처리 중 예상치 못한 오류
                task_log.exception(f"Unexpected error processing item {key} in batch: {e}")
                error_message = f"Unexpected error: {e}"

            # --- 아이템 처리 결과 집계 ---
            if item_processed_successfully:
                successful_item_uploads_in_batch += 1
            else:
                failed_item_keys_in_batch.append({"key": key if key else f"unknown_item_from_{data_item.get('meta_abs_path', 'unknown_meta')}", "reason": error_message})

        # --- 배치 처리 결과 로깅 ---
        task_log.info(
            f"Batch processing summary (dataset ID {dataset_api_id}): "
            f"Total items processed: {len(actual_batch_of_items)} (derived from {len(meta_files_for_this_batch)} meta file specs), "
            f"Successfully processed: {successful_item_uploads_in_batch} items, "
            f"Failed: {len(failed_item_keys_in_batch)} items. "
            f"New data record IDs: {data_record_ids_created}"
        )

        if failed_item_keys_in_batch:
            task_log.warning(f"Failed item details for dataset ID {dataset_api_id}: {failed_item_keys_in_batch}")
            # 필요시, 실패 항목이 있으면 전체 배치를 실패로 간주할 수 있음
            # raise AirflowFailException(f"{len(failed_item_keys_in_batch)} items failed in batch. Check logs.")

        return {
            "batch_total_items": len(actual_batch_of_items),  # 실제 처리 시도된 (성공적으로 구성된) 아이템 수
            "successful_items": successful_item_uploads_in_batch,
            "failed_items_count": len(failed_item_keys_in_batch),
            "failed_item_details": failed_item_keys_in_batch,
            "new_data_record_ids": data_record_ids_created
        }

    @task
    def get_or_create_dataset(
        dataset_name_for_api: str,
    ) -> str:
        """이름으로 기존 데이터셋의 ID를 가져오거나, 새 데이터셋을 생성합니다."""
        task_log = logging.getLogger(__name__)
        hook = SunriseApiHook(http_conn_id=SUNRISE_API_CONN_ID)  # Directly use the DAG-level variable
        selected_dataset = None
        cursor = None
        task_log.info(
            f"Checking dataset by name: {dataset_name_for_api}"
        )

        try:
            while True:
                # 페이지네이션 고려하여 조회
                response = hook.get_datasets(
                    name_contains_filter=dataset_name_for_api,
                    cursor=cursor
                )
                datasets_found = response["datasets"]
                cursor = response.get("next")  # .get() for safety

                for item in datasets_found:
                    if item.get("name") == dataset_name_for_api:
                        selected_dataset = item
                        break
                if selected_dataset or cursor is None:
                    break
            
            if selected_dataset:
                dataset_id = selected_dataset.get("id")
                if dataset_id:
                    task_log.info(
                        f"Existing dataset found: {dataset_id} "
                        f"(API name: {dataset_name_for_api})"
                    )
                    return dataset_id
                else:
                    # ID 없는 경우 오류 처리
                    raise ValueError(f"Dataset {dataset_name_for_api} found but ID is missing: {selected_dataset}")
            else:
                # 데이터셋 생성
                task_log.info(f"Creating dataset by name: {dataset_name_for_api}")
                created_dataset_response = hook.create_dataset(
                    name=dataset_name_for_api,
                    description=f"Automatically generated dataset for {dataset_name_for_api}"
                )
                created_dataset = created_dataset_response  # API 응답 구조에 따라 조정 필요할 수 있음
                dataset_id = created_dataset.get("id")
                if dataset_id:
                    task_log.info(f"Dataset created: {created_dataset}")
                    return dataset_id
                else:
                    # 생성 후 ID 없는 경우 오류 처리
                    raise ValueError(f"{dataset_name_for_api} created dataset is invalid (ID missing)")
                    
        except Exception as e:
            task_log.error(f"Error getting or creating dataset by name {dataset_name_for_api}: {e}")
            raise  # 에러를 다시 발생시켜 태스크 실패 유도

    @task
    def log_final_summary(results: list):
        """배치 처리 결과의 요약을 로깅합니다."""
        task_log = logging.getLogger(__name__)
        total_items_processed = 0
        total_successful = 0
        total_failed = 0
        failed_keys_summary = []

        # expand 결과는 리스트로 전달됨
        if not results:
            task_log.warning("No batch results received. Previous tasks may have failed or not generated results.")
            return
        
        # Airflow는 Iterator 결과를 expand하면 None을 포함한 리스트를 반환할 수 있음
        # 또는 태스크 실패 시 'None'이 포함될 수 있음. 필터링 필요.
        valid_results = [r for r in results if isinstance(r, dict)]
        
        if not valid_results:
            task_log.warning("No valid batch result dictionaries found in the input list.")
            # 모든 배치 태스크가 실패했거나, 이전 태스크에서 빈 리스트가 넘어왔을 수 있음
            return
            
        task_log.info(f"Processing {len(valid_results)} valid batch result(s)...")

        for batch_result in valid_results:
            total_items_processed += batch_result.get("batch_total_items", 0)
            total_successful += batch_result.get("successful_items", 0)
            failed_count = batch_result.get("failed_items_count", 0)
            total_failed += failed_count
            if failed_count > 0:
                failed_keys_summary.extend(batch_result.get("failed_item_details", []))

        task_log.info("--- Final Upload Summary ---")
        task_log.info(f"Total Batches Attempted (based on valid results): {len(valid_results)}")
        task_log.info(f"Total Items Across Batches: {total_items_processed}")
        task_log.info(f"Total Successfully Processed Items: {total_successful}")
        task_log.info(f"Total Failed Items: {total_failed}")
        if total_failed > 0:
            # 너무 많은 실패 키 로깅 방지 (예: 처음 50개만)
            log_limit = 50
            task_log.warning(f"Failed Item Keys/Reasons (showing up to {log_limit}):")
            for i, failed_item in enumerate(failed_keys_summary):
                if i >= log_limit:
                    task_log.warning(f"... and {total_failed - log_limit} more failures.")
                    break
                task_log.warning(f"  - Key: {failed_item.get('key')}, Reason: {failed_item.get('reason')}")
        task_log.info("-----------------------------")

    # --- DAG 흐름 ---
    # 파라미터 추출
    dataset_name_param = "{{ dag_run.conf.get('dataset_name', params.dataset_name) }}"
    batch_size_param = "{{ dag_run.conf.get('batch_size', params.batch_size) }}"

    # 1. 경로 검증
    dataset_path = validate_dataset_path(
        data_root=UPLOAD_DATA_ROOT_PATH,  # Use predefined variable
        dataset_name=dataset_name_param
    )

    # 2. API에서 데이터셋 ID 가져오기/생성하기
    # dataset_path가 None이 아닐 때만 실행되도록 의존성 설정 (TaskFlow가 처리)
    api_dataset_id = get_or_create_dataset(
        dataset_name_for_api=dataset_name_param,
    )

    # 3. 아이템 배치 생성 (Generator) -> 배치 명세 생성으로 변경
    # dataset_path와 api_dataset_id가 유효해야 실행됨 (TaskFlow가 처리)
    list_of_batch_specifications = define_batch_specifications(
        dataset_root_path=dataset_path,
        dataset_name=dataset_name_param,
        batch_size=batch_size_param
    )

    # 4. 배치 업로드 태스크 실행 (Dynamic Task Mapping)
    # item_batches 결과(Iterator)에 대해 expand 적용 -> list_of_batch_specifications 사용
    # upload_results는 각 배치 처리 결과(dict 또는 실패 시 None)를 담은 리스트가 됨
    # list_of_batch_specifications가 비어있으면 expand는 아무 태스크도 생성하지 않음
    upload_results = upload_batch_data_items.partial(
        dataset_api_id=api_dataset_id,
        dataset_root_path=dataset_path,
        dataset_name=dataset_name_param
    ).expand(batch_specification=list_of_batch_specifications)

    # 5. 최종 결과 로깅 (upload_results 리스트를 받아서 처리)
    # upload_results는 list_of_batch_specifications가 비어있을 경우 호출되지 않을 수 있음
    # (Airflow 버전에 따라 동작 상이 가능)
    # 확실히 하려면 BranchPythonOperator 등으로 분기 처리 필요.
    # 여기서는 마지막에 항상 호출되도록 의존성만 설정 (결과 리스트가 비어있을 수 있음)
    log_final_summary(upload_results)