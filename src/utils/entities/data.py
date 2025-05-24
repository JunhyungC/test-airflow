from datetime import datetime
from typing import Tuple, Optional, Dict, Any, List

from .annotation import Annotation
from .data_meta import DataMeta
from .prediction import Prediction
from .scene import Scene
from .content import ContentBase
from pydantic import BaseModel


class Data(BaseModel):
    id: str
    datasetId: str
    sliceIds: Optional[List[str]] = None
    key: Optional[str] = None
    type: Optional[str] = None
    scene: Optional[List[Scene]] = None
    thumbnail: Optional[ContentBase] = None
    annotation: Optional[Annotation] = None
    predictions: Optional[List[Prediction]] = None
    meta: Optional[List[DataMeta]] = None
    systemMeta: Optional[List[DataMeta]] = None
    totalAnnotationCount: Optional[int] = None
    annotationSearch: Optional[List[str]] = None
    createdAt: Optional[datetime] = None
    createdBy: Optional[str] = None
    updatedAt: Optional[datetime] = None
    updatedBy: Optional[str] = None

    class Config:
        validate_assignment = True

    def to_es_document(self) -> Dict[str, Any]:
        datetime_meta, numeric_meta, string_meta = self._split_meta(self.meta)
        system_datetime_meta, system_numeric_meta, system_string_meta, annotation_meta = self._split_meta(self.systemMeta, annotation=True)
        return {
            "id": self.id,
            "datasetId": self.datasetId,
            "key": self.key,
            "type": self.type,
            "sliceIds": self.sliceIds,
            "createdBy": self.createdBy,
            "updatedBy": self.updatedBy,
            "createdAt": self.createdAt,
            "updatedAt": self.updatedAt,
            "scene": [scene.model_dump() if scene else None for scene in self.scene] if self.scene else None,
            "thumbnail": self.thumbnail.model_dump() if self.thumbnail else None,
            "annotation": self.annotation.model_dump() if self.annotation else None,
            "predictions": [prediction.model_dump() if prediction else None for prediction in self.predictions] if self.predictions else None,
            "predictionSetIds": [prediction.setId for prediction in self.predictions] if self.predictions else None,
            "meta": [meta.model_dump() if meta else None for meta in self.meta] if self.meta else None,
            "systemMeta": [meta.model_dump() if meta else None for meta in self.systemMeta] if self.systemMeta else None,
            "datetimeMeta": datetime_meta,
            "numericMeta": numeric_meta,
            "stringMeta": string_meta,
            "systemDatetimeMeta": system_datetime_meta,
            "systemNumericMeta": system_numeric_meta,
            "systemStringMeta": system_string_meta,
            "annotationMeta": annotation_meta,
            "totalAnnotationCount": self.totalAnnotationCount,
            "annotationSearch": self.annotationSearch
        }

    def _split_meta(self, metas: Optional[List[DataMeta]], annotation: bool = False) -> Tuple[List[Dict[str, Any]], ...]:
        datetime_meta = []
        numeric_meta = []
        string_meta = []
        annotation_meta = []

        if metas:
            for meta in metas:
                if annotation and meta.type == "Annotation":
                    if isinstance(meta.value, list):
                        annotation_meta.extend([
                            {
                                "className": data.get("className"),
                                "classCount": data.get("classCount"),
                                "annotationType": data.get("annotationType"),
                                "annoTypeAndClassName": data.get("annoTypeAndClassName")
                            } for data in meta.value
                        ])
                elif meta.type == "DateTime":
                    datetime_meta.append({"key": meta.key, "value": meta.value})
                elif meta.type == "Number":
                    numeric_meta.append({"key": meta.key, "value": meta.value})
                elif meta.type == "String":
                    string_meta.append({"key": meta.key, "value": meta.value})

        if annotation:
            return datetime_meta, numeric_meta, string_meta, annotation_meta
        return datetime_meta, numeric_meta, string_meta
