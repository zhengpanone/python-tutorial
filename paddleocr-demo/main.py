import io
import json
import logging
import os
import tempfile
import threading
import warnings
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

warnings.filterwarnings(
    "ignore",
    message="No ccache found.*",
    category=UserWarning,
    module=r"paddle\.utils\.cpp_extension\.extension_utils",
)

import numpy as np
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from paddleocr import PaddleOCR

try:
    from paddleocr import PaddleOCRVL
except ImportError:
    PaddleOCRVL = None

from PIL import Image


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="OCR Service", version="1.1.0")

BASE_DIR = Path(__file__).resolve().parent
DET_MODEL_DIR = BASE_DIR / "models" / "PP-OCRv5_server_det_infer"
REC_MODEL_DIR = BASE_DIR / "models" / "PP-OCRv5_server_rec_infer"
DET_MODEL_NAME = "PP-OCRv5_server_det"
REC_MODEL_NAME = "PP-OCRv5_server_rec"
DOCUMENT_SUFFIXES = {".pdf", ".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp"}

ocr_model: Optional[PaddleOCR] = None
ocr_model_lock = threading.Lock()
vl_pipeline: Optional[Any] = None
vl_pipeline_lock = threading.Lock()


def load_ocr_model() -> PaddleOCR:
    global ocr_model

    if ocr_model is not None:
        return ocr_model

    if not DET_MODEL_DIR.exists() or not REC_MODEL_DIR.exists():
        raise RuntimeError(
            "Local PP-OCRv5 model directories are missing. "
            f"Expected: {DET_MODEL_DIR} and {REC_MODEL_DIR}"
        )

    with ocr_model_lock:
        if ocr_model is not None:
            return ocr_model

        ocr_model = PaddleOCR(
            text_detection_model_name=DET_MODEL_NAME,
            text_detection_model_dir=str(DET_MODEL_DIR),
            text_recognition_model_name=REC_MODEL_NAME,
            text_recognition_model_dir=str(REC_MODEL_DIR),
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            device=os.getenv("PADDLEOCR_DEVICE", "cpu"),
        )
        logger.info("OCR model loaded successfully")
        return ocr_model


def load_vl_pipeline() -> Any:
    global vl_pipeline

    if vl_pipeline is not None:
        return vl_pipeline

    if PaddleOCRVL is None:
        raise RuntimeError(
            "PaddleOCRVL is not available. Install paddleocr[doc-parser] first."
        )

    with vl_pipeline_lock:
        if vl_pipeline is not None:
            return vl_pipeline

        kwargs: Dict[str, Any] = {
            "device": os.getenv("PADDLEOCR_VL_DEVICE", "cpu"),
            "use_doc_orientation_classify": _env_bool(
                "PADDLEOCR_VL_USE_DOC_ORIENTATION_CLASSIFY", False
            ),
            "use_doc_unwarping": _env_bool("PADDLEOCR_VL_USE_DOC_UNWARPING", False),
        }
        _set_optional_env(kwargs, "pipeline_version", "PADDLEOCR_VL_PIPELINE_VERSION")
        _set_optional_env(kwargs, "engine", "PADDLEOCR_VL_ENGINE")
        _set_optional_env(kwargs, "vl_rec_backend", "PADDLEOCR_VL_REC_BACKEND")
        _set_optional_env(kwargs, "vl_rec_server_url", "PADDLEOCR_VL_REC_SERVER_URL")
        _set_optional_env(
            kwargs,
            "vl_rec_api_key",
            "PADDLEOCR_VL_REC_API_KEY",
            fallback_env_name="PADDLEOCR_VL_REC_SERVER_API_KEY",
        )
        _set_optional_env(
            kwargs,
            "vl_rec_api_model_name",
            "PADDLEOCR_VL_REC_API_MODEL_NAME",
        )
        _set_optional_env(
            kwargs,
            "vl_rec_model_name",
            "PADDLEOCR_VL_REC_MODEL_NAME",
        )

        vl_pipeline = PaddleOCRVL(**kwargs)
        logger.info("PaddleOCR-VL pipeline loaded successfully")
        return vl_pipeline


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _set_optional_env(
    kwargs: Dict[str, Any],
    key: str,
    env_name: str,
    fallback_env_name: Optional[str] = None,
) -> None:
    value = os.getenv(env_name)
    if value is None and fallback_env_name is not None:
        value = os.getenv(fallback_env_name)
    if value:
        kwargs[key] = value


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "ocr_model_loaded": ocr_model is not None,
        "ocr_model_dirs_exist": DET_MODEL_DIR.exists() and REC_MODEL_DIR.exists(),
        "paddleocr_vl_available": PaddleOCRVL is not None,
        "paddleocr_vl_loaded": vl_pipeline is not None,
    }


@app.post("/ocr")
async def recognize(file: UploadFile = File(...)):
    """Recognize text in an uploaded image with local PP-OCRv5 models."""
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Only image files are supported")

    try:
        model = load_ocr_model()
        contents = await file.read()
        image = Image.open(io.BytesIO(contents))

        if image.mode != "RGB":
            image = image.convert("RGB")

        result = model.predict(np.array(image))
        formatted_result = format_ocr_result(result)

        return JSONResponse(
            content={
                "success": True,
                "filename": file.filename,
                "text_count": len(formatted_result),
                "results": formatted_result,
            }
        )

    except Exception as exc:
        logger.exception("OCR recognition failed")
        raise HTTPException(status_code=500, detail=f"OCR recognition failed: {exc}")


@app.post("/vl")
async def parse_with_paddleocr_vl(
    file: UploadFile = File(...),
    restructure_pages: bool = False,
    merge_tables: bool = False,
    relevel_titles: bool = False,
    concatenate_pages: bool = False,
):
    """Parse an image or PDF with PaddleOCR-VL and return JSON/Markdown outputs."""
    if not _is_supported_document(file):
        raise HTTPException(
            status_code=400,
            detail="Only PDF and common image files are supported",
        )

    try:
        contents = await file.read()
        if not contents:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        pipeline = load_vl_pipeline()

        with tempfile.TemporaryDirectory() as temp_dir:
            work_dir = Path(temp_dir)
            input_path = work_dir / f"input{_file_suffix(file)}"
            output_dir = work_dir / "output"
            output_dir.mkdir()
            input_path.write_bytes(contents)

            results = list(pipeline.predict(input=str(input_path)))
            if restructure_pages:
                results = list(
                    pipeline.restructure_pages(
                        results,
                        merge_tables=merge_tables,
                        relevel_titles=relevel_titles,
                        concatenate_pages=concatenate_pages,
                    )
                )

            for result in results:
                result.save_to_json(save_path=str(output_dir))
                result.save_to_markdown(save_path=str(output_dir))

            return JSONResponse(
                content={
                    "success": True,
                    "filename": file.filename,
                    "page_count": len(results),
                    "json_outputs": _read_json_outputs(output_dir),
                    "markdown_outputs": _read_markdown_outputs(output_dir),
                }
            )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("PaddleOCR-VL parsing failed")
        raise HTTPException(
            status_code=500,
            detail=f"PaddleOCR-VL parsing failed: {exc}",
        )


@app.get("/test")
async def test():
    model = load_ocr_model()
    img = np.full((100, 100, 3), 255, dtype=np.uint8)
    result = model.predict(img)

    return {
        "ok": True,
        "text_count": len(format_ocr_result(result)),
    }


def _is_supported_document(file: UploadFile) -> bool:
    content_type = file.content_type or ""
    if content_type == "application/pdf" or content_type.startswith("image/"):
        return True
    return _file_suffix(file) in DOCUMENT_SUFFIXES


def _file_suffix(file: UploadFile) -> str:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix:
        return suffix

    content_type = file.content_type or ""
    if content_type == "application/pdf":
        return ".pdf"
    if content_type == "image/jpeg":
        return ".jpg"
    if content_type == "image/png":
        return ".png"
    if content_type == "image/webp":
        return ".webp"
    if content_type == "image/tiff":
        return ".tiff"
    return ".bin"


def _read_json_outputs(output_dir: Path) -> List[Dict[str, Any]]:
    outputs: List[Dict[str, Any]] = []
    for path in sorted(output_dir.rglob("*.json")):
        outputs.append(
            {
                "name": path.name,
                "data": json.loads(path.read_text(encoding="utf-8")),
            }
        )
    return outputs


def _read_markdown_outputs(output_dir: Path) -> List[Dict[str, str]]:
    outputs: List[Dict[str, str]] = []
    for path in sorted(output_dir.rglob("*.md")):
        outputs.append(
            {
                "name": path.name,
                "content": path.read_text(encoding="utf-8"),
            }
        )
    return outputs


def format_ocr_result(result: Optional[List[Any]]) -> List[Dict[str, Any]]:
    """Normalize PaddleOCR 3.x and legacy PaddleOCR result shapes."""
    if not result:
        return []

    formatted: List[Dict[str, Any]] = []
    for page_result in result:
        if page_result is None:
            continue

        if isinstance(page_result, dict):
            formatted.extend(_format_paddlex_result(page_result))
            continue

        formatted.extend(_format_legacy_result(page_result))

    return formatted


def _format_paddlex_result(page_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    texts = page_result.get("rec_texts")
    scores = page_result.get("rec_scores")
    boxes = _first_available(page_result, ("rec_polys", "dt_polys", "rec_boxes"))

    texts = [] if texts is None else texts
    scores = [] if scores is None else scores
    boxes = [] if boxes is None else boxes

    formatted: List[Dict[str, Any]] = []
    for index, text in enumerate(texts):
        score = scores[index] if index < len(scores) else None
        box = boxes[index] if index < len(boxes) else None
        formatted.append(
            {
                "text": str(text),
                "confidence": float(score) if score is not None else None,
                "bbox": _format_bbox(box),
            }
        )
    return formatted


def _first_available(data: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return None


def _format_legacy_result(page_result: Iterable[Any]) -> List[Dict[str, Any]]:
    formatted: List[Dict[str, Any]] = []
    for line in page_result:
        if not line or len(line) < 2:
            continue

        bbox = line[0]
        text_info = line[1]
        text = text_info[0] if len(text_info) > 0 else ""
        score = text_info[1] if len(text_info) > 1 else None

        formatted.append(
            {
                "text": str(text),
                "confidence": float(score) if score is not None else None,
                "bbox": _format_bbox(bbox),
            }
        )
    return formatted


def _format_bbox(box: Any) -> Optional[Dict[str, List[float]]]:
    if box is None:
        return None

    points = np.asarray(box, dtype=float).reshape(-1, 2).tolist()
    if len(points) == 2:
        left, top = points[0]
        right, bottom = points[1]
        points = [[left, top], [right, top], [right, bottom], [left, bottom]]

    if len(points) < 4:
        return None

    return {
        "left_top": [float(points[0][0]), float(points[0][1])],
        "right_top": [float(points[1][0]), float(points[1][1])],
        "right_bottom": [float(points[2][0]), float(points[2][1])],
        "left_bottom": [float(points[3][0]), float(points[3][1])],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
