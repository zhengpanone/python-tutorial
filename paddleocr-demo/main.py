import io
import logging
import os
import warnings
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
from PIL import Image


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="OCR Service", version="1.0.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DET_MODEL_DIR = os.path.join(BASE_DIR, "models", "PP-OCRv5_server_det_infer")
REC_MODEL_DIR = os.path.join(BASE_DIR, "models", "PP-OCRv5_server_rec_infer")


def load_ocr_model() -> Optional[PaddleOCR]:
    try:
        model = PaddleOCR(
            text_detection_model_dir=DET_MODEL_DIR,
            text_recognition_model_dir=REC_MODEL_DIR,
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            device="cpu",
        )
        logger.info("OCR model loaded successfully")
        return model
    except Exception:
        logger.exception("Failed to load OCR model")
        return None


ocr = load_ocr_model()


@app.post("/ocr")
async def recognize(file: UploadFile = File(...)):
    """Recognize text in an uploaded image."""
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Only image files are supported")

    if ocr is None:
        raise HTTPException(status_code=500, detail="OCR model is not loaded")

    try:
        contents = await file.read()
        image = Image.open(io.BytesIO(contents))

        if image.mode != "RGB":
            image = image.convert("RGB")

        result = ocr.predict(np.array(image))
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


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "model_loaded": ocr is not None,
    }


@app.get("/test")
async def test():
    if ocr is None:
        raise HTTPException(status_code=500, detail="OCR model is not loaded")

    img = np.full((100, 100, 3), 255, dtype=np.uint8)
    result = ocr.predict(img)

    return {
        "ok": True,
        "text_count": len(format_ocr_result(result)),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
