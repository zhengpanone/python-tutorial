# PaddleOCR Demo

基于 FastAPI、PaddleOCR 和 PaddleOCR-VL 的文字识别与文档解析服务。

- `/ocr`：普通图片 OCR，使用本地 `models/` 目录下的 PP-OCRv5 检测与识别模型。
- `/vl`：PaddleOCR-VL 文档解析，支持图片和 PDF，返回 JSON 与 Markdown 结果。

## 项目结构

```text
.
|-- main.py
|-- pyproject.toml
|-- uv.lock
|-- Dockerfile
`-- models/
    |-- PP-OCRv5_server_det_infer/
    `-- PP-OCRv5_server_rec_infer/
```

## 环境要求

- Python 3.12+
- uv
- Docker 可选

## 本地启动

安装依赖：

```sh
uv sync
```

启动服务：

```sh
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

接口文档地址：

```text
http://127.0.0.1:8000/docs
```

## Docker 启动

Dockerfile 默认使用清华 Debian apt 源、清华 PyPI 镜像源和 PaddlePaddle CPU 包源，并增加了 uv 下载超时和重试次数。

构建镜像：

```powershell
docker build -t paddleocr-demo .
```

如果需要切换 apt 源或 PyPI 镜像源：

```powershell
docker build `
  --build-arg APT_MIRROR=https://mirrors.aliyun.com/debian `
  --build-arg UV_INDEX_URL=https://mirrors.aliyun.com/pypi/simple `
  -t paddleocr-demo .
```

运行容器：

```powershell
docker run --rm -p 8000:8000 paddleocr-demo
```

## 接口说明

### 健康检查

```sh
curl http://127.0.0.1:8000/health
```

返回字段包含：

- `ocr_model_loaded`：普通 OCR 模型是否已加载
- `paddleocr_vl_available`：当前环境是否安装 PaddleOCR-VL
- `paddleocr_vl_loaded`：PaddleOCR-VL 是否已加载

### 普通图片 OCR

PowerShell 示例：

```powershell
curl.exe -X POST `
  "http://127.0.0.1:8000/ocr" `
  -F "file=@D:\test\1.png"
```

Linux/macOS 示例：

```sh
curl -X POST \
  "http://127.0.0.1:8000/ocr" \
  -F "file=@/path/to/image.png"
```

### PaddleOCR-VL 文档解析

解析图片：

```powershell
curl.exe -X POST `
  "http://127.0.0.1:8000/vl" `
  -F "file=@D:\test\page.png"
```

解析 PDF：

```powershell
curl.exe -X POST `
  "http://127.0.0.1:8000/vl" `
  -F "file=@D:\test\demo.pdf"
```

开启跨页重组：

```powershell
curl.exe -X POST `
  "http://127.0.0.1:8000/vl?restructure_pages=true&merge_tables=true&relevel_titles=true" `
  -F "file=@D:\test\demo.pdf"
```

返回字段：

- `success`：是否解析成功
- `filename`：上传文件名
- `page_count`：解析页数
- `json_outputs`：PaddleOCR-VL JSON 结果
- `markdown_outputs`：PaddleOCR-VL Markdown 结果

## PaddleOCR-VL 配置

默认使用 CPU：

```powershell
$env:PADDLEOCR_VL_DEVICE = "cpu"
```

可通过环境变量切换 PaddleOCR-VL 参数：

- `PADDLEOCR_VL_DEVICE`：推理设备，例如 `cpu` 或 `gpu`
- `PADDLEOCR_VL_PIPELINE_VERSION`：Pipeline 版本
- `PADDLEOCR_VL_ENGINE`：推理引擎
- `PADDLEOCR_VL_REC_BACKEND`：VLM 后端
- `PADDLEOCR_VL_REC_SERVER_URL`：VLM 服务地址
- `PADDLEOCR_VL_REC_API_KEY`：VLM 服务密钥
- `PADDLEOCR_VL_REC_API_MODEL_NAME`：VLM 服务模型名
- `PADDLEOCR_VL_REC_MODEL_NAME`：本地 VLM 模型名

## 注意事项

- `/ocr` 只接受 `image/*` 类型的上传文件。
- `/ocr` 当前显式使用 `PP-OCRv5_server_det` 和 `PP-OCRv5_server_rec`，需要与 `models/` 下的本地模型目录匹配。
- `/vl` 支持 PDF 和常见图片格式：PNG、JPG、JPEG、BMP、TIFF、WEBP。
- PaddleOCR-VL 模型较大，第一次调用 `/vl` 时会加载或下载模型，耗时会明显更长。
- Docker 镜像会复制本地 PP-OCRv5 模型目录，`.dockerignore` 已排除模型压缩包，避免镜像重复变大。
- 如果构建仍在 apt 或 Python 依赖下载阶段超时，可以重新执行 `docker build`，Docker 会复用已经完成的构建层。
