# PaddleOCR Demo

基于 FastAPI 和 PaddleOCR 的图片文字识别服务，默认使用本地 `models/` 目录下的 PP-OCRv5 检测与识别模型，并以 CPU 模式运行。

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

Dockerfile 默认使用清华 PyPI 镜像源，并增加了 uv 下载超时和重试次数，适合国内网络环境构建。

构建镜像：

```sh
docker build -t paddleocr-demo .
```

如果需要切换 PyPI 镜像源：

```sh
docker build ^
  --build-arg UV_INDEX_URL=https://mirrors.aliyun.com/pypi/simple ^
  -t paddleocr-demo .
```

运行容器：

```sh
docker run --rm -p 8000:8000 paddleocr-demo
```

服务启动后访问：

```text
http://127.0.0.1:8000
```

## 接口说明

### 健康检查

```sh
curl http://127.0.0.1:8000/health
```

返回示例：

```json
{
  "status": "healthy",
  "model_loaded": true
}
```

### 测试接口

```sh
curl http://127.0.0.1:8000/test
```

### OCR 识别

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

返回字段：

- `success`：是否识别成功
- `filename`：上传文件名
- `text_count`：识别到的文本数量
- `results`：识别结果列表，包含文本、置信度和文本框坐标

## 注意事项

- 当前服务只接受 `image/*` 类型的上传文件。
- Docker 镜像会复制 `models/PP-OCRv5_server_det_infer/` 和 `models/PP-OCRv5_server_rec_infer/`，`.dockerignore` 已排除模型压缩包，避免镜像重复变大。
- 首次加载 OCR 模型可能需要一些时间，请等待服务日志显示模型加载完成。
- 如果构建仍在下载依赖时超时，可以重新执行 `docker build`，Docker 会复用已经完成的构建层。
