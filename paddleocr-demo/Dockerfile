FROM python:3.12-slim-bookworm

ARG APT_MIRROR=https://mirrors.tuna.tsinghua.edu.cn/debian
ARG UV_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_INDEX_URL=${UV_INDEX_URL} \
    UV_DEFAULT_INDEX=${UV_INDEX_URL} \
    UV_HTTP_TIMEOUT=300 \
    UV_HTTP_RETRIES=10 \
    PIP_INDEX_URL=${UV_INDEX_URL}

WORKDIR /app

RUN sed -i "s|http://deb.debian.org/debian|${APT_MIRROR}|g; s|http://deb.debian.org/debian-security|${APT_MIRROR}-security|g" /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev

COPY . .

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
