FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install telethon cryptg "python-socks[asyncio]" cryptography

COPY app/ /app/

ENTRYPOINT ["python", "-u", "/app/benchmark.py"]
