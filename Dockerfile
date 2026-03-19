# syntax=docker/dockerfile:1.5
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/app

WORKDIR ${APP_HOME}

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir --upgrade wheel==0.46.2 \
    && pip uninstall -y wheel \
    && rm -rf /usr/local/lib/python*/site-packages/wheel* \
    && rm -rf /usr/local/lib/python*/site-packages/pip/_vendor/wheel* \
    && rm -rf /usr/local/lib/python*/ensurepip/_bundled/wheel*

COPY app ./app
COPY data ./data
COPY README.md ./README.md

RUN mkdir -p data/uploads data/warehouse

RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser ${APP_HOME}

USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
