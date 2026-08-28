# syntax=docker/dockerfile:1.10

FROM python:3.11-slim-bookworm AS builder

WORKDIR /build
COPY requirements.txt .
# hadolint ignore=DL3042
# BuildKit cache mount keeps pip's cache out of the image layer, so
# --no-cache-dir would only cost us rebuild speed for no size benefit.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-compile --prefix=/install -r requirements.txt

# Must match the builder's Python minor. confluent-kafka ships librdkafka
# as a compiled extension named for the exact interpreter version - a
# mismatch fails at import, not at build. Same class of bug as payment-api.
FROM gcr.io/distroless/python3-debian12:nonroot

USER 65532:65532
WORKDIR /app

COPY --from=builder /install/lib/python3.11/site-packages /app/site-packages
COPY --chown=65532:65532 worker.py /app/worker.py

ENV PYTHONPATH=/app/site-packages \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8080

ENTRYPOINT ["python", "/app/worker.py"]
