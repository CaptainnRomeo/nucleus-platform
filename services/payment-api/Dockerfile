# syntax=docker/dockerfile:1.10

# ---- builder ----------------------------------------------------------------
FROM python:3.11-slim-bookworm AS builder
# Cache mount: pip's cache persists across builds without landing in a layer.
# Layer ordering matters - requirements.txt copied alone so a code change
# does not invalidate the dependency install.
WORKDIR /build
COPY requirements.txt .

# hadolint ignore=DL3042
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-compile --prefix=/install -r requirements.txt

# ---- runtime ----------------------------------------------------------------
# Distroless: no shell, no package manager, no busybox. An attacker with RCE
# gets no tooling. Also removes ~100 CVEs that live in base-image OS packages
# and have nothing to do with your app.
FROM gcr.io/distroless/python3-debian12:nonroot

# UID 65532 = "nonroot" in distroless. Numeric, not a name: runAsNonRoot
# cannot verify a username, only a UID.
USER 65532:65532
WORKDIR /app

COPY --from=builder /install/lib/python3.11/site-packages /app/site-packages
COPY --chown=65532:65532 main.py /app/main.py

ENV PYTHONPATH=/app/site-packages \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8080

# No shell in distroless, so exec form is mandatory - and it is correct anyway:
# shell form makes the shell PID 1, which swallows SIGTERM and breaks graceful
# shutdown. This is why containers "take 30 seconds to stop".
ENTRYPOINT ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
