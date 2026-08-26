"""payment-api - tier-0 synchronous payment intake.

Production concerns demonstrated here:
  - /healthz vs /readyz mean different things (liveness must not check deps)
  - RED metrics with bounded label cardinality
  - graceful SIGTERM handling for zero-downtime rollouts
  - idempotency keys, because payments must not double-charge
"""
import asyncio
import logging
import os
import signal
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
log = logging.getLogger("payment-api")

SERVICE = os.getenv("SERVICE_NAME", "payment-api")
VERSION = os.getenv("APP_VERSION", "dev")
# Fault injection - Lab C ships a "bad" build by flipping this env var.
ERROR_RATE = float(os.getenv("INJECT_ERROR_RATE", "0"))
LATENCY_MS = int(os.getenv("INJECT_LATENCY_MS", "0"))

# --- metrics -----------------------------------------------------------------
# Labels are LOW cardinality on purpose. Never put payment_id, user_id, or a
# raw path here: every unique combination is a new time series, and that is
# how you OOM a Prometheus. Lab D2 breaks this deliberately.
REQS = Counter("http_requests_total", "HTTP requests", ["method", "route", "status"])
LATENCY = Histogram(
    "http_request_duration_seconds", "Request latency", ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
INFLIGHT = Gauge("http_requests_inflight", "In-flight requests")
BUILD = Gauge("app_build_info", "Build info", ["version", "service"])

_ready = False
_shutting_down = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ready
    BUILD.labels(version=VERSION, service=SERVICE).set(1)
    await asyncio.sleep(float(os.getenv("STARTUP_DELAY_S", "2")))
    _ready = True
    log.info(f"ready version={VERSION}")

    loop = asyncio.get_running_loop()

    def _drain():
        # On SIGTERM: fail readiness FIRST so the endpoint controller pulls us
        # out of rotation, then keep serving in-flight work. This is the other
        # half of the preStop hook - without it, rollouts drop requests.
        global _ready, _shutting_down
        _shutting_down, _ready = True, False
        log.info("SIGTERM received, draining")

    loop.add_signal_handler(signal.SIGTERM, _drain)
    yield
    log.info("shutdown complete")


app = FastAPI(title=SERVICE, lifespan=lifespan)


@app.middleware("http")
async def observe(request: Request, call_next):
    # route (template), not path. "/payments/{id}" is one series;
    # "/payments/abc123" would be one series PER PAYMENT.
    route = request.scope.get("route").path if request.scope.get("route") else "unmatched"
    start = time.perf_counter()
    INFLIGHT.inc()
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception:
        status = 500
        raise
    finally:
        INFLIGHT.dec()
        elapsed = time.perf_counter() - start
        LATENCY.labels(request.method, route).observe(elapsed)
        REQS.labels(request.method, route, str(status)).inc()
    response.headers["X-Request-Id"] = request.headers.get("X-Request-Id", str(uuid.uuid4()))
    return response


class PaymentRequest(BaseModel):
    amount_paise: int = Field(gt=0, le=10_000_000)
    payee_vpa: str = Field(min_length=3, max_length=255)


_idempotency: dict[str, dict] = {}


@app.post("/payments")
async def create_payment(
    body: PaymentRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    if LATENCY_MS:
        await asyncio.sleep(LATENCY_MS / 1000)

    # Replay protection. Without this, a client retry after a timeout charges
    # the customer twice - the single most important property of a payments API.
    if idempotency_key and idempotency_key in _idempotency:
        return {**_idempotency[idempotency_key], "replayed": True}

    import random
    if ERROR_RATE and random.random() < ERROR_RATE:
        raise HTTPException(status_code=500, detail="injected failure")

    result = {
        "payment_id": str(uuid.uuid4()),
        "status": "ACCEPTED",
        "amount_paise": body.amount_paise,
        "version": VERSION,
    }
    if idempotency_key:
        _idempotency[idempotency_key] = result
    return result


@app.get("/healthz")
async def healthz():
    # LIVENESS: "is this process wedged?" Nothing else.
    # A liveness probe that checks Kafka or the DB turns a downstream outage
    # into a cluster-wide restart storm. This is a top-5 production mistake.
    return {"status": "alive"}


@app.get("/readyz")
async def readyz(response: Response):
    # READINESS: "should I receive traffic right now?"
    if not _ready:
        response.status_code = 503
        return {"status": "not-ready", "draining": _shutting_down}
    return {"status": "ready"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
