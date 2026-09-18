import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
import psycopg2.extras
import redis
from elasticsearch import Elasticsearch
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from prometheus_client import Counter, Histogram, make_asgi_app

from shared.config import ELASTICSEARCH_URL, POSTGRES_URL, REDIS_URL
from shared.models import AlertRecord

TRANSACTIONS_INDEX = "transactions"

http_requests_total = Counter(
    "http_requests_total", "Total HTTP requests", ["endpoint", "status"]
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "HTTP request duration in seconds", ["endpoint"]
)

NUMERIC_STATS_FIELDS = {
    "transaction_count": int,
    "success_count": int,
    "success_rate": float,
    "avg_latency_ms": float,
    "avg_fraud_score": float,
    "volume_per_min": int,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    pg_conn = psycopg2.connect(POSTGRES_URL)
    pg_conn.autocommit = True

    app.state.pg_conn = pg_conn
    app.state.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    app.state.es = Elasticsearch(ELASTICSEARCH_URL)

    yield

    pg_conn.close()
    app.state.redis.close()
    app.state.es.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def track_request_metrics(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start

    # Route path template (e.g. "/merchants/{merchant_id}/stats"), not the
    # raw resolved URL - labeling by the real URL would create a new time
    # series per merchant_id, unbounded cardinality for no benefit. Falls
    # back to the raw path only for unmatched requests (404s), which have
    # no route to read a template from.
    route = request.scope.get("route")
    endpoint = route.path if route else request.url.path

    http_requests_total.labels(endpoint=endpoint, status=response.status_code).inc()
    http_request_duration_seconds.labels(endpoint=endpoint).observe(duration)
    return response


@app.get("/health")
def health():
    try:
        with app.state.pg_conn.cursor() as cur:
            cur.execute("SELECT 1")
        app.state.redis.ping()
        return {"status": "ok", "postgres": "connected", "redis": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {e}")


@app.get("/merchants")
def list_merchants():
    with app.state.pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, name, created_at FROM merchants ORDER BY name")
        return cur.fetchall()


@app.get("/merchants/{merchant_id}/stats")
def get_merchant_stats(merchant_id: str):
    stats = app.state.redis.hgetall(f"stats:{merchant_id}")

    if not stats:
        raise HTTPException(status_code=404, detail=f"No stats found for merchant {merchant_id}")

    return {
        field: NUMERIC_STATS_FIELDS.get(field, str)(value)
        for field, value in stats.items()
    }


@app.get("/transactions/search")
def search_transactions(q: str, merchant_id: Optional[str] = Query(default=None)):
    query: dict = {"bool": {"must": [{"match": {"merchant_name": q}}]}}

    if merchant_id:
        query["bool"]["filter"] = [{"term": {"merchant_id": merchant_id}}]

    response = app.state.es.search(
        index=TRANSACTIONS_INDEX,
        query=query,
        sort=[{"timestamp": {"order": "desc"}}],
        size=20,
    )

    return [hit["_source"] for hit in response["hits"]["hits"]]


@app.get("/merchants/{merchant_id}/alerts/active")
def get_active_alerts(merchant_id: str):
    active = []

    for key in app.state.redis.scan_iter(match=f"alert:{merchant_id}:*"):
        alert = app.state.redis.hgetall(key)
        if alert.get("status") == "FIRING":
            active.append(alert)

    return active


@app.get("/merchants/{merchant_id}/alerts/history", response_model=list[AlertRecord])
def get_alert_history(merchant_id: str):
    with app.state.pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                a.id::text AS alert_id,
                a.merchant_id::text AS merchant_id,
                m.name AS merchant_name,
                a.metric,
                a.current_value,
                a.baseline_value,
                a.z_score,
                a.description,
                a.fired_at,
                a.resolved_at
            FROM alerts a
            JOIN merchants m ON a.merchant_id = m.id
            WHERE a.merchant_id = %s
            ORDER BY a.fired_at DESC
            LIMIT 50
            """,
            (merchant_id,),
        )
        return cur.fetchall()


DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "dashboard"
app.mount("/dashboard", StaticFiles(directory=DASHBOARD_DIR, html=True), name="dashboard")

app.mount("/metrics", make_asgi_app())
