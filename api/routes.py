"""FastAPI route definitions for the feature serving API."""

import json
import logging
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Response
import jsonschema
from pydantic import BaseModel, Field

from api import metrics as m

logger = logging.getLogger("api.routes")

router = APIRouter()

# Load schema for validation
_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "telemetry_v1.json"
try:
    with open(_SCHEMA_PATH) as _f:
        _SCHEMA = json.load(_f)
    _schema_validator = jsonschema.Draft7Validator(_SCHEMA)
except Exception as _e:
    logger.warning(f"Failed to load telemetry schema at {_SCHEMA_PATH}: {_e}")
    _schema_validator = None


# ------------------------------------------------------------------
# Request / Response models
# ------------------------------------------------------------------

class FeatureResponse(BaseModel):
    entity_id: str
    event_id: str
    timestamp: str
    feature_vector: list[float]
    source: str = Field(description="'cache' or 'database'")


class BatchRequest(BaseModel):
    entity_ids: list[str] = Field(
        ...,
        min_length=1,
        max_length=500,
        description="List of entity IDs to fetch (max 500)",
    )


class BatchResponse(BaseModel):
    results: dict[str, Optional[FeatureResponse]]
    found: int
    not_found: int


class HealthResponse(BaseModel):
    status: str
    redis: str
    postgres: str


class IngestTelemetryResponse(BaseModel):
    status: str
    event_id: Optional[str] = None
    partition: Optional[int] = None
    message: Optional[str] = None


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

@router.get("/features/{entity_id}", response_model=FeatureResponse)
async def get_features(entity_id: str, request: Request):
    """Return the latest feature vector for a single entity."""
    start = time.monotonic()
    cache = request.app.state.cache

    data = cache.get_features(entity_id)
    latency = time.monotonic() - start

    if data is None:
        m.REQUEST_LATENCY.labels(
            method="GET",
            endpoint="/features/{entity_id}",
            status_code="404",
        ).observe(latency)
        raise HTTPException(status_code=404, detail=f"Entity '{entity_id}' not found")

    source = data.get("source")
    if not source:
        source = "database" if not getattr(cache, "cache_enabled", True) else "cache"

    m.REQUEST_LATENCY.labels(
        method="GET",
        endpoint="/features/{entity_id}",
        status_code="200",
    ).observe(latency)

    return FeatureResponse(
        entity_id=data["entity_id"],
        event_id=str(data["event_id"]),
        timestamp=str(data["timestamp"]),
        feature_vector=list(data["feature_vector"]),
        source=source,
    )


@router.post("/features/batch", response_model=BatchResponse)
async def get_features_batch(body: BatchRequest, request: Request):
    """Return latest feature vectors for a batch of entities."""
    start = time.monotonic()
    cache = request.app.state.cache

    results_raw = cache.get_features_batch(body.entity_ids)
    latency = time.monotonic() - start

    m.REQUEST_LATENCY.labels(
        method="POST",
        endpoint="/features/batch",
        status_code="200",
    ).observe(latency)

    results: dict[str, Optional[FeatureResponse]] = {}
    found = 0
    not_found = 0

    for eid, data in results_raw.items():
        if data is not None:
            source = data.get("source")
            if not source:
                source = "database" if not getattr(cache, "cache_enabled", True) else "cache"
            results[eid] = FeatureResponse(
                entity_id=data["entity_id"],
                event_id=str(data["event_id"]),
                timestamp=str(data["timestamp"]),
                feature_vector=list(data["feature_vector"]),
                source=source,
            )
            found += 1
        else:
            results[eid] = None
            not_found += 1

    return BatchResponse(results=results, found=found, not_found=not_found)


@router.get("/health", response_model=HealthResponse)
async def health(request: Request):
    """Check connectivity to Redis and Postgres."""
    cache = request.app.state.cache
    status = cache.health()
    overall = "ok" if all(v in ("ok", "disabled") for v in status.values()) else "degraded"
    return HealthResponse(
        status=overall,
        redis=status.get("redis", "unknown"),
        postgres=status.get("postgres", "unknown"),
    )


@router.post("/api/v1/telemetry/ingest", response_model=IngestTelemetryResponse)
async def ingest_telemetry(payload: dict, response: Response):
    """Validate incoming telemetry event against schema and return routing outcome."""
    if _schema_validator is not None:
        errors = list(_schema_validator.iter_errors(payload))
        if errors:
            reasons = "; ".join(e.message[:100] for e in errors[:3])
            response.status_code = 400
            return IngestTelemetryResponse(
                status="rejected",
                message=f"schema_violation: {reasons}",
            )

    entity_id = str(payload.get("entity_id", ""))
    part = abs(hash(entity_id)) % 12
    return IngestTelemetryResponse(
        status="ingested",
        event_id=payload.get("event_id"),
        partition=part,
        message="Telemetry event successfully ingested and routed",
    )
