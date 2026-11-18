"""
===============================================================================
GeoPulse Retail Mobility Analytics - FastAPI Application Main Entrypoint
File: backend/app/main.py
Description: Main ASGI entrypoint initializing the FastAPI application, CORS
             middleware, process timing headers, API routers, and health checks.
===============================================================================
"""

import time
from datetime import datetime, timezone
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.app.config import settings
from backend.app.routers import cannibalization, hexagons, stores

# Initialize FastAPI Application
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="High-performance geospatial analytics API for retail mobility, H3 hexagonal indexing, and store cannibalization analysis.",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
)

# -----------------------------------------------------------------------------
# CORS Middleware Configuration
# -----------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Process-Time-Ms"],
)


# -----------------------------------------------------------------------------
# Request Latency Benchmark Middleware
# -----------------------------------------------------------------------------
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response: Response = await call_next(request)
    process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
    response.headers["X-Process-Time-Ms"] = str(process_time_ms)
    return response


# -----------------------------------------------------------------------------
# Router Registration
# -----------------------------------------------------------------------------
app.include_router(stores.router, prefix=settings.API_V1_PREFIX)
app.include_router(hexagons.router, prefix=settings.API_V1_PREFIX)
app.include_router(cannibalization.router, prefix=settings.API_V1_PREFIX)


# -----------------------------------------------------------------------------
# Health & Service Discovery Endpoints
# -----------------------------------------------------------------------------
@app.get("/health", tags=["Health & Status"])
def health_check():
    """Service health check endpoint for container probes and load balancers."""
    return {
        "status": "healthy",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "local_fallback_mode": settings.LOCAL_FALLBACK_MODE,
    }


@app.get("/", tags=["Health & Status"])
def root():
    """Root metadata discovery endpoint."""
    return {
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "docs_url": "/docs",
        "health_check": "/health",
        "api_v1": {
            "stores": f"{settings.API_V1_PREFIX}/stores",
            "hexagons": f"{settings.API_V1_PREFIX}/hexagons/heatmap",
            "cannibalization": f"{settings.API_V1_PREFIX}/cannibalization/summary",
        },
    }
