"""
===============================================================================
GeoPulse Retail Mobility Analytics - Store Cannibalization & Simulation Endpoints
File: backend/app/routers/cannibalization.py
Description: Endpoints delivering analytical cannibalization summaries between
             existing and proposed stores, as well as real-time what-if simulations
             for new candidate retail locations in Manhattan.
===============================================================================
"""

from typing import Dict, Optional
from fastapi import APIRouter, HTTPException, Query

from backend.app.db.snowflake_client import snowflake_client
from backend.app.schemas.mobility import (
    CannibalizationSummary,
    SimulationRequest,
    SimulationResponse,
)
from backend.app.services.spatial_service import spatial_service

router = APIRouter(prefix="/cannibalization", tags=["Cannibalization & Simulation"])


@router.get("/summary", response_model=CannibalizationSummary)
def get_cannibalization_summary(
    existing_store_id: str = Query("STORE_A_EXISTING", description="Existing reference store ID"),
    candidate_store_id: str = Query("STORE_B_PROPOSED_5TH", description="Candidate expansion store ID"),
) -> CannibalizationSummary:
    """
    Retrieves cross-visitation rates, shared visitor pool, Jaccard similarity index,
    and business risk tier between Store A (Herald Square) and Store B (5th Ave).
    """
    raw_data = snowflake_client.get_cannibalization_summary()

    # Ensure store identifiers align with query parameters
    raw_data["existing_store_id"] = existing_store_id
    raw_data["candidate_store_id"] = candidate_store_id

    return CannibalizationSummary(**raw_data)


@router.post("/simulate", response_model=SimulationResponse)
def simulate_candidate_site(payload: SimulationRequest) -> SimulationResponse:
    """
    Simulates what-if retail expansion scenarios for a candidate geographic coordinate
    and catchment radius in Manhattan, computing projected visitor overlap,
    cannibalization rate, and incremental net-new audience captures.
    """
    stores_raw = snowflake_client.get_stores_metadata()
    store_map = {s.get("store_id"): s for s in stores_raw}

    comparison_id = payload.comparison_store_id
    comparison_meta = store_map.get(
        comparison_id,
        {"latitude": 40.7549, "longitude": -73.9840, "radius_meters": 250.0}
    )

    exist_lat = float(comparison_meta["latitude"])
    exist_lon = float(comparison_meta["longitude"])
    exist_radius = float(comparison_meta.get("radius_meters", 250.0))

    sim_result = spatial_service.simulate_what_if_candidate(
        candidate_lat=payload.latitude,
        candidate_lon=payload.longitude,
        candidate_radius=payload.catchment_radius_meters,
        existing_store_lat=exist_lat,
        existing_store_lon=exist_lon,
        existing_store_radius=exist_radius,
    )

    return SimulationResponse(
        candidate_store_id=payload.candidate_store_id,
        candidate_name=payload.candidate_name,
        latitude=payload.latitude,
        longitude=payload.longitude,
        catchment_radius_meters=payload.catchment_radius_meters,
        comparison_store_id=comparison_id,
        distance_to_existing_meters=sim_result["distance_to_existing_meters"],
        is_overlapping=sim_result["is_overlapping"],
        overlap_area_sq_meters=sim_result["overlap_area_sq_meters"],
        projected_total_footfall=sim_result["projected_total_footfall"],
        projected_unique_devices=sim_result["projected_unique_devices"],
        projected_shared_devices=sim_result["projected_shared_devices"],
        projected_cannibalization_rate_pct=sim_result["projected_cannibalization_rate_pct"],
        projected_net_new_devices=sim_result["projected_net_new_devices"],
        risk_classification=sim_result["risk_classification"],
        catchment_geometry=sim_result["catchment_geometry"],
        intersection_geometry=sim_result["intersection_geometry"],
    )
