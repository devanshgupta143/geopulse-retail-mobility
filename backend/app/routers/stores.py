"""
===============================================================================
GeoPulse Retail Mobility Analytics - Store Catchment & Traffic Endpoints
File: backend/app/routers/stores.py
Description: FastAPI REST endpoints for retrieving retail store geofence boundaries,
             250m circular catchment GeoJSON features, and hourly foot-traffic trends.
===============================================================================
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query

from backend.app.db.snowflake_client import snowflake_client
from backend.app.schemas.mobility import (
    GeoJSONFeature,
    GeoJSONFeatureCollection,
    HourlyTrafficPoint,
    StoreCatchmentFeature,
    StoreTrafficResponse,
)
from backend.app.services.spatial_service import spatial_service

router = APIRouter(prefix="/stores", tags=["Stores & Catchments"])


@router.get("", response_model=GeoJSONFeatureCollection)
def get_store_catchments() -> GeoJSONFeatureCollection:
    """
    Retrieves all configured retail store geofences formatted as a GeoJSON FeatureCollection.
    Each feature contains a 250-meter geodesic circular buffer polygon and store metadata.
    """
    stores_raw = snowflake_client.get_stores_metadata()
    features: List[GeoJSONFeature] = []

    for s in stores_raw:
        store_id = s.get("store_id")
        store_name = s.get("store_name") or s.get("name") or store_id
        store_type = s.get("store_type") or s.get("status") or "EXISTING"
        lat = float(s.get("latitude") or s.get("lat"))
        lon = float(s.get("longitude") or s.get("lon"))
        radius = float(s.get("catchment_radius_meters") or s.get("radius_meters") or 250.0)

        # Generate 250m circular buffer polygon
        catchment_poly = spatial_service.create_circular_catchment_polygon(
            center_lat=lat,
            center_lon=lon,
            radius_meters=radius,
        )

        props = {
            "store_id": store_id,
            "store_name": store_name,
            "store_type": store_type,
            "center_latitude": lat,
            "center_longitude": lon,
            "catchment_radius_meters": radius,
        }

        features.append(
            GeoJSONFeature(
                id=store_id,
                geometry=catchment_poly,
                properties=props,
            )
        )

    return GeoJSONFeatureCollection(
        features=features,
        total_features=len(features),
    )


@router.get("/{store_id}/traffic", response_model=StoreTrafficResponse)
def get_store_traffic_trends(
    store_id: str,
    date_filter: Optional[str] = Query(None, description="Optional date filter YYYY-MM-DD"),
) -> StoreTrafficResponse:
    """
    Retrieves hourly foot-traffic trends, visit counts, and unique device volumes
    for a specific retail store, highlighting morning and evening transit peak periods.
    """
    stores_raw = snowflake_client.get_stores_metadata()
    store_lookup = {s.get("store_id"): s for s in stores_raw}

    if store_id not in store_lookup:
        raise HTTPException(
            status_code=404,
            detail=f"Store '{store_id}' not found. Valid stores: {list(store_lookup.keys())}",
        )

    store_meta = store_lookup[store_id]
    hourly_data = snowflake_client.get_store_hourly_traffic(store_id, date_filter)

    total_visits = sum(h["visit_count"] for h in hourly_data)
    unique_devices = sum(h["unique_devices"] for h in hourly_data) // 2  # De-duplicate across hours

    peak_morning = sum(h["visit_count"] for h in hourly_data if h["is_morning_peak"])
    peak_evening = sum(h["visit_count"] for h in hourly_data if h["is_evening_peak"])

    points = [HourlyTrafficPoint(**h) for h in hourly_data]

    return StoreTrafficResponse(
        store_id=store_id,
        store_name=store_meta.get("store_name") or store_meta.get("name") or store_id,
        date_filter=date_filter,
        total_visits=total_visits,
        unique_devices=unique_devices,
        hourly_trends=points,
        peak_morning_visits=peak_morning,
        peak_evening_visits=peak_evening,
    )
