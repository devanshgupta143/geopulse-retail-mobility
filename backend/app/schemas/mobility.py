"""
===============================================================================
GeoPulse Retail Mobility Analytics - Pydantic v2 Mobility & Spatial Schemas
File: backend/app/schemas/mobility.py
Description: Strongly-typed Pydantic v2 response and request models for H3
             hexagonal index telemetry, store geofence catchments, hourly traffic,
             cannibalization analytics, and GeoJSON FeatureCollections.
===============================================================================
"""

from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from pydantic import BaseModel, Field, field_validator


# =============================================================================
# GeoJSON Base Primitives
# =============================================================================

class GeoJSONPolygonGeometry(BaseModel):
    """GeoJSON Polygon Geometry definition."""
    type: Literal["Polygon"] = "Polygon"
    coordinates: List[List[List[float]]] = Field(
        ...,
        description="Array of linear ring coordinate arrays [[lon, lat], ...]"
    )


class GeoJSONPointGeometry(BaseModel):
    """GeoJSON Point Geometry definition."""
    type: Literal["Point"] = "Point"
    coordinates: List[float] = Field(
        ...,
        description="[longitude, latitude]"
    )


class GeoJSONFeature(BaseModel):
    """Standard GeoJSON Feature representation."""
    type: Literal["Feature"] = "Feature"
    id: Optional[str] = None
    geometry: Union[GeoJSONPolygonGeometry, GeoJSONPointGeometry]
    properties: Dict[str, Any] = Field(default_factory=dict)


class GeoJSONFeatureCollection(BaseModel):
    """Standard GeoJSON FeatureCollection container."""
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[GeoJSONFeature]
    total_features: int = Field(..., description="Count of features in collection")


# =============================================================================
# Store Catchments & Foot-Traffic Schemas
# =============================================================================

class StoreCatchmentFeature(BaseModel):
    """Retail store geofence metadata and 250m circular buffer polygon."""
    store_id: str = Field(..., json_schema_extra={"example": "STORE_A_EXISTING"})
    store_name: str = Field(..., json_schema_extra={"example": "Herald Square Flagship"})
    store_type: str = Field(..., json_schema_extra={"example": "EXISTING"})
    center_latitude: float = Field(..., ge=-90.0, le=90.0)
    center_longitude: float = Field(..., ge=-180.0, le=180.0)
    catchment_radius_meters: float = Field(default=250.0, gt=0.0)
    geometry: GeoJSONPolygonGeometry
    properties: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("center_latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if not (40.70 <= v <= 40.85):
            raise ValueError(f"Latitude {v} lies outside the Manhattan study bounds [40.70, 40.85]")
        return v

    @field_validator("center_longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if not (-74.05 <= v <= -73.90):
            raise ValueError(f"Longitude {v} lies outside the Manhattan study bounds [-74.05, -73.90]")
        return v


class HourlyTrafficPoint(BaseModel):
    """Hourly foot-traffic volume point for temporal profiling."""
    hour: int = Field(..., ge=0, le=23, json_schema_extra={"example": 8})
    visit_count: int = Field(..., ge=0, json_schema_extra={"example": 1240})
    unique_devices: int = Field(..., ge=0, json_schema_extra={"example": 980})
    is_morning_peak: bool = Field(default=False)
    is_evening_peak: bool = Field(default=False)


class StoreTrafficResponse(BaseModel):
    """Comprehensive traffic trends response for a store."""
    store_id: str
    store_name: str
    date_filter: Optional[str] = None
    total_visits: int
    unique_devices: int
    hourly_trends: List[HourlyTrafficPoint]
    peak_morning_visits: int
    peak_evening_visits: int


# =============================================================================
# H3 Hexagonal Cell Metrics Schemas
# =============================================================================

class H3HexMetric(BaseModel):
    """H3 Resolution 9 hexagonal cell mobility density metric."""
    h3_index: str = Field(..., json_schema_extra={"example": "892a100d2b7ffff"})
    resolution: int = Field(default=9, ge=7, le=10)
    total_visits: int = Field(..., ge=0, json_schema_extra={"example": 452})
    unique_devices: int = Field(..., ge=0, json_schema_extra={"example": 312})
    morning_peak_visits: int = Field(default=0, ge=0)
    evening_peak_visits: int = Field(default=0, ge=0)
    centroid_latitude: float
    centroid_longitude: float
    geometry: GeoJSONPolygonGeometry

    @field_validator("h3_index")
    @classmethod
    def validate_h3_index(cls, v: str) -> str:
        clean = v.strip().lower()
        if len(clean) != 15:
            raise ValueError(f"H3 index '{v}' must be a 15-character hexadecimal string")
        try:
            int(clean, 16)
        except ValueError:
            raise ValueError(f"H3 index '{v}' contains invalid hexadecimal characters")
        return clean


class H3HeatmapResponse(BaseModel):
    """Container for Kepler.gl H3 heatmap layers."""
    resolution: int = 9
    total_cells: int
    total_visits_sum: int
    feature_collection: GeoJSONFeatureCollection
    hexagons: List[H3HexMetric]


# =============================================================================
# Cannibalization & Simulation Schemas
# =============================================================================

class CannibalizationSummary(BaseModel):
    """Daily cross-visitation and cannibalization analytics."""
    evaluation_date: Optional[date] = None
    existing_store_id: str = Field(default="STORE_A_EXISTING")
    candidate_store_id: str = Field(default="STORE_B_PROPOSED_5TH")
    existing_store_visitors: int = Field(..., ge=0)
    candidate_store_visitors: int = Field(..., ge=0)
    shared_visitors: int = Field(..., ge=0)
    net_incremental_visitors: int = Field(..., ge=0)
    cannibalization_rate_pct: float = Field(..., ge=0.0, le=100.0)
    cross_shopping_rate_pct: float = Field(..., ge=0.0, le=100.0)
    jaccard_similarity_coefficient: float = Field(..., ge=0.0, le=1.0)
    risk_classification: Literal[
        "HIGH_CANNIBALIZATION_RISK",
        "MODERATE_CANNIBALIZATION_RISK",
        "LOW_CANNIBALIZATION_EXPANSION"
    ]


class SimulationRequest(BaseModel):
    """Payload for what-if retail site candidate simulation."""
    candidate_store_id: str = Field(default="CANDIDATE_CUSTOM", json_schema_extra={"example": "STORE_EXPANSION_BROADWAY"})
    candidate_name: str = Field(default="Broadway Expansion Site")
    latitude: float = Field(..., ge=40.70, le=40.85, json_schema_extra={"example": 40.7535})
    longitude: float = Field(..., ge=-74.05, le=-73.90, json_schema_extra={"example": -73.9860})
    catchment_radius_meters: float = Field(default=250.0, gt=50.0, le=1000.0)
    comparison_store_id: str = Field(default="STORE_A_EXISTING")


class SimulationResponse(BaseModel):
    """Output metrics of a simulated what-if retail location."""
    candidate_store_id: str
    candidate_name: str
    latitude: float
    longitude: float
    catchment_radius_meters: float
    comparison_store_id: str
    distance_to_existing_meters: float
    is_overlapping: bool
    overlap_area_sq_meters: float
    projected_total_footfall: int
    projected_unique_devices: int
    projected_shared_devices: int
    projected_cannibalization_rate_pct: float
    projected_net_new_devices: int
    risk_classification: str
    catchment_geometry: GeoJSONPolygonGeometry
    intersection_geometry: Optional[GeoJSONPolygonGeometry] = None
