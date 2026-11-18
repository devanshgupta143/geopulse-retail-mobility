"""
===============================================================================
GeoPulse Retail Mobility Analytics - FastAPI Integration & Spatial Test Suite
File: backend/tests/test_api.py
Description: End-to-end integration tests using FastAPI TestClient and pytest to
             validate health probes, store geofences, H3 heatmaps, cannibalization
             summaries, what-if site simulation, and GeoJSON polygon geometry schemas.
===============================================================================
"""

import math
import pytest
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.spatial_service import spatial_service

client = TestClient(app)


# =============================================================================
# 1. Health & Discovery Endpoints
# =============================================================================

def test_health_check():
    """Validates service health check endpoint and latency benchmark header."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "service" in data
    assert "version" in data
    assert "X-Process-Time-Ms" in response.headers


def test_root_metadata():
    """Validates root discovery endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "api_v1" in data
    assert "docs_url" in data


# =============================================================================
# 2. Store Catchment & Traffic Endpoints
# =============================================================================

def test_get_store_catchments_geojson():
    """Validates GeoJSON FeatureCollection containing store circular catchments."""
    response = client.get("/api/v1/stores")
    assert response.status_code == 200
    data = response.json()

    assert data["type"] == "FeatureCollection"
    assert data["total_features"] >= 3
    features = data["features"]
    assert len(features) >= 3

    store_ids = [f["id"] for f in features]
    assert "STORE_A_EXISTING" in store_ids
    assert "STORE_B_PROPOSED_5TH" in store_ids

    # Validate first feature geometry
    f1 = features[0]
    assert f1["type"] == "Feature"
    assert f1["geometry"]["type"] == "Polygon"
    coords = f1["geometry"]["coordinates"][0]
    assert len(coords) >= 48
    # Closed polygon ring
    assert coords[0] == coords[-1]


def test_get_store_traffic_trends():
    """Validates hourly pedestrian foot-traffic volume endpoint."""
    response = client.get("/api/v1/stores/STORE_A_EXISTING/traffic")
    assert response.status_code == 200
    data = response.json()

    assert data["store_id"] == "STORE_A_EXISTING"
    assert data["total_visits"] > 0
    assert len(data["hourly_trends"]) == 24

    # Verify peak morning detection
    morning_pts = [h for h in data["hourly_trends"] if h["hour"] in (7, 8, 9)]
    for pt in morning_pts:
        assert pt["is_morning_peak"] is True


def test_get_store_traffic_not_found():
    """Validates 404 response for invalid store identifier."""
    response = client.get("/api/v1/stores/NON_EXISTENT_STORE/traffic")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


# =============================================================================
# 3. H3 Hexagonal Heatmap Endpoints
# =============================================================================

def test_get_h3_heatmap():
    """Validates H3 resolution 9 heatmap dataset formatted for Kepler.gl."""
    response = client.get("/api/v1/hexagons/heatmap?resolution=9&limit=50")
    assert response.status_code == 200
    data = response.json()

    assert data["resolution"] == 9
    assert data["total_cells"] > 0
    assert len(data["hexagons"]) > 0

    hex0 = data["hexagons"][0]
    assert len(hex0["h3_index"]) == 15
    assert hex0["total_visits"] > 0
    assert hex0["geometry"]["type"] == "Polygon"

    # Validate Kepler.gl GeoJSON elevation property
    fc = data["feature_collection"]
    assert fc["type"] == "FeatureCollection"
    feat0 = fc["features"][0]
    assert "elevation" in feat0["properties"]
    assert feat0["properties"]["elevation"] == hex0["total_visits"]


def test_get_h3_cell_inspection_valid():
    """Validates single H3 hexagon inspection endpoint."""
    test_h3 = "892a100d66bffff"
    response = client.get(f"/api/v1/hexagons/{test_h3}")
    assert response.status_code == 200
    data = response.json()
    assert data["h3_index"] == test_h3
    assert data["resolution"] == 9
    assert data["geometry"]["type"] == "Polygon"


def test_get_h3_cell_inspection_invalid():
    """Validates 400 response for corrupted H3 hexadecimal string."""
    response = client.get("/api/v1/hexagons/not-a-valid-h3-index")
    assert response.status_code == 400
    assert "invalid" in response.json()["detail"].lower()


# =============================================================================
# 4. Cannibalization & Simulation Endpoints
# =============================================================================

def test_get_cannibalization_summary():
    """Validates existing store cross-shopping and cannibalization summary."""
    response = client.get("/api/v1/cannibalization/summary")
    assert response.status_code == 200
    data = response.json()

    assert data["existing_store_id"] == "STORE_A_EXISTING"
    assert data["candidate_store_id"] == "STORE_B_PROPOSED_5TH"
    assert data["cannibalization_rate_pct"] >= 0.0
    assert data["jaccard_similarity_coefficient"] >= 0.0
    assert data["risk_classification"] in [
        "HIGH_CANNIBALIZATION_RISK",
        "MODERATE_CANNIBALIZATION_RISK",
        "LOW_CANNIBALIZATION_EXPANSION",
    ]


def test_post_cannibalization_simulate_overlapping():
    """Validates candidate site simulation with overlapping catchment."""
    payload = {
        "candidate_store_id": "STORE_TEST_5TH",
        "candidate_name": "5th Ave Expansion",
        "latitude": 40.7565,
        "longitude": -73.9815,
        "catchment_radius_meters": 250.0,
        "comparison_store_id": "STORE_A_EXISTING",
    }
    response = client.post("/api/v1/cannibalization/simulate", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["candidate_store_id"] == "STORE_TEST_5TH"
    assert data["is_overlapping"] is True
    assert data["distance_to_existing_meters"] < 500.0
    assert data["projected_shared_devices"] > 0
    assert data["projected_cannibalization_rate_pct"] > 0.0
    assert data["intersection_geometry"] is not None


def test_post_cannibalization_simulate_non_overlapping():
    """Validates candidate site simulation with distant non-overlapping catchment."""
    payload = {
        "candidate_store_id": "STORE_DISTANT_SITE",
        "candidate_name": "Distant Manhattan Site",
        "latitude": 40.7100,  # Far south near Lower Manhattan
        "longitude": -74.0100,
        "catchment_radius_meters": 200.0,
        "comparison_store_id": "STORE_A_EXISTING",
    }
    response = client.post("/api/v1/cannibalization/simulate", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["is_overlapping"] is False
    assert data["distance_to_existing_meters"] > 2000.0
    assert data["projected_shared_devices"] == 0
    assert data["projected_cannibalization_rate_pct"] == 0.0
    assert data["risk_classification"] == "LOW_CANNIBALIZATION_EXPANSION"


# =============================================================================
# 5. Spatial Service Unit Calculations
# =============================================================================

def test_haversine_distance_calculation():
    """Verifies spherical Haversine distance accuracy."""
    # Herald Sq (40.7549, -73.9840) to 5th Ave (40.7565, -73.9815) ~275 meters
    dist = spatial_service.calculate_haversine_distance(40.7549, -73.9840, 40.7565, -73.9815)
    assert 260.0 <= dist <= 290.0


def test_catchment_polygon_closure():
    """Verifies geodesic circular polygon vertex count and ring closure."""
    poly = spatial_service.create_circular_catchment_polygon(40.7549, -73.9840, 250.0, num_vertices=32)
    coords = poly.coordinates[0]
    assert len(coords) == 33
    assert coords[0] == coords[-1]
