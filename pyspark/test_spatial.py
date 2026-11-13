"""
GeoPulse Retail Mobility Analytics - Spatial Test Suite
=======================================================
Unit tests using pytest to validate:
- H3 hexagonal indexing bounds, resolution fidelity, and edge-case coordinate handling.
- Haversine geodesic distance calculation accuracy, symmetry, and triangle inequality.
- Store geofence catchment threshold boundary logic (250-meter threshold).
"""

import math
import sys
from pathlib import Path

import h3
import numpy as np
import pytest

# Ensure pyspark module directory is on sys.path
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

from spatial_indexing import (
    CATCHMENT_RADIUS_METERS,
    EARTH_RADIUS_METERS,
    H3_RESOLUTION,
    haversine_distance_vectorized,
    latlng_to_h3,
    load_store_geofences,
)


# ==========================================
# FIXTURES & REFERENCE COORDINATES
# ==========================================
@pytest.fixture
def store_a_coords() -> tuple[float, float]:
    """STORE_A_EXISTING: Herald Square Flagship."""
    return (40.7549, -73.9840)


@pytest.fixture
def store_b_coords() -> tuple[float, float]:
    """STORE_B_PROPOSED_5TH: Fifth Avenue Luxury Corridor."""
    return (40.7565, -73.9815)


@pytest.fixture
def store_c_coords() -> tuple[float, float]:
    """STORE_C_ALT_WEST: West 34th District Alternative."""
    return (40.7510, -73.9930)


@pytest.fixture
def geofences_dict() -> dict:
    """Loads store geofence definitions from JSON."""
    json_path = current_dir / "store_geofences.json"
    return load_store_geofences(json_path)


# ==========================================
# TEST SUITE: H3 HEXAGONAL INDEXING
# ==========================================
class TestH3Indexing:
    """Validates H3 hexagonal indexing across standard, boundary, and edge coordinates."""

    def test_h3_resolution_and_format(self, store_a_coords):
        """Validates that resolution 9 produces a valid 15-character hex string."""
        lat, lon = store_a_coords
        cell = latlng_to_h3(lat, lon, resolution=9)
        assert isinstance(cell, str)
        assert len(cell) == 15
        assert cell.startswith("89")

        # Verify H3 resolution property
        if hasattr(h3, "h3_get_resolution"):
            res = h3.h3_get_resolution(cell)
        else:
            res = h3.get_resolution(cell)
        assert res == 9

    def test_h3_coordinate_roundtrip(self, store_a_coords):
        """Validates that cell centroid roundtrips back within the resolution 9 bounding area."""
        lat, lon = store_a_coords
        cell = latlng_to_h3(lat, lon, resolution=9)

        if hasattr(h3, "h3_to_geo"):
            centroid_lat, centroid_lon = h3.h3_to_geo(cell)
        else:
            centroid_lat, centroid_lon = h3.cell_to_latlng(cell)

        # Great-circle distance from anchor to its hex centroid should be within the H3 res 9 circumradius (< 200m)
        dist = haversine_distance_vectorized(
            np.array([lat]), np.array([lon]), centroid_lat, centroid_lon
        )[0]
        assert dist < 200.0

        # Centroid should index back to the exact same H3 cell
        centroid_cell = latlng_to_h3(centroid_lat, centroid_lon, resolution=9)
        assert centroid_cell == cell

    @pytest.mark.parametrize(
        "lat,lon,expected_prefix",
        [
            (0.0, 0.0, "89"),            # Null Island
            (0.0, -73.9840, "89"),        # Equator + Midtown Longitude
            (85.0, 0.0, "89"),           # Arctic boundary
            (-85.0, 0.0, "89"),          # Antarctic boundary
            (40.7549, 179.9999, "89"),   # Eastern Antimeridian boundary
            (40.7549, -179.9999, "89"),  # Western Antimeridian boundary
        ],
    )
    def test_h3_edge_case_coordinates(self, lat, lon, expected_prefix):
        """Validates H3 indexing across global topological edge cases."""
        cell = latlng_to_h3(lat, lon, resolution=9)
        assert isinstance(cell, str)
        assert len(cell) == 15
        assert cell.startswith(expected_prefix)

    def test_h3_nearby_points_clustering(self, store_a_coords):
        """Points within 5 meters should fall in the same or immediate neighbor hexagon."""
        lat, lon = store_a_coords
        # 5 meters north: delta_lat ~ 5 / 111132
        lat_nearby = lat + (5.0 / 111132.0)
        cell_1 = latlng_to_h3(lat, lon, resolution=9)
        cell_2 = latlng_to_h3(lat_nearby, lon, resolution=9)

        if hasattr(h3, "k_ring"):
            neighbors = set(h3.k_ring(cell_1, 1))
        else:
            neighbors = set(h3.grid_disk(cell_1, 1))

        assert cell_2 in neighbors


# ==========================================
# TEST SUITE: CATCHMENT & DISTANCE LOGIC
# ==========================================
class TestDistanceCalculations:
    """Validates geodesic Haversine distance computations and catchment bounds."""

    def test_zero_distance_at_origin(self, store_a_coords):
        """Distance from a point to itself must be strictly 0.0 meters."""
        lat, lon = store_a_coords
        dist = haversine_distance_vectorized(np.array([lat]), np.array([lon]), lat, lon)[0]
        assert pytest.approx(dist, abs=1e-4) == 0.0

    def test_distance_symmetry(self, store_a_coords, store_b_coords):
        """Haversine distance must be symmetric: dist(A, B) == dist(B, A)."""
        lat_a, lon_a = store_a_coords
        lat_b, lon_b = store_b_coords

        dist_ab = haversine_distance_vectorized(np.array([lat_a]), np.array([lon_a]), lat_b, lon_b)[0]
        dist_ba = haversine_distance_vectorized(np.array([lat_b]), np.array([lon_b]), lat_a, lon_a)[0]
        assert pytest.approx(dist_ab, rel=1e-5) == dist_ba

    def test_triangle_inequality(self, store_a_coords, store_b_coords, store_c_coords):
        """Geodesic distance must satisfy the triangle inequality: dist(A, C) <= dist(A, B) + dist(B, C)."""
        lat_a, lon_a = store_a_coords
        lat_b, lon_b = store_b_coords
        lat_c, lon_c = store_c_coords

        dist_ac = haversine_distance_vectorized(np.array([lat_a]), np.array([lon_a]), lat_c, lon_c)[0]
        dist_ab = haversine_distance_vectorized(np.array([lat_a]), np.array([lon_a]), lat_b, lon_b)[0]
        dist_bc = haversine_distance_vectorized(np.array([lat_b]), np.array([lon_b]), lat_c, lon_c)[0]

        assert dist_ac <= (dist_ab + dist_bc + 1e-4)

    def test_interstore_distance_exceeds_catchment(self, store_a_coords, store_b_coords):
        """Store A and Store B are distinct anchors separated by ~270m-290m (outside 250m buffer)."""
        lat_a, lon_a = store_a_coords
        lat_b, lon_b = store_b_coords

        dist_ab = haversine_distance_vectorized(np.array([lat_a]), np.array([lon_a]), lat_b, lon_b)[0]
        assert 250.0 < dist_ab < 350.0

    @pytest.mark.parametrize(
        "offset_meters,should_be_in_catchment",
        [
            (0.0, True),      # Anchor center
            (50.0, True),     # Deep inside catchment
            (240.0, True),    # Inside perimeter
            (249.5, True),    # Just inside 250m threshold
            (250.5, False),   # Just outside 250m threshold
            (300.0, False),   # Clearly outside catchment
            (1000.0, False),  # 1km away
        ],
    )
    def test_catchment_radius_boundary_evaluation(
        self, store_a_coords, offset_meters, should_be_in_catchment
    ):
        """Validates threshold boundary classification around the 250-meter perimeter."""
        lat, lon = store_a_coords
        # Offset north: delta_lat = offset_meters / (EARTH_RADIUS * pi / 180)
        meters_per_deg_lat = 111132.95
        lat_offset = lat + (offset_meters / meters_per_deg_lat)

        dist = haversine_distance_vectorized(
            np.array([lat_offset]), np.array([lon]), lat, lon
        )[0]
        in_catchment = dist <= CATCHMENT_RADIUS_METERS

        assert in_catchment == should_be_in_catchment
        assert pytest.approx(dist, abs=1.0) == offset_meters


# ==========================================
# TEST SUITE: STORE GEOFENCE CONFIG VALIDATION
# ==========================================
class TestStoreGeofenceConfig:
    """Validates store geofence definitions and configuration schema."""

    def test_geofence_stores_presence(self, geofences_dict):
        """Validates all required stores are present in configuration."""
        expected_stores = {"STORE_A_EXISTING", "STORE_B_PROPOSED_5TH", "STORE_C_ALT_WEST"}
        assert set(geofences_dict.keys()) == expected_stores

    def test_geofence_coordinates_manhattan_bounds(self, geofences_dict):
        """Validates that all store anchors fall inside Midtown Manhattan bounding box."""
        for store_id, store in geofences_dict.items():
            assert 40.740 <= store["lat"] <= 40.770, f"Lat out of bounds for {store_id}"
            assert -74.010 <= store["lon"] <= -73.970, f"Lon out of bounds for {store_id}"
            assert store["catchment_radius_meters"] == 250.0
