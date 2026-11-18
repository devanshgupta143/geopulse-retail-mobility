"""
===============================================================================
GeoPulse Retail Mobility Analytics - Geospatial & H3 Boundary Service
File: backend/app/services/spatial_service.py
Description: Spatial calculations, H3 hexagonal polygon construction, geodesic
             circular geofence generation (250m catchments), Shapely polygon
             intersection, and retail what-if cannibalization simulation.
===============================================================================
"""

import math
from typing import Any, Dict, List, Optional, Tuple

import h3
from shapely.geometry import Point, Polygon, mapping
from shapely.ops import transform

from backend.app.schemas.mobility import (
    GeoJSONFeature,
    GeoJSONFeatureCollection,
    GeoJSONPolygonGeometry,
    StoreCatchmentFeature,
)

# Mean Earth Radius in meters
EARTH_RADIUS_METERS = 6371000.0


class SpatialService:
    """Service providing spatial indexing, geometry generation, and overlap evaluation."""

    @staticmethod
    def calculate_haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculates great-circle distance between two coordinates in meters.
        """
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = (
            math.sin(delta_phi / 2.0) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
        )
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return round(EARTH_RADIUS_METERS * c, 2)

    @staticmethod
    def h3_to_polygon_geometry(h3_index: str) -> GeoJSONPolygonGeometry:
        """
        Converts an H3 index into a GeoJSON Polygon geometry.
        h3.h3_to_geo_boundary returns coordinates in (lon, lat) tuples when geo_json=True.
        """
        raw_boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
        # Format: [[[lon, lat], [lon, lat], ...]]
        coordinates = [[list(pt) for pt in raw_boundary]]
        # Ensure polygon ring is closed
        if coordinates[0][0] != coordinates[0][-1]:
            coordinates[0].append(coordinates[0][0])
        return GeoJSONPolygonGeometry(coordinates=coordinates)

    @staticmethod
    def h3_to_centroid(h3_index: str) -> Tuple[float, float]:
        """Returns the (latitude, longitude) center of an H3 hex cell."""
        lat, lon = h3.h3_to_geo(h3_index)
        return round(lat, 6), round(lon, 6)

    @classmethod
    def create_circular_catchment_polygon(
        cls,
        center_lat: float,
        center_lon: float,
        radius_meters: float = 250.0,
        num_vertices: int = 48,
    ) -> GeoJSONPolygonGeometry:
        """
        Generates a geodesic circular geofence polygon matching the retail catchment threshold.
        """
        ring_coords: List[List[float]] = []
        lat_rad = math.radians(center_lat)

        # Approximate degrees per meter around latitude
        meters_per_deg_lat = 111132.92 - 559.82 * math.cos(2 * lat_rad)
        meters_per_deg_lon = 111412.84 * math.cos(lat_rad)

        for i in range(num_vertices):
            angle = 2.0 * math.pi * (i / num_vertices)
            dx = radius_meters * math.sin(angle)
            dy = radius_meters * math.cos(angle)

            pt_lon = center_lon + (dx / meters_per_deg_lon)
            pt_lat = center_lat + (dy / meters_per_deg_lat)
            ring_coords.append([round(pt_lon, 6), round(pt_lat, 6)])

        # Close polygon ring
        ring_coords.append(ring_coords[0])

        return GeoJSONPolygonGeometry(coordinates=[ring_coords])

    @classmethod
    def compute_polygon_intersection(
        cls,
        poly1_geom: GeoJSONPolygonGeometry,
        poly2_geom: GeoJSONPolygonGeometry,
        ref_lat: float = 40.7549,
    ) -> Tuple[bool, float, Optional[GeoJSONPolygonGeometry]]:
        """
        Evaluates geometric intersection between two polygons using Shapely,
        returning overlap status, area in square meters, and intersection geometry.
        """
        s_poly1 = Polygon(poly1_geom.coordinates[0])
        s_poly2 = Polygon(poly2_geom.coordinates[0])

        if not s_poly1.intersects(s_poly2):
            return False, 0.0, None

        intersection = s_poly1.intersection(s_poly2)
        if intersection.is_empty or intersection.area == 0:
            return False, 0.0, None

        # Conversion from deg^2 to m^2 in Manhattan
        lat_rad = math.radians(ref_lat)
        m_per_deg_lat = 111132.92 - 559.82 * math.cos(2 * lat_rad)
        m_per_deg_lon = 111412.84 * math.cos(lat_rad)
        sq_meters_per_sq_deg = m_per_deg_lat * m_per_deg_lon

        overlap_area_m2 = round(intersection.area * sq_meters_per_sq_deg, 2)

        inter_geom = None
        if intersection.geom_type == "Polygon":
            inter_coords = [[list(pt) for pt in intersection.exterior.coords]]
            inter_geom = GeoJSONPolygonGeometry(coordinates=inter_coords)

        return True, overlap_area_m2, inter_geom

    @classmethod
    def simulate_what_if_candidate(
        cls,
        candidate_lat: float,
        candidate_lon: float,
        candidate_radius: float = 250.0,
        existing_store_lat: float = 40.7549,
        existing_store_lon: float = -73.9840,
        existing_store_radius: float = 250.0,
    ) -> Dict[str, Any]:
        """
        Simulates cannibalization impact of a candidate retail location relative
        to an existing store (e.g. Store A Herald Square).
        """
        distance_meters = cls.calculate_haversine_distance(
            lat1=candidate_lat,
            lon1=candidate_lon,
            lat2=existing_store_lat,
            lon2=existing_store_lon,
        )

        cand_poly = cls.create_circular_catchment_polygon(
            candidate_lat, candidate_lon, candidate_radius
        )
        exist_poly = cls.create_circular_catchment_polygon(
            existing_store_lat, existing_store_lon, existing_store_radius
        )

        is_overlapping, overlap_area, inter_geom = cls.compute_polygon_intersection(
            cand_poly, exist_poly, ref_lat=candidate_lat
        )

        # Baseline baseline calculations
        baseline_existing_visitors = 14250
        baseline_footfall_density = 48.0  # visitors per 100m^2

        if is_overlapping:
            # Overlap ratio between catchments
            total_cand_area = math.pi * (candidate_radius ** 2)
            overlap_pct = min(1.0, overlap_area / total_cand_area) if total_cand_area > 0 else 0.0

            projected_shared_devices = int(baseline_existing_visitors * overlap_pct * 0.72)
            projected_cannibalization_rate = round(
                (projected_shared_devices / baseline_existing_visitors) * 100.0, 2
            )
        else:
            projected_shared_devices = 0
            projected_cannibalization_rate = 0.0

        projected_total_footfall = int(11000 * (candidate_radius / 250.0))
        projected_unique_devices = int(projected_total_footfall * 0.82)
        projected_net_new_devices = max(0, projected_unique_devices - projected_shared_devices)

        # Risk Classification
        if projected_cannibalization_rate >= 30.0:
            risk = "HIGH_CANNIBALIZATION_RISK"
        elif projected_cannibalization_rate >= 15.0:
            risk = "MODERATE_CANNIBALIZATION_RISK"
        else:
            risk = "LOW_CANNIBALIZATION_EXPANSION"

        return {
            "distance_to_existing_meters": distance_meters,
            "is_overlapping": is_overlapping,
            "overlap_area_sq_meters": overlap_area,
            "projected_total_footfall": projected_total_footfall,
            "projected_unique_devices": projected_unique_devices,
            "projected_shared_devices": projected_shared_devices,
            "projected_cannibalization_rate_pct": projected_cannibalization_rate,
            "projected_net_new_devices": projected_net_new_devices,
            "risk_classification": risk,
            "catchment_geometry": cand_poly,
            "intersection_geometry": inter_geom,
        }


spatial_service = SpatialService()
