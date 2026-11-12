# GeoPulse: Retail Mobility & Foot-Traffic Analytics

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Geospatial-29B5E8.svg)](https://www.snowflake.com/)
[![dbt](https://img.shields.io/badge/dbt-Core-FF694B.svg)](https://www.getdbt.com/)
[![Kepler.gl](https://img.shields.io/badge/Kepler.gl-Mobility_Vis-2C3E50.svg)](https://kepler.gl/)

**GeoPulse** is an enterprise geospatial data engineering framework designed to model, ingest, transform, and visualize hyper-local mobile foot-traffic mobility patterns for retail network planning. It evaluates store catchment dynamics, device dwell times, and foot-traffic cannibalization across competing and prospective store sites in dense urban centers.

---

## 🏬 Case Study: Midtown Manhattan Retail Footprint
The initial pipeline models three key retail sites in Midtown Manhattan:
- **`STORE_A_EXISTING`**: Lat `40.7549`, Lon `-73.9840` (Herald Square / 34th St Flagship)
- **`STORE_B_PROPOSED_5TH`**: Lat `40.7565`, Lon `-73.9815` (Fifth Avenue Luxury Corridor)
- **`STORE_C_ALT_WEST`**: Lat `40.7510`, Lon `-73.9930` (West Midtown / Penn Station Corridor)

### Key Problem Statement
Quantify foot-traffic cannibalization between the existing flagship (`STORE_A_EXISTING`) and the prospective store (`STORE_B_PROPOSED_5TH`), where 30% of commuter foot traffic overlaps during morning transit peaks (07:00–09:59).

---

## 📁 Repository Structure
```
geopulse-retail-mobility/
├── airflow/                    # Airflow orchestration DAGs and data quality sensors
│   └── .gitkeep
├── dbt/                        # dbt models, staging transformations, and marts
│   └── .gitkeep
├── docs/                       # Architecture diagrams, schemas, and analytics specifications
│   └── architecture.md
├── frontend/
│   └── kepler-app/             # Kepler.gl / Deck.gl web UI for spatiotemporal visualization
│       └── .gitkeep
├── pyspark/                    # Data synthesis scripts, Spark sessionization, geofence configs
│   ├── generate_gps_pings.py   # High-throughput GPS mobility generator
│   └── store_geofences.json    # Store coordinates, metadata & 250m catchment radius
├── snowflake/                  # DDL, stage definitions, and spatial SQL functions
│   └── .gitkeep
├── .gitignore                  # Git ignore rules for data, venvs, cache
└── README.md                   # Project overview and setup instructions
```

---

## 🚀 Quickstart: Generating Synthetic Mobility Data

### Prerequisites
- Python 3.10+
- Dependencies: `pandas`, `numpy`

### Installation & Execution
```bash
# 1. Install dependencies
pip install pandas numpy

# 2. Run data synthesis script
python pyspark/generate_gps_pings.py
```

### Outputs
- `pyspark/mock_gps_pings.csv`: 200,000 GPS ping records (5,000 devices × 40 pings) with columns `[DeviceID, Lat, Long, Timestamp]`. *(Ignored in git to prevent bloating)*
- `pyspark/store_geofences.json`: GeoJSON-compatible store reference definitions with 250m circular catchment radii.

---

## 📊 Mobility Data Schema
| Column | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `DeviceID` | String (UUIDv4) | Unique mobile device identifier | `550e8400-e29b-41d4-a716-446655440000` |
| `Lat` | Float64 | WGS84 Latitude coordinate | `40.755120` |
| `Long` | Float64 | WGS84 Longitude coordinate | `-73.983845` |
| `Timestamp`| String / Timestamp | Event timestamp (2026-11-01 06:00 to 22:00) | `2026-11-01 08:15:32` |

---

## 🗺️ Geofence Reference Specification
Each store is defined with a **250-meter target catchment radius** ($\approx 0.00225^\circ$ latitude) in `pyspark/store_geofences.json`:
- `STORE_A_EXISTING`: Anchor coordinates `[40.7549, -73.9840]`
- `STORE_B_PROPOSED_5TH`: Anchor coordinates `[40.7565, -73.9815]`
- `STORE_C_ALT_WEST`: Anchor coordinates `[40.7510, -73.9930]`
