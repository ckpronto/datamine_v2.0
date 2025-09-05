# DATABASES

## Overview
This directory contains database-related files and configurations for the DataMine V2 pipeline system.

## Directory Structure
```
DATABASES/
├── README.md          # This documentation file
└── datamine_data/     # Database data directory (currently empty)
```

## Purpose
The DATABASES folder serves as the local storage location for database-related files and configurations used by the V2 pipeline. This includes:

- Database initialization files
- Configuration scripts  
- Data export/import utilities
- Database backup files (when created)

## Database Configuration
The V2 pipeline uses PostgreSQL with TimescaleDB and PostGIS extensions:

### V2 Development Database
- **Database**: `datamine_v2_db`
- **User**: `ahs_user`
- **Password**: `ahs_password`
- **Host**: `localhost` (via Docker)
- **Port**: `5432`

### Connection Setup
```bash
# Start the database container
docker-compose up -d

# Connect to V2 database
PGPASSWORD="ahs_password" psql -h localhost -p 5432 -U ahs_user -d datamine_v2_db
```

## Database Schema Overview
The V2 pipeline creates the following table progression:

1. **`01_raw_telemetry`** - Raw CSV telemetry ingestion (Step 1)
2. **`02_raw_telemetry_transformed`** - Transformed telemetry data (Step 2)
3. **`04_primary_feature_table`** - Engineered features (Step 4)
4. **`05_candidate_events`** - Change point detection results (Step 5)

## Data Dictionary

### Core Tables

#### `01_raw_telemetry`
Raw telemetry data from autonomous mining trucks (8.8M+ records).

| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Unique identifier for mining truck |
| timestamp | TIMESTAMP | UTC timestamp of telemetry reading |
| latitude | NUMERIC | GPS latitude coordinate |
| longitude | NUMERIC | GPS longitude coordinate |
| altitude | NUMERIC | Altitude in meters |
| speed | NUMERIC | Vehicle speed in m/s |
| load_weight | NUMERIC | Payload weight in kg |
| fuel_level | NUMERIC | Fuel level percentage |

#### `02_raw_telemetry_transformed`
Processed telemetry with calculated fields and data cleaning.

| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Device identifier |
| timestamp | TIMESTAMP | Timestamp |
| latitude | NUMERIC | Latitude |
| longitude | NUMERIC | Longitude |  
| altitude | NUMERIC | Altitude in meters |
| speed_kmh | NUMERIC | Speed converted to km/h |
| load_weight_tons | NUMERIC | Load weight converted to tons |
| has_reliable_payload | BOOLEAN | Flag for payload sensor reliability |

#### `04_primary_feature_table`
Engineered features for machine learning and analysis.

| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Device identifier |
| timestamp | TIMESTAMP | Timestamp |
| load_weight_rate_of_change | NUMERIC | Primary signal for CPD |
| speed_rolling_avg_5s | NUMERIC | 5-second rolling average speed |
| altitude_rate_of_change | NUMERIC | Altitude change rate |
| zone_id | INTEGER | Mining zone identifier |

#### `05_candidate_events`
Change point detection results identifying load/dump events.

| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Device identifier |
| timestamp_start | TIMESTAMP | Event start timestamp |
| raw_event_hash_id | VARCHAR | Unique event identifier |

## Usage Notes

- The `datamine_data/` subdirectory is currently empty but reserved for local database files
- All database operations should use the connection parameters specified above
- The V2 database is separate from the production `datamine_main_db`
- Database tables are created automatically by pipeline steps

## Dependencies

- PostgreSQL 14+
- TimescaleDB extension
- PostGIS extension for geospatial operations
- Docker for containerized database deployment

## Related Files

- Root level `docker-compose.yml` for database container setup
- Individual step scripts contain table creation SQL
- Connection configuration in step scripts uses standard DB_CONFIG format