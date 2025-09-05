# STEP 1 - Ingest Raw Telemetry

## Overview
High-performance ingestion of raw telemetry data from CSV files into PostgreSQL database using optimized COPY operations. This is the entry point of the V2 pipeline, processing 8.8M+ telemetry records from autonomous mining trucks.

## Directory Contents
```
STEP 1 - Ingest Raw Telemetry/
├── README.md                    # This documentation  
├── 01_ingest_raw_telemetry.py   # Main ingestion script
├── 00_database_setup.sql        # Zone definitions setup
└── requirements.txt             # Python dependencies
```

## Purpose
- Ingest raw telemetry CSV files into the `datamine_v2_db` database
- Create the `01_raw_telemetry` table with optimized schema
- Set up zone definitions for spatial analysis in later steps
- Provide high-performance bulk loading using PostgreSQL COPY operations

## Input Data
Processes raw telemetry CSV files with the following columns:
- `timestamp` - UTC timestamp with microseconds
- `device_id` - Mining truck identifier (e.g., `lake-605-8-0896`)
- `state` - Operational state (e.g., `leadQueuedDumpToLoad`)
- `software_state` - Software system status
- `system_engaged` - Boolean autonomous system status
- `current_speed` - Vehicle speed in m/s
- `current_position` - JSON GPS coordinates `{lat, lon, alt}`
- `load_weight` - Payload weight in kg
- `prndl` - Transmission position
- `parking_brake_applied` - Boolean brake status
- `extras` - JSON diagnostic information

## Output Tables

### `01_raw_telemetry`
Primary table containing all ingested telemetry data:

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PRIMARY KEY | Auto-incrementing record ID |
| timestamp | TIMESTAMP | UTC timestamp |
| device_id | VARCHAR(100) | Device identifier |
| state | VARCHAR(100) | Operational state |
| software_state | VARCHAR(50) | Software status |
| system_engaged | BOOLEAN | Autonomous system status |
| current_speed | NUMERIC | Speed in m/s |
| current_position | TEXT | Raw JSON position data |
| load_weight | INTEGER | Payload weight in kg |
| prndl | VARCHAR(20) | Transmission position |
| parking_brake_applied | BOOLEAN | Brake status |
| extras | TEXT | Raw JSON diagnostic data |

### `00_lbp_zones` (Created by setup script)
Zone definitions for spatial analysis:

| Column | Type | Description |
|--------|------|-------------|
| zone_id | SERIAL PRIMARY KEY | Zone identifier |
| zone_name | VARCHAR(255) | Human-readable zone name |
| zone_polygon | GEOGRAPHY(POLYGON) | PostGIS polygon geometry |

## Usage Instructions

### Prerequisites
1. **Database Setup**:
   ```bash
   # Start TimescaleDB container
   docker-compose up -d
   
   # Verify connection
   PGPASSWORD="ahs_password" psql -h localhost -p 5432 -U ahs_user -d datamine_v2_db
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Ingestion

#### Basic Usage
```bash
# Default ingestion (uses hardcoded CSV path)
python3 01_ingest_raw_telemetry.py

# With custom CSV file
python3 01_ingest_raw_telemetry.py /path/to/your/data.csv
```

#### Advanced Options
```bash
# Recreate table if it exists
python3 01_ingest_raw_telemetry.py --recreate-table

# Custom file with table recreation
python3 01_ingest_raw_telemetry.py /path/to/data.csv --recreate-table
```

#### Expected Default File Location
The script defaults to: `../RAW TELEMETRY DATA/lake_snapshots_2025-07-30_2025-08-12.csv`

### Database Setup (One-time)
```bash
# Run zone setup SQL (optional - run once)
PGPASSWORD="ahs_password" psql -h localhost -p 5432 -U ahs_user -d datamine_v2_db -f 00_database_setup.sql
```

## Performance Features

### High-Performance Ingestion
- **PostgreSQL COPY**: 10-50x faster than INSERT operations
- **Batch Processing**: Processes entire CSV file in single operation
- **Deferred Indexing**: Creates indexes AFTER data ingestion
- **Type Conversion**: Automatic PostgreSQL data type handling

### Optimization Strategies
1. **Minimal Processing**: Raw JSON stored as TEXT (parsed in later steps)
2. **Stream Processing**: Direct file-to-database streaming
3. **Error Tolerance**: Continues ingestion despite individual record errors
4. **Progress Reporting**: Real-time ingestion status

### Performance Metrics
- **Typical Speed**: 50,000+ records/second
- **8.8M Records**: ~3-5 minutes ingestion time
- **Memory Usage**: <1GB (streaming approach)

## Database Configuration

### Connection Settings
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',  # V2 database
    'user': 'ahs_user',
    'password': 'ahs_password'
}
```

### Required Extensions
- **PostGIS**: For spatial operations on zone polygons
- **TimescaleDB**: For time-series optimization (optional)

## Error Handling

### Common Issues
1. **File Not Found**: Verify CSV file path
2. **Permission Denied**: Check file permissions and database access
3. **Memory Errors**: Use streaming approach (built-in)
4. **Invalid Data**: Script continues with warnings

### Troubleshooting
```bash
# Check table exists
\d 01_raw_telemetry

# Verify record count
SELECT COUNT(*) FROM 01_raw_telemetry;

# Check recent records
SELECT * FROM 01_raw_telemetry ORDER BY timestamp DESC LIMIT 10;
```

## Validation

### Data Integrity Checks
1. **Record Count**: Verify CSV lines match database records
2. **Timestamp Range**: Check min/max timestamps
3. **Device Coverage**: Ensure all devices represented
4. **Data Types**: Validate automatic type conversions

### Sample Validation Queries
```sql
-- Record count verification
SELECT COUNT(*) FROM 01_raw_telemetry;

-- Device distribution
SELECT device_id, COUNT(*) FROM 01_raw_telemetry GROUP BY device_id;

-- Timestamp range
SELECT MIN(timestamp), MAX(timestamp) FROM 01_raw_telemetry;

-- Load weight distribution (identify invalid -99 values)
SELECT load_weight, COUNT(*) FROM 01_raw_telemetry GROUP BY load_weight ORDER BY load_weight;
```

## Next Steps

After successful ingestion, proceed to:
1. **STEP 2** - Transform Raw Telemetry (data cleaning and enrichment)
2. **STEP 3** - EDA Analysis (exploratory data analysis)
3. **STEP 4** - Feature Engineering (derive analytical features)

## Dependencies

### Python Packages
- `psycopg2-binary==2.9.7` - PostgreSQL adapter
- `pandas==2.1.0` - Data manipulation
- `numpy==1.24.3` - Numerical operations  
- `python-dateutil==2.8.2` - Date parsing utilities

### System Requirements
- Python 3.8+
- PostgreSQL 14+ with PostGIS extension
- 8GB+ RAM for large datasets
- 10GB+ disk space for database storage

## Data Lineage

- **Input**: Raw CSV files from autonomous mining truck telemetry systems
- **Processing**: High-performance PostgreSQL COPY operations
- **Output**: Structured database table ready for transformation
- **Quality**: Preserves all original data with minimal processing