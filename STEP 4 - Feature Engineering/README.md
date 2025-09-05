# STEP 4 - Feature Engineering

## Overview
High-performance feature engineering pipeline that transforms raw telemetry data into analytical features optimized for machine learning and change point detection. This step creates the `04_primary_feature_table` with engineered features including rolling averages, rate calculations, and spatial analysis.

## Directory Contents
```
STEP 4 - Feature Engineering/
├── README.md                    # This documentation  
├── 04_feature_engineering.py    # Main orchestration script
├── 04_worker_script.sql         # Parallel SQL worker logic
├── 04_final_assembly.sql        # Final table assembly and indexing
├── _archive/                    # Deprecated implementations
└── __pycache__/                 # Python cache files
```

## Purpose
- **Feature Creation**: Derive analytical features from raw telemetry data
- **Spatial Analysis**: Integrate GPS coordinates with operational zone definitions
- **Temporal Features**: Calculate rolling averages and rate-of-change metrics
- **Performance Optimization**: Use parallel SQL workers for scalable processing
- **Data Quality**: Ensure feature consistency and handle edge cases

## Architecture

### Parallel Processing Pipeline
```
02_raw_telemetry_transformed (Input)
         ↓
04_feature_engineering.py (Orchestrator)
         ↓
04_worker_script.sql (Parallel Workers) → Staging Tables
         ↓
04_final_assembly.sql → 04_primary_feature_table (Output)
```

### Three-Stage Processing
1. **Orchestration**: Python script manages parallel execution
2. **Parallel Workers**: SQL scripts process data in chunks
3. **Final Assembly**: Combine worker outputs with optimized indexing

## Core Scripts

### 1. `04_feature_engineering.py`
**Purpose**: Main orchestration script that manages parallel feature engineering

**Key Features**:
- **Parallel Execution**: Spawns multiple SQL workers for concurrent processing
- **Progress Monitoring**: Real-time status reporting and error handling
- **Memory Management**: Chunked processing prevents memory overflow
- **Database Connection Pool**: Optimized connection handling for workers

**Usage**:
```bash
python3 04_feature_engineering.py
```

**Configuration Options**:
- Worker count: Auto-detects CPU cores (default: 8 workers)
- Batch size: Configurable chunk size for processing (default: 100K records)
- Memory limit: Automatic memory management with batch processing

### 2. `04_worker_script.sql`
**Purpose**: Parallel SQL worker that processes telemetry data chunks

**Feature Engineering Logic**:
```sql
-- Rolling averages for noise reduction
speed_rolling_avg_5s = AVG(speed_kmh) OVER (
    PARTITION BY device_id 
    ORDER BY timestamp 
    RANGE BETWEEN INTERVAL '5 seconds' PRECEDING AND CURRENT ROW
)

-- Rate of change calculations for change point detection
load_weight_rate_of_change = (
    load_weight_tons - LAG(load_weight_tons) OVER (
        PARTITION BY device_id ORDER BY timestamp
    )
) / EXTRACT(EPOCH FROM (
    timestamp - LAG(timestamp) OVER (
        PARTITION BY device_id ORDER BY timestamp
    )
))

-- Spatial zone assignment using PostGIS
zone_id = (
    SELECT z.zone_id 
    FROM 00_lbp_zones z 
    WHERE ST_Contains(z.zone_polygon, ST_Point(longitude, latitude))
)
```

### 3. `04_final_assembly.sql`
**Purpose**: Final assembly, indexing, and optimization of feature table

**Key Operations**:
- **Table Creation**: Optimized schema for downstream processing
- **Index Creation**: B-tree and GIST indexes for performance
- **Data Validation**: Quality checks and constraint enforcement
- **Statistics Update**: PostgreSQL query planner optimization

## Output Table: `04_primary_feature_table`

### Schema Definition
| Column | Type | Description | Used By |
|--------|------|-------------|---------|
| id | SERIAL PRIMARY KEY | Auto-incrementing record ID | All steps |
| timestamp | TIMESTAMP | UTC timestamp | All steps |
| device_id | VARCHAR(100) | Device identifier | All steps |
| latitude | NUMERIC | GPS latitude | Spatial analysis |
| longitude | NUMERIC | GPS longitude | Spatial analysis |
| altitude | NUMERIC | GPS altitude | CPD (775G series) |
| speed_kmh | NUMERIC | Original speed in km/h | Analysis |
| speed_rolling_avg_5s | NUMERIC | 5-second rolling average speed | **CPD Primary Signal (775G)** |
| speed_rolling_avg_15s | NUMERIC | 15-second rolling average speed | Analysis |
| speed_rolling_avg_30s | NUMERIC | 30-second rolling average speed | Analysis |
| load_weight_tons | NUMERIC | Original load weight in tons | Analysis |
| load_weight_rate_of_change | NUMERIC | **PRIMARY CPD SIGNAL (605 series)** | **CPD Core Algorithm** |
| altitude_rate_of_change | NUMERIC | Altitude change rate | **CPD Secondary Signal (775G)** |
| has_reliable_payload | BOOLEAN | Payload sensor reliability flag | **CPD Signal Selection** |
| zone_id | INTEGER | Operational zone identifier | Spatial filtering |
| state | VARCHAR(100) | Operational state | Context analysis |
| system_engaged | BOOLEAN | Autonomous system status | Filtering |

### Critical Features for CPD (STEP 5)

#### Primary Signals
- **`load_weight_rate_of_change`**: Core signal for 605 series vehicles with reliable payload sensors
- **`speed_rolling_avg_5s` + `altitude_rate_of_change`**: Combined signal for 775G series with unreliable sensors

#### Device Selection Logic
```sql
-- CPD algorithm selects signal based on this flag
CASE 
    WHEN device_id LIKE 'lake-605%' THEN has_reliable_payload = true
    WHEN device_id LIKE 'lake-775g%' THEN has_reliable_payload = false
    ELSE has_reliable_payload = (load_weight != -99)
END
```

## Feature Engineering Algorithms

### Rolling Averages
**Purpose**: Smooth sensor noise and identify trends
```sql
-- Multiple time windows for different analysis needs
speed_rolling_avg_5s   -- CPD processing (primary)
speed_rolling_avg_15s  -- Trend analysis  
speed_rolling_avg_30s  -- Long-term patterns
```

### Rate of Change Calculations
**Purpose**: Detect sudden changes indicating load/dump events
```sql
-- Primary CPD signal calculation
load_weight_rate_of_change = (current_weight - previous_weight) / time_delta

-- Secondary signals for unreliable payload vehicles
altitude_rate_of_change = (current_altitude - previous_altitude) / time_delta
```

### Spatial Feature Engineering
**Purpose**: Integrate GPS coordinates with operational zones
```sql
-- Zone assignment using PostGIS spatial joins
SELECT z.zone_id, z.zone_name
FROM 00_lbp_zones z
WHERE ST_Contains(z.zone_polygon, ST_Point(longitude, latitude))
```

## Performance Features

### Parallel SQL Processing
- **Multi-Worker**: Concurrent SQL execution across CPU cores
- **Chunked Processing**: Memory-efficient batch processing  
- **Connection Pooling**: Optimized database connection management
- **Progress Monitoring**: Real-time processing status and ETA

### Database Optimizations
- **Staged Processing**: Temporary staging tables for intermediate results
- **Deferred Indexing**: Indexes created after data processing
- **Query Optimization**: Window functions optimized for time-series data
- **Statistics Updates**: Automatic table statistics for query planner

### Performance Metrics
- **Processing Speed**: 15,000+ records/second per worker
- **8.8M Records**: 10-15 minutes with 8 parallel workers
- **Memory Usage**: <4GB total across all workers
- **CPU Utilization**: Scales linearly with available cores

## Spatial Analysis Integration

### Zone Definitions
Uses `00_lbp_zones` table created in STEP 1:
- **Crusher**: Material processing zone
- **Stockpiles 1-3**: Material storage areas
- **Pits 1-3**: Excavation zones

### PostGIS Operations
```sql
-- High-performance spatial joins with GIST indexing
CREATE INDEX idx_zones_geometry ON 00_lbp_zones USING GIST (zone_polygon);
CREATE INDEX idx_telemetry_point ON telemetry USING GIST (ST_Point(longitude, latitude));

-- Zone assignment query optimized for millions of records
SELECT t.*, z.zone_id
FROM telemetry t
LEFT JOIN 00_lbp_zones z ON ST_Contains(z.zone_polygon, ST_Point(t.longitude, t.latitude));
```

## Data Quality and Validation

### Input Validation
- **GPS Bounds**: Coordinates within operational area
- **Speed Limits**: Reasonable speed values (0-80 km/h)
- **Load Weight**: Handle -99 values for broken sensors
- **Timestamp**: Monotonic increasing per device

### Quality Assurance Checks
```sql
-- Validate feature engineering results
SELECT 
    device_id,
    COUNT(*) as total_records,
    COUNT(zone_id) as records_with_zones,
    AVG(speed_rolling_avg_5s) as avg_speed_5s,
    COUNT(CASE WHEN load_weight_rate_of_change IS NOT NULL THEN 1 END) as valid_load_changes
FROM 04_primary_feature_table
GROUP BY device_id;
```

### Edge Case Handling
- **Zone Boundaries**: GPS points outside operational zones (null zone_id)
- **Sensor Failures**: -99 load weights handled gracefully
- **Timestamp Gaps**: Rate calculations handle missing data points
- **State Transitions**: Rapid state changes filtered appropriately

## Usage Instructions

### Prerequisites
1. **Completed STEP 2**: `02_raw_telemetry_transformed` table populated
2. **Zone Setup**: `00_lbp_zones` table created (from STEP 1)
3. **Database Extensions**: PostGIS enabled for spatial operations
4. **System Resources**: 16GB+ RAM recommended for full dataset

### Execution
```bash
# Basic execution with default settings
python3 04_feature_engineering.py

# Monitor progress in real-time
python3 04_feature_engineering.py --verbose

# Custom worker configuration
python3 04_feature_engineering.py --workers 12 --batch-size 50000
```

### Validation
```sql
-- Check output table completeness
SELECT COUNT(*) FROM 02_raw_telemetry_transformed;
SELECT COUNT(*) FROM 04_primary_feature_table;

-- Verify feature quality
SELECT 
    device_id,
    MIN(timestamp) as start_time,
    MAX(timestamp) as end_time,
    COUNT(*) as record_count,
    AVG(speed_rolling_avg_5s) as avg_speed,
    COUNT(zone_id) * 100.0 / COUNT(*) as zone_coverage_pct
FROM 04_primary_feature_table
GROUP BY device_id;
```

## Integration with Pipeline

### Input Dependencies
- **STEP 1**: Zone definitions (`00_lbp_zones` table)
- **STEP 2**: Transformed telemetry (`02_raw_telemetry_transformed` table)
- **PostGIS**: Spatial extension for zone analysis

### Critical Output for CPD (STEP 5)
The `04_primary_feature_table` is the **direct input** to STEP 5 CPD processing:
- **Primary Signal**: `load_weight_rate_of_change` for change point detection
- **Device Flags**: `has_reliable_payload` for algorithm selection
- **Secondary Signals**: `speed_rolling_avg_5s`, `altitude_rate_of_change` for 775G vehicles

## Error Handling

### Common Issues
1. **PostGIS Missing**: Ensure PostGIS extension installed and enabled
2. **Memory Overflow**: Reduce batch size or worker count for large datasets
3. **Spatial Join Performance**: Verify GIST indexes on zone polygons
4. **Missing Zones**: Some GPS coordinates outside operational areas (expected)

### Recovery Procedures
```bash
# Check for partial processing
\dt "*staging*"  # List staging tables

# Resume from checkpoint
python3 04_feature_engineering.py --resume

# Force recreation if needed
python3 04_feature_engineering.py --drop-staging --recreate
```

## Dependencies

### Python Packages
```
psycopg2-binary>=2.9.7  # PostgreSQL adapter
pandas>=1.4.0           # Data manipulation (optional)
numpy>=1.21.0           # Numerical operations
```

### Database Requirements
- PostgreSQL 14+ with PostGIS extension
- TimescaleDB (optional, for time-series optimization)
- 32GB+ RAM for full dataset with 8+ workers
- SSD storage recommended for performance

### System Requirements
- Python 3.8+
- 8+ CPU cores (scales to available cores)
- 16GB+ RAM for concurrent processing
- 10GB+ disk space for staging tables

## Next Steps

After successful feature engineering:
1. **STEP 5**: Change Point Detection using engineered features
2. **STEP 6**: Model training with feature-rich dataset
3. **Validation**: Compare features against ground truth data

## Data Lineage

- **Input**: `02_raw_telemetry_transformed` (cleaned telemetry data)
- **Processing**: Parallel feature engineering with spatial integration
- **Output**: `04_primary_feature_table` (ML-ready analytical features)
- **Quality**: Optimized for change point detection and machine learning