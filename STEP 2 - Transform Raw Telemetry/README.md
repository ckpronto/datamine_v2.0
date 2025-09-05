# STEP 2 - Transform Raw Telemetry

## Overview
Data transformation and cleaning pipeline that processes raw telemetry from `01_raw_telemetry` table into the cleaned `02_raw_telemetry_transformed` table. This step handles JSON parsing, unit conversions, data validation, and handles sensor reliability issues common in mining equipment.

## Directory Contents
```
STEP 2 - Transform Raw Telemetry/
├── README.md                                    # This documentation
├── 02_raw_telemetry_transform.py               # Main transformation script
├── 02_raw_telemetry_transform.sql              # SQL transformation logic
├── 02_raw_telemetry_transform_100CPU.conf      # Parallel processing configuration
├── requirements.txt                             # Python dependencies
└── _archive/                                    # Deprecated scripts
```

## Purpose
- Parse JSON fields (current_position, extras) from raw telemetry
- Convert units (m/s to km/h, kg to tons)
- Handle invalid sensor readings (-99 for broken load sensors)
- Calculate derived fields and data quality flags
- Create optimized indexes for downstream processing
- Enable high-performance parallel processing

## Input Table: `01_raw_telemetry`
Raw telemetry data as ingested from CSV files with minimal processing.

## Output Table: `02_raw_telemetry_transformed`

### Schema
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PRIMARY KEY | Auto-incrementing record ID |
| timestamp | TIMESTAMP | UTC timestamp |
| device_id | VARCHAR(100) | Device identifier |
| state | VARCHAR(100) | Operational state |
| software_state | VARCHAR(50) | Software system status |
| system_engaged | BOOLEAN | Autonomous system active |
| current_speed | NUMERIC | Original speed in m/s |
| speed_kmh | NUMERIC | Speed converted to km/h |
| latitude | NUMERIC | Parsed GPS latitude |
| longitude | NUMERIC | Parsed GPS longitude |
| altitude | NUMERIC | Parsed GPS altitude |
| load_weight | INTEGER | Original load weight in kg |
| load_weight_tons | NUMERIC | Load weight converted to tons |
| has_reliable_payload | BOOLEAN | Payload sensor reliability flag |
| prndl | VARCHAR(20) | Transmission position |
| parking_brake_applied | BOOLEAN | Brake status |
| extras | TEXT | Preserved raw JSON diagnostic data |

## Key Transformations

### JSON Parsing
- **current_position**: `{lat, lon, alt}` → separate lat/lng/alt columns
- **extras**: Preserved as TEXT for diagnostic purposes

### Unit Conversions
- **Speed**: m/s → km/h (multiply by 3.6)
- **Load Weight**: kg → tons (divide by 1000)

### Data Quality Flags
- **has_reliable_payload**: `true` for 605 series trucks, `false` for 775G series
- Handles `-99` values from broken load sensors
- Identifies vehicles with unreliable payload data

### Validation Rules
- GPS coordinates within reasonable bounds
- Speed values within operational limits
- Timestamp continuity checks
- Device ID consistency validation

## Usage Instructions

### Prerequisites
1. **Completed STEP 1**: `01_raw_telemetry` table populated
2. **Database connection**: V2 database accessible
3. **Dependencies installed**: `pip install -r requirements.txt`

### Basic Execution
```bash
# Standard transformation
python3 02_raw_telemetry_transform.py

# With custom parameters
python3 02_raw_telemetry_transform.py --batch-size 50000
```

### High-Performance Mode
```bash
# Use parallel processing configuration
python3 02_raw_telemetry_transform.py --config 02_raw_telemetry_transform_100CPU.conf

# For large datasets (8M+ records)
python3 02_raw_telemetry_transform.py --parallel --workers 8
```

## Performance Features

### Optimized Processing
- **Batch Processing**: Configurable batch sizes for memory management
- **Parallel Workers**: Multi-process execution for CPU-intensive operations
- **Indexed Staging**: Creates optimized indexes after transformation
- **Memory Management**: Streaming processing prevents OOM crashes

### Performance Metrics
- **Processing Speed**: 25,000+ records/second
- **8.8M Records**: ~6-10 minutes processing time
- **Memory Usage**: <2GB with batch processing
- **CPU Utilization**: Scales to available cores

## Configuration Files

### `02_raw_telemetry_transform_100CPU.conf`
High-performance configuration for systems with 100+ CPU cores:
```ini
[processing]
batch_size = 100000
parallel_workers = 90
memory_limit = 8GB
enable_indexing = true
```

### `02_raw_telemetry_transform.sql`
Core SQL transformation logic executed by Python orchestrator:
- JSON parsing functions
- Unit conversion calculations
- Data quality flag generation
- Index creation statements

## Data Quality Handling

### Invalid Sensor Readings
- **Load Weight -99**: Marks `has_reliable_payload = false`
- **GPS Anomalies**: Filters impossible coordinates
- **Speed Outliers**: Caps speeds at reasonable maximums
- **Timestamp Issues**: Handles timezone and format inconsistencies

### Vehicle-Specific Logic
```python
# 605 series: Reliable payload sensors
if device_id.startswith('lake-605'):
    has_reliable_payload = True
    
# 775G series: Unreliable payload sensors
elif '775g' in device_id.lower():
    has_reliable_payload = False
```

## Validation and Testing

### Data Integrity Checks
```sql
-- Verify transformation completeness
SELECT COUNT(*) FROM 01_raw_telemetry;
SELECT COUNT(*) FROM 02_raw_telemetry_transformed;

-- Check unit conversions
SELECT AVG(current_speed * 3.6), AVG(speed_kmh) 
FROM 02_raw_telemetry_transformed;

-- Validate GPS parsing
SELECT COUNT(*) FROM 02_raw_telemetry_transformed 
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Check payload reliability flags
SELECT device_id, has_reliable_payload, COUNT(*)
FROM 02_raw_telemetry_transformed 
GROUP BY device_id, has_reliable_payload;
```

### Quality Assurance Queries
```sql
-- Identify data quality issues
SELECT 
    device_id,
    COUNT(*) as total_records,
    COUNT(CASE WHEN load_weight = -99 THEN 1 END) as invalid_load_readings,
    COUNT(CASE WHEN latitude IS NULL THEN 1 END) as missing_gps,
    AVG(speed_kmh) as avg_speed_kmh
FROM 02_raw_telemetry_transformed
GROUP BY device_id;
```

## Error Handling

### Common Issues
1. **JSON Parsing Errors**: Malformed position data handled gracefully
2. **Division by Zero**: Safe unit conversions with null checks
3. **Memory Overflow**: Batch processing prevents crashes
4. **Index Creation**: Deferred until after data transformation

### Recovery Procedures
```bash
# Restart from checkpoint
python3 02_raw_telemetry_transform.py --resume --from-checkpoint

# Skip problematic batches
python3 02_raw_telemetry_transform.py --skip-errors --log-level DEBUG

# Force recreation of target table
python3 02_raw_telemetry_transform.py --drop-table --recreate
```

## Dependencies

### Python Packages
- `psycopg2-binary==2.9.7` - PostgreSQL adapter
- `pandas==2.1.0` - Data manipulation
- `numpy==1.24.3` - Numerical operations

### Database Requirements
- PostgreSQL 14+ with JSON functions
- PostGIS extension for spatial operations
- 16GB+ RAM for full dataset processing
- SSD storage recommended for index creation

## Next Steps

After successful transformation:
1. **STEP 2.1** - Load/Dump Event Labeling (ML-based event detection)
2. **STEP 3** - EDA Analysis (exploratory data analysis)
3. **STEP 4** - Feature Engineering (derive analytical features)

## Data Lineage

- **Input**: `01_raw_telemetry` (raw CSV ingestion)
- **Processing**: JSON parsing, unit conversion, quality flagging
- **Output**: `02_raw_telemetry_transformed` (cleaned analytical data)
- **Quality**: Handles sensor reliability and data validation issues

## Monitoring and Logging

### Log Files
- Transformation progress with timestamps
- Error handling and recovery actions
- Performance metrics and batch processing stats
- Data quality warnings and validation results

### Success Criteria
- All records from source table processed
- JSON fields successfully parsed (>95% success rate)
- Unit conversions accurate within tolerance
- Payload reliability flags correctly assigned
- Indexes created successfully for performance