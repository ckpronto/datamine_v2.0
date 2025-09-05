# RAW TELEMETRY DATA

## Overview
This directory contains raw telemetry data files from autonomous mining trucks at Lake Bridgeport Quarry. These files serve as the primary input source for the V2 pipeline processing system.

## Directory Contents
```
RAW TELEMETRY DATA/
├── README.md                                                    # This documentation
├── lake_snapshots_2025-07-30_2025-08-12.csv                   # Main telemetry dataset (8.8M+ records)
└── lake-605-8-0896_20250801_inferences_hand_labeled_correct.csv # Hand-labeled ground truth data
```

## File Descriptions

### `lake_snapshots_2025-07-30_2025-08-12.csv`
- **Size**: 4.7 GB
- **Records**: 8,831,278 telemetry readings
- **Period**: July 30, 2025 - August 12, 2025 (14 days)
- **Purpose**: Primary dataset for V2 pipeline processing

### `lake-605-8-0896_20250801_inferences_hand_labeled_correct.csv`  
- **Size**: 24.4 MB
- **Records**: 86,557 labeled telemetry readings
- **Purpose**: Ground truth validation dataset for model training and testing
- **Device**: Specific to vehicle `lake-605-8-0896`
- **Date**: August 1, 2025

## Data Format

### Telemetry Schema
The raw telemetry CSV files contain the following columns:

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `timestamp` | TIMESTAMP | UTC timestamp with microseconds | `2025-07-30 00:00:05.734539+00` |
| `device_id` | STRING | Unique mining truck identifier | `lake-605-8-0896`, `lake-775g-2-2266` |
| `state` | STRING | Operational state of vehicle | `leadQueuedDumpToLoad`, `loading` |
| `software_state` | STRING | Software system status | `fault`, `normal` |
| `system_engaged` | BOOLEAN | Whether autonomous system is active | `true`, `false` |
| `current_speed` | FLOAT | Vehicle speed in m/s | `0.004872177981488213` |
| `current_position` | JSON | GPS coordinates `{lat, lon, alt}` | `{33.268851,-97.835149,250.679}` |
| `load_weight` | INTEGER | Payload weight in kg | `0`, `45000`, `-99` (invalid) |
| `prndl` | STRING | Transmission gear position | `neutral`, `drive`, `reverse` |
| `parking_brake_applied` | BOOLEAN | Parking brake status | `t`, `f` |
| `extras` | JSON | Additional diagnostic information | Complex JSON array |

### Data Quality Notes

#### Vehicle Types
- **605 Series**: Reliable payload sensors (normal load_weight values)
- **775G Series**: Unreliable payload sensors (often `-99` values)

#### Known Issues
- **Invalid Load Weights**: `-99` indicates sensor malfunction
- **High Frequency**: ~2Hz sampling rate creates large datasets
- **Complex JSON**: `current_position` and `extras` require parsing
- **Mixed States**: Multiple operational states per vehicle per day

## Usage in Pipeline

### Step 1: Raw Ingestion
```bash
python3 "STEP 1 - Ingest Raw Telemetry/01_ingest_raw_telemetry.py"
```
- Ingests `lake_snapshots_2025-07-30_2025-08-12.csv`
- Creates `01_raw_telemetry` table in `datamine_v2_db`
- Parses JSON fields and validates data types

### Ground Truth Integration
```bash
# Ground truth data used for validation in various steps
# Specifically targets device lake-605-8-0896 for August 1, 2025
```

## Data Processing Requirements

### System Requirements
- **Memory**: 16GB+ RAM recommended for processing 8.8M records
- **Storage**: 10GB+ free space for intermediate files
- **Database**: PostgreSQL with TimescaleDB extension

### Performance Considerations
- **File Size**: 4.7GB main dataset requires streaming processing
- **Record Count**: 8.8M records need batch/chunk processing
- **JSON Parsing**: Position and extras fields are computationally expensive
- **Memory Management**: Use file-based aggregation to prevent crashes

### Data Validation
The pipeline includes validation against:
- Hand-labeled ground truth data
- Expected value ranges for each sensor
- Consistency checks across related fields
- Temporal continuity validation

## Related Pipeline Steps

1. **STEP 1** - Ingests this raw data into PostgreSQL
2. **STEP 2** - Transforms and cleans the ingested data
3. **STEP 3** - Performs EDA analysis on the processed data
4. **Ground Truth** - Uses labeled data for model validation

## File Access Patterns

### Read-Only Access
These files should be treated as **read-only** source data:
- Never modify original CSV files
- Use copy operations for any transformations
- Maintain backup copies of critical datasets

### Processing Strategy
1. Stream large files using pandas `chunksize` parameter
2. Use Polars for high-performance processing when possible
3. Implement checkpointing for long-running operations
4. Monitor memory usage during processing

## Data Lineage

- **Source**: Autonomous mining truck telemetry systems
- **Collection Period**: July 30 - August 12, 2025
- **Devices**: Multiple mining trucks (605 and 775G series)
- **Frequency**: ~2Hz (every 500ms)
- **Quality**: Production-grade operational data with known sensor limitations