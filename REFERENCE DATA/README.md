# REFERENCE DATA

## Overview
This directory contains reference and lookup data used throughout the V2 pipeline for operational intelligence and spatial analysis. These files provide essential context for processing telemetry data from autonomous mining trucks.

## Directory Contents
```
REFERENCE DATA/
├── README.md                              # This documentation
└── LBP Zone Definitions Polygons.txt     # Mining zone polygon definitions
```

## File Descriptions

### `LBP Zone Definitions Polygons.txt`
**Purpose**: Defines operational zones within Lake Bridgeport Quarry for spatial analysis and event classification.

**Format**: Text file containing polygon coordinates for each mining zone
**Size**: 1.1 KB
**Encoding**: UTF-8 with BOM

## Zone Definitions

### Operational Zones
The quarry is divided into distinct operational areas, each defined by GPS polygon coordinates:

#### **Crusher Zone**
- **Purpose**: Rock crushing and processing facility
- **Coordinates**: Polygon with 6 points
- **Usage**: Identifies dump events at the crusher location

#### **Stockpile Zones (1-3)**
- **Stockpile 1**: Primary stockpile area
- **Stockpile 2**: Secondary stockpile area  
- **Stockpile 3**: Tertiary stockpile area
- **Purpose**: Material storage and inventory management
- **Usage**: Classifies dump events by stockpile location

#### **Pit Zones (1-3)**  
- **Pit 1**: Primary excavation area
- **Pit 2**: Secondary excavation area
- **Pit 3**: Tertiary excavation area
- **Purpose**: Material extraction zones
- **Usage**: Identifies load events from specific pit locations

### Coordinate System
- **Format**: WGS84 decimal degrees (longitude, latitude)
- **Precision**: 7 decimal places (~1.1 meter accuracy)
- **Example**: `-97.8302154, 33.2580123`

## Data Format Structure

### Polygon Definition Format
```
Zone Name:longitude1, latitude1longitude2, latitude2...longitudeN, latitudeN
```

### Example Entry
```
Crusher:-97.8302154, 33.2580123-97.8301054, 33.2578261-97.8299310, 33.2579001...
```

## Usage in Pipeline

### Step 4: Feature Engineering
The zone definitions are used in `STEP 4 - Feature Engineering` for:

1. **Spatial Joins**: Determining which zone each telemetry point falls within
2. **Event Classification**: Categorizing load/dump events by operational area
3. **Zone Analysis**: Calculating time spent in each zone

### Database Integration
Zone polygons are loaded into the `00_lbp_zones` table with:
- `zone_id`: Unique identifier for each zone
- `zone_name`: Human-readable zone name
- `zone_polygon`: PostGIS geometry for spatial queries

### PostGIS Operations
```sql
-- Example spatial query using zone polygons
SELECT zone_name 
FROM 00_lbp_zones 
WHERE ST_Contains(zone_polygon, ST_Point(longitude, latitude));
```

## Processing Requirements

### Coordinate Parsing
The text format requires parsing to extract:
- Zone name (before the colon)
- Coordinate pairs (longitude, latitude)
- Polygon closure (first point = last point)

### Validation Checks
1. **Polygon Validity**: Ensure polygons are properly closed
2. **Coordinate Range**: Verify coordinates are within expected bounds
3. **Overlap Detection**: Check for overlapping zone boundaries

## Integration Points

### Feature Engineering (Step 4)
```python
# Zone assignment during feature engineering
zone_assignment = spatial_join_with_zones(telemetry_data, zone_polygons)
```

### Change Point Detection (Step 5)
Zone information enhances CPD analysis by:
- Filtering events to operational areas only
- Providing context for event classification
- Enabling zone-specific algorithm tuning

### Model Training (Step 6)
Zone features serve as:
- Input features for ML models
- Stratification criteria for train/test splits
- Context for model performance analysis

## Data Quality

### Accuracy
- Polygons manually surveyed and validated
- Coordinates verified against satellite imagery
- Regular updates to reflect operational changes

### Completeness
- All major operational zones defined
- Covers entire active mining area
- Includes buffer zones for edge cases

### Consistency
- Standardized coordinate precision
- Consistent naming convention
- No overlapping zones

## Maintenance

### Updates Required When:
- New mining zones are established
- Existing zones are modified or expanded
- Equipment operational patterns change
- Higher precision coordinates become available

### Validation Process:
1. Visual inspection against current site maps
2. Telemetry data overlay verification
3. Operational staff review and approval
4. Pipeline integration testing

## Related Files
- Database schema in `STEP 4 - Feature Engineering`
- Zone loading utilities in feature engineering scripts
- Spatial analysis functions in EDA notebooks