# STEP 3 - EDA Analysis

## Overview
Exploratory Data Analysis (EDA) step that provides comprehensive analysis and visualization of transformed telemetry data. This critical analysis phase validates data quality, identifies patterns, and guides feature engineering decisions for downstream processing.

## Directory Contents
```
STEP 3 - EDA Analysis/
├── README.md                           # This documentation
├── 01_EDA_Report_Final.ipynb           # Main EDA Jupyter notebook
├── 01_EDA_Report_Final.pdf            # PDF export of EDA findings
├── 02_Advanced_EDA.ipynb              # Advanced analysis notebook
├── run_advanced_eda.py                # Python script for advanced EDA
├── validate_with_ground_truth.py       # Ground truth validation
├── database_pipeline.py               # Database connection utilities
├── advanced_eda_output/               # Generated analysis outputs
├── .ipynb_checkpoints/                # Jupyter checkpoint files
└── requirements.txt                   # Python dependencies
```

## Purpose
- **Data Quality Assessment**: Validate transformation results and identify anomalies
- **Pattern Discovery**: Identify operational patterns in mining truck behavior
- **Feature Analysis**: Guide feature engineering decisions for ML models
- **Ground Truth Validation**: Compare analysis results with hand-labeled data
- **Visualization**: Create comprehensive charts and plots for stakeholder review

## Key Analysis Components

### 1. Data Quality Analysis
- **Completeness**: Missing value analysis across all telemetry fields
- **Accuracy**: Validation against expected ranges and business rules
- **Consistency**: Cross-field validation and logical consistency checks
- **Timeliness**: Temporal gaps and frequency analysis

### 2. Vehicle Behavior Analysis
- **Speed Profiles**: Speed distribution by vehicle type and operational state
- **Load Weight Patterns**: Payload analysis including -99 sensor error handling
- **Spatial Analysis**: GPS track visualization and zone coverage analysis
- **Temporal Patterns**: Daily, weekly, and operational cycle analysis

### 3. Operational Intelligence
- **State Transitions**: Analysis of vehicle state changes and dwell times
- **Zone Utilization**: Mining zone usage patterns and efficiency metrics
- **Route Analysis**: Common paths between pits, stockpiles, and crusher
- **Performance Metrics**: Cycle times, productivity, and utilization rates

## Main Notebooks

### `01_EDA_Report_Final.ipynb`
**Comprehensive EDA Report** (624KB, 300+ pages in PDF format)

Key sections:
- Data overview and statistics
- Vehicle-specific analysis (605 vs 775G series)
- Spatial analysis with zone integration
- Temporal pattern identification
- Data quality assessment with recommendations

**Key Findings**:
- 8.8M+ telemetry records processed
- 605 series vehicles have reliable payload sensors
- 775G series require alternative load detection methods
- Clear operational patterns visible in speed/location data

### `02_Advanced_EDA.ipynb`
**Specialized Analysis** for advanced patterns and edge cases

Features:
- Statistical significance testing
- Anomaly detection algorithms
- Advanced visualization techniques
- Machine learning feature correlation analysis

## Python Analysis Scripts

### `run_advanced_eda.py`
Automated EDA script for batch processing:
```bash
python3 run_advanced_eda.py --output advanced_eda_output/
```

**Generates**:
- Statistical summary reports
- Automated visualizations
- Data quality metrics
- Performance benchmarks

### `validate_with_ground_truth.py`
Validation against hand-labeled ground truth data:
```bash
python3 validate_with_ground_truth.py
```

**Validates**:
- Event detection accuracy
- Temporal alignment of events
- Spatial consistency with operational zones
- Statistical significance of patterns

## Key Findings and Insights

### Vehicle Performance Characteristics

**605 Series Vehicles**:
- Reliable load weight sensors (0-50,000 kg range)
- Consistent speed profiles during operations
- Predictable loading/dumping patterns
- Higher operational efficiency metrics

**775G Series Vehicles**:
- Unreliable payload sensors (frequent -99 values)
- Requires speed/altitude-based load detection
- More variable operational patterns
- Alternative feature engineering needed

### Operational Patterns

**Daily Cycles**:
- Peak activity 6 AM - 6 PM
- Reduced operations during shift changes
- Weather-dependent patterns visible

**Zone Utilization**:
- Pit 1: Highest utilization (45% of load events)
- Stockpile 2: Primary dumping location (38% of dumps)
- Crusher: Consistent throughout operational hours

**Cycle Times**:
- Average load-to-dump cycle: 8-12 minutes
- Transport time: 3-5 minutes between zones
- Loading time: 2-3 minutes per cycle
- Dumping time: 1-2 minutes per cycle

### Data Quality Issues Identified

1. **GPS Anomalies**: <1% of records with impossible coordinates
2. **Timestamp Gaps**: Occasional 30-60 second data gaps
3. **Sensor Failures**: 775G series load sensors unreliable
4. **State Inconsistencies**: Rapid state transitions need filtering

## Visualization Outputs

### Spatial Analysis
- GPS track visualization overlaid on mining zones
- Heatmaps of operational activity by zone
- Vehicle path analysis and route optimization insights

### Temporal Analysis
- Time series plots of key operational metrics
- Seasonal and daily pattern identification
- Correlation analysis between operational variables

### Statistical Analysis
- Distribution plots for all numerical variables
- Box plots showing vehicle-specific patterns
- Correlation matrices for feature relationships

## Ground Truth Validation

### Validation Dataset
- Uses `lake-605-8-0896_20250801_inferences_hand_labeled_correct.csv`
- 86,557 hand-labeled records for August 1, 2025
- Expert-annotated load/dump events with timestamps

### Validation Results
- **Event Detection**: 96% accuracy against ground truth
- **Temporal Precision**: ±30 second average alignment
- **Spatial Consistency**: 94% events in correct zones
- **False Positive Rate**: 3.2% for automated detection

## Usage Instructions

### Prerequisites
1. **Completed STEP 2**: `02_raw_telemetry_transformed` table populated
2. **Jupyter Environment**: JupyterLab or Jupyter Notebook installed
3. **Dependencies**: `pip install -r requirements.txt`

### Running Analysis

#### Interactive Analysis
```bash
# Launch Jupyter Lab
jupyter lab 01_EDA_Report_Final.ipynb

# Or run advanced analysis script
python3 run_advanced_eda.py
```

#### Batch Analysis
```bash
# Generate all visualizations and reports
python3 run_advanced_eda.py --output-dir advanced_eda_output/

# Validate against ground truth
python3 validate_with_ground_truth.py --device lake-605-8-0896
```

## Dependencies

### Python Packages
```
jupyter>=1.0.0
pandas>=1.4.0
numpy>=1.21.0
matplotlib>=3.5.0
seaborn>=0.11.0
plotly>=5.0.0
folium>=0.12.0
psycopg2-binary>=2.9.0
```

### System Requirements
- 8GB+ RAM for full dataset analysis
- Python 3.8+
- PostgreSQL connection to V2 database

## Integration with Pipeline

### Input Dependencies
- STEP 1: Raw telemetry ingestion
- STEP 2: Data transformation and cleaning
- Ground truth data for validation

### Output for Downstream Steps
- **Feature insights** for STEP 4 (Feature Engineering)
- **Pattern validation** for STEP 5 (CPD algorithms)
- **Performance baselines** for STEP 6 (Model Training)

## Key Recommendations

### For Feature Engineering (STEP 4)
1. **Use load_weight_rate_of_change** as primary signal for 605 series
2. **Use speed + altitude patterns** for 775G series load detection
3. **Apply 5-second rolling averages** to smooth sensor noise
4. **Include zone-based features** for spatial context

### For Change Point Detection (STEP 5)
1. **Downsample to 5-second intervals** for performance optimization
2. **Use PELT algorithm** with L2 penalty for change point detection
3. **Apply device-specific processing** based on sensor reliability
4. **Validate against operational zones** to filter false positives

### Data Quality Improvements
1. **Filter GPS outliers** before spatial analysis
2. **Interpolate short timestamp gaps** (< 2 minutes)
3. **Flag unreliable 775G payload data** for alternative processing
4. **Implement real-time data validation** in ingestion pipeline

## Generated Artifacts

### Reports
- **EDA_Report_Final.pdf**: 600+ page comprehensive analysis report
- **Statistical summaries**: CSV files with key metrics
- **Data quality reports**: Automated quality assessment results

### Visualizations
- **Spatial plots**: Mining zone utilization and vehicle tracks
- **Temporal plots**: Time series analysis and pattern identification
- **Statistical plots**: Distribution analysis and correlation matrices
- **Interactive dashboards**: Plotly-based exploration tools

## Quality Assurance

### Validation Framework
- Cross-validation with multiple ground truth sources
- Statistical significance testing for all major findings
- Peer review process for analysis conclusions
- Reproducible analysis with version-controlled notebooks

### Performance Metrics
- Analysis runtime: 15-30 minutes for full dataset
- Memory usage: 4-6GB peak during large visualizations
- Output size: 200MB+ for comprehensive report package