# STEP 2.1 - LD Event Labeling

## Overview
Machine Learning-based Load/Dump event labeling system that uses neural networks and XGBoost models to automatically identify and classify loading and dumping events from telemetry data. This step bridges the gap between raw data transformation and advanced analytics by providing labeled training data for downstream models.

## Directory Contents
```
STEP 2.1 - LD Event Labeling/
├── README.md                                    # This documentation
├── 02.1.1_neural_net_training_ldevents_lsprep.py      # Neural network training
├── 02.1.2_neural_net_training_ldevents_label_recombine.py  # Label recombination
├── 02.1.3_train_xgboost_model.py               # XGBoost model training
├── model_comparison_validation.py              # Model performance comparison
├── load_dump_events_config.xml                 # Label Studio configuration
├── requirements.txt                             # Python dependencies
├── labelstudio_input/                           # Input data for labeling
├── labelstudio_output/                          # Labeled output data
├── *.log                                        # Training and execution logs
├── xgboost_enhanced_*.png                      # Model performance visualizations
├── xgboost_enhanced_results.json               # Model performance metrics
└── __pycache__/                                # Python cache files
```

## Purpose
- Train neural networks for initial load/dump event detection
- Use XGBoost for enhanced classification accuracy
- Generate labeled training data for downstream machine learning
- Provide model comparison and validation frameworks
- Integrate with Label Studio for manual annotation workflows

## Machine Learning Pipeline

### Stage 1: Neural Network Preparation (`02.1.1`)
**Purpose**: Prepare data and train initial neural network for event detection

**Key Features**:
- Time series windowing for temporal pattern recognition
- Feature engineering specific to mining operations
- LSTM/RNN architecture for sequence modeling
- Handles high-frequency telemetry data (1-2Hz)

**Input**: `02_raw_telemetry_transformed` table
**Output**: Neural network model + preliminary event predictions

### Stage 2: Label Recombination (`02.1.2`)
**Purpose**: Combine neural network outputs with domain knowledge

**Key Features**:
- Merges multiple model predictions
- Applies business rules and constraints
- Handles conflicting predictions between models
- Generates confidence scores for predictions

**Input**: Neural network predictions + business rules
**Output**: Reconciled event labels with confidence scores

### Stage 3: XGBoost Enhancement (`02.1.3`)
**Purpose**: Final classification using gradient boosting

**Key Features**:
- Ensemble learning for improved accuracy
- Feature importance analysis
- Handles imbalanced datasets (loading vs dumping events)
- Cross-validation for model robustness

**Input**: Reconciled labels + engineered features
**Output**: Final XGBoost model + performance metrics

## Model Performance

### XGBoost Enhanced Results
Based on `xgboost_enhanced_results.json`:

- **Overall Accuracy**: >95% on validation set
- **Precision/Recall**: Balanced across load/dump classes
- **Feature Importance**: Load weight, speed, and location-based features
- **Cross-Validation**: 5-fold CV with stable performance

### Performance Visualizations
- **Confusion Matrix**: `xgboost_enhanced_confusion_matrix.png`
- **Feature Importance**: `xgboost_enhanced_feature_importance.png`

## Usage Instructions

### Prerequisites
1. **Completed STEP 2**: `02_raw_telemetry_transformed` table populated
2. **Ground Truth Data**: Hand-labeled events for training
3. **Computing Resources**: GPU recommended for neural network training

### Sequential Execution
```bash
# Step 1: Neural network preparation and training
python3 02.1.1_neural_net_training_ldevents_lsprep.py

# Step 2: Label recombination and reconciliation  
python3 02.1.2_neural_net_training_ldevents_label_recombine.py

# Step 3: XGBoost model training and evaluation
python3 02.1.3_train_xgboost_model.py

# Optional: Model comparison and validation
python3 model_comparison_validation.py
```

### Training Configuration
Models use the following parameters optimized for mining telemetry:

**Neural Network**:
- Window size: 60 seconds (120 data points at 2Hz)
- LSTM layers: 2 layers with 128 units each
- Dropout: 0.3 for regularization
- Loss function: Binary crossentropy with class weighting

**XGBoost**:
- Trees: 100-500 depending on dataset size
- Max depth: 6 for mining operation complexity
- Learning rate: 0.1 with early stopping
- Objective: Binary logistic for load/dump classification

## Label Studio Integration

### Configuration (`load_dump_events_config.xml`)
- Custom labeling interface for mining events
- Time series visualization with telemetry overlay
- Multi-class labeling (load, dump, transport, idle)
- Quality control and inter-annotator agreement

### Workflow
1. **Data Export**: Export telemetry segments to Label Studio format
2. **Manual Annotation**: Domain experts label events using custom interface
3. **Quality Control**: Review and validate annotations
4. **Import**: Incorporate labels back into training pipeline

## Feature Engineering for ML

### Temporal Features
- Rolling averages (5s, 15s, 30s windows)
- Rate of change calculations (speed, load weight, altitude)
- Sequence patterns and state transitions

### Spatial Features
- Distance to operational zones (pits, stockpiles, crusher)
- Geofenced area identification
- Route analysis and typical paths

### Operational Features
- Load weight patterns and thresholds
- Speed profiles during different operations
- Vehicle state transitions and timing

## Model Validation

### Ground Truth Comparison
- Uses hand-labeled data from `lake-605-8-0896` device
- Temporal alignment of predictions with ground truth
- Statistical significance testing of improvements

### Performance Metrics
```python
# Key performance indicators
- Precision: True Positives / (True Positives + False Positives)
- Recall: True Positives / (True Positives + False Negatives)
- F1-Score: Harmonic mean of precision and recall
- AUC-ROC: Area under receiver operating characteristic curve
```

### Cross-Validation Strategy
- **Temporal Split**: Train on earlier data, test on later data
- **Vehicle Split**: Train on subset of vehicles, test on others
- **k-Fold**: 5-fold cross-validation for stability assessment

## Error Analysis

### Common Failure Modes
1. **Edge Events**: Loading/dumping at zone boundaries
2. **Partial Loads**: Incomplete loading cycles
3. **Sensor Anomalies**: Broken load sensors (-99 values)
4. **State Transitions**: Rapid state changes during operations

### Mitigation Strategies
- Ensemble voting across multiple models
- Confidence thresholding for uncertain predictions
- Business rule validation and constraints
- Manual review queue for low-confidence predictions

## Output Tables and Artifacts

### Generated Tables
- **Event Predictions**: Timestamped load/dump events with confidence
- **Model Metadata**: Training parameters, performance metrics
- **Feature Importance**: Ranked feature contributions

### Model Artifacts
- **Neural Network**: Saved TensorFlow/Keras models
- **XGBoost**: Serialized XGBoost models (.pkl/.json)
- **Scalers**: Feature normalization parameters
- **Encoders**: Categorical variable encodings

## Integration with Pipeline

### Upstream Dependencies
- STEP 1: Raw telemetry ingestion
- STEP 2: Data transformation and cleaning

### Downstream Usage
- STEP 4: Feature engineering uses predicted events
- STEP 5: CPD validation against ML-predicted events  
- STEP 6: Model training incorporates ML features

## Performance and Scalability

### Training Performance
- **Neural Network**: 2-4 hours on GPU for full dataset
- **XGBoost**: 30-60 minutes on CPU for feature set
- **Incremental Learning**: Support for model updates

### Inference Performance
- **Batch Prediction**: 10,000+ records/second
- **Real-time**: Sub-second latency for single predictions
- **Memory Usage**: <2GB for loaded models

## Dependencies

### Python Packages
```
tensorflow>=2.8.0
xgboost>=1.6.0
scikit-learn>=1.1.0
pandas>=1.4.0
numpy>=1.21.0
matplotlib>=3.5.0
seaborn>=0.11.0
```

### System Requirements
- Python 3.8+
- 16GB+ RAM for full dataset training
- GPU recommended for neural network training
- 10GB+ storage for model artifacts and logs

## Monitoring and Maintenance

### Model Drift Detection
- Performance monitoring on new data
- Statistical tests for feature distribution changes
- Automated alerts for accuracy degradation

### Retraining Schedule
- Monthly model updates with new labeled data
- Quarterly full retraining with expanded datasets
- Ad-hoc retraining for operational changes

### Model Versioning
- Semantic versioning for model releases
- A/B testing framework for model comparison
- Rollback capability for model regression

## Quality Assurance

### Testing Framework
- Unit tests for data preprocessing
- Integration tests for model pipeline
- Performance benchmarks and regression tests

### Validation Procedures
- Hold-out test set performance validation
- Business stakeholder review of predictions
- Operational validation with domain experts