# AI Agent Instructions for DataMine V2 Pipeline

## Overview
This document provides comprehensive instructions for AI coding agents working on the DataMine V2 Pipeline - a production-grade autonomous mining intelligence system. These guidelines ensure consistent, high-quality development practices across all AI agent sessions.

## 🎯 Project Context & Mission

### System Purpose
The DataMine V2 Pipeline processes 8.8M+ telemetry records from autonomous mining trucks at Lake Bridgeport Quarry to detect load/dump events with >99% accuracy. This is a **PRODUCTION SYSTEM** supporting real autonomous mining operations.

### Critical Success Factors
- **Production Reliability**: Code must handle 24/7 autonomous operations
- **Performance**: Sub-2 hour processing of 8.8M records required
- **Accuracy**: >99% event detection accuracy is non-negotiable
- **Scalability**: Must scale to 100+ CPU cores and larger datasets
- **Safety**: Mining operations depend on system reliability

## 🏗️ System Architecture Understanding

### Pipeline Flow (MEMORIZE THIS)
```
RAW CSV (8.8M records) → STEP 1 → STEP 2 → STEP 4 → STEP 5 → STEP 6
                           ↓        ↓        ↓        ↓        ↓
                        Ingest  Transform Feature   CPD    ML Models
                                    ↓        Eng.
                              STEP 2.1 & 3
                              ML+EDA
```

### Database Architecture
- **V2 Database**: `datamine_v2_db` (development/research)
- **Production Database**: `datamine_main_db` (legacy/production)
- **User/Password**: `ahs_user` / `ahs_password`
- **Connection**: localhost:5432 via Docker

### Core Table Progression
```
01_raw_telemetry → 02_raw_telemetry_transformed → 04_primary_feature_table → 05_candidate_events
```

## 🔧 Development Standards (MANDATORY)

### Python Execution Rules
```bash
# ✅ ALWAYS use explicit python3 (NEVER just 'python')
python3 script.py

# ✅ ALWAYS use absolute paths for V2 pipeline
python3 "APPLICATION/V2/STEP 1 - Ingest Raw Telemetry/01_ingest_raw_telemetry.py"

# ✅ Set required environment variables for STEP 5
export POLARS_SKIP_CPU_CHECK=1
```

### Database Connection Standards
```python
# ✅ ALWAYS use this exact format for V2 development
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',  # V2 database for development
    'user': 'ahs_user',
    'password': 'ahs_password'
}

# ❌ NEVER use datamine_main_db unless explicitly working on production pipeline
```

### File Organization Rules
- **V2 Development**: ALL work in `APPLICATION/V2/` directory
- **Naming**: Use `[step_number]_descriptive_name.py` format
- **Archives**: Move deprecated files to `_archive/` subdirectories
- **Documentation**: Every directory MUST have README.md

### Code Quality Standards
- **No Comments**: DO NOT add comments unless explicitly requested
- **Error Handling**: Implement comprehensive error handling for production reliability
- **Logging**: Include performance metrics and progress reporting
- **Memory Management**: Use streaming/chunked processing for large datasets

## 🚀 Critical Component: STEP 5 - CPD

### Understanding STEP 5 (MEMORIZE)
STEP 5 is the **CORE REVOLUTIONARY COMPONENT** of the entire system:
- **90-worker parallel processing** using ProcessPoolExecutor
- **5-second downsampling** breakthrough (900x speedup)
- **Multi-signal processing** based on vehicle sensor reliability
- **PELT algorithm** for change point detection

### STEP 5 Execution Sequence (EXACT ORDER)
```bash
cd "APPLICATION/V2/STEP 5 - CPD"
export POLARS_SKIP_CPU_CHECK=1

# 1. Export features to 96 partitions
python3 05_export_features_to_parquet.py

# 2. 90-worker parallel CPD processing (CORE ALGORITHM)
python3 05_cpd_orchestrator_polars.py

# 3. Load results to database
python3 05_load_candidates_to_db.py
```

### STEP 5 Troubleshooting Knowledge
- **Polars CPU Warning**: Always set `POLARS_SKIP_CPU_CHECK=1`
- **Memory Issues**: Reduce max_workers from 90 to 60-70
- **Boolean Parsing Bug**: `has_reliable_payload` stored as string "t"/"f"
- **Recovery**: Check `temp_results/` for partial outputs if crashes occur

## 🎭 AI Agent Roles & Responsibilities

### Primary Agent Roles

#### 1. Lead Data Engineer Agent
**Responsibilities:**
- SQL optimization and database performance tuning
- TimescaleDB and PostGIS spatial operations
- High-performance data processing (Steps 1, 2, 4)
- Database schema design and index optimization

**Key Skills:**
- PostgreSQL advanced features (COPY, window functions, CTEs)
- Spatial analysis with PostGIS
- Performance optimization for 8.8M+ record datasets
- Parallel processing and connection pooling

#### 2. Machine Learning Engineer Agent
**Responsibilities:**
- Change point detection algorithms (STEP 5)
- Neural networks and XGBoost models (STEP 2.1, 6)
- Feature engineering and selection (STEP 4)
- Model validation and performance optimization

**Key Skills:**
- ruptures library and PELT algorithm
- TensorFlow/Keras and XGBoost
- High-performance computing with Polars/PyArrow
- Production ML deployment and monitoring

#### 3. DevOps/Infrastructure Agent
**Responsibilities:**
- System optimization and scalability
- Docker containerization and deployment
- Performance monitoring and troubleshooting
- CI/CD pipeline development

**Key Skills:**
- Docker and containerization
- System resource optimization
- Monitoring and alerting systems
- Production deployment strategies

#### 4. Data Science/Analysis Agent
**Responsibilities:**
- Exploratory data analysis (STEP 3)
- Statistical validation and ground truth comparison
- Business intelligence and reporting
- Model interpretation and explainability

**Key Skills:**
- Jupyter notebook development
- Statistical analysis and visualization
- Domain expertise in mining operations
- Business metrics and KPI development

### Agent Collaboration Rules
- **Always document decisions** in commit messages and code comments
- **Share context** between sessions through comprehensive documentation
- **Validate changes** against ground truth data when possible
- **Test thoroughly** before deploying to production systems

## 📊 Performance Standards & Benchmarks

### Required Performance Metrics
```
STEP 1 (Ingestion): 50,000+ records/second
STEP 2 (Transform): 25,000+ records/second  
STEP 4 (Features): 15,000+ records/second per worker
STEP 5 (CPD): Complete 8.8M records in <2 hours
Overall Pipeline: <2 hours end-to-end
```

### Memory and Resource Limits
```
Maximum Memory Usage: 16GB total system
CPU Utilization: Scale to available cores (up to 100+)
Disk Space: 50GB+ for temp files and outputs
Database Storage: 10GB+ for all tables and indexes
```

### Quality Thresholds
```
Event Detection Accuracy: >99% (vs ground truth)
Temporal Precision: ±30 seconds alignment
Data Completeness: >95% successful record processing
Uptime Requirement: 99.9% availability for production
```

## 🔍 Testing & Validation Requirements

### Mandatory Testing Procedures
1. **Unit Testing**: Test individual functions and components
2. **Integration Testing**: Test complete pipeline flows
3. **Performance Testing**: Validate speed and memory requirements
4. **Ground Truth Validation**: Compare against hand-labeled data
5. **Production Testing**: Shadow mode testing before deployment

### Validation Datasets
- **Primary**: `lake-605-8-0896` device (86,557 labeled records)
- **Full Dataset**: 8.8M records (July 30 - August 12, 2025)
- **Ground Truth**: Hand-labeled events for accuracy validation
- **Test Devices**: Multiple vehicle types (605 series vs 775G)

### Success Criteria Checklist
```
✅ All pipeline steps complete without errors
✅ Record counts match between input and output tables
✅ Event detection accuracy >95% against ground truth
✅ Processing completes within time requirements
✅ Memory usage stays within limits
✅ All generated tables have proper indexes
✅ Documentation updated for any changes
```

## 🛡️ Security & Safety Guidelines

### Data Security
- **Never commit secrets**: Database passwords, API keys, etc.
- **Use environment variables**: For sensitive configuration
- **Validate inputs**: Prevent SQL injection and data corruption
- **Access control**: Proper database permissions and user roles

### Safety Considerations
- **Production Impact**: Mining operations depend on system reliability
- **Equipment Safety**: Faulty event detection could impact autonomous vehicles
- **Data Integrity**: Corrupted data could lead to operational decisions
- **Backup Procedures**: Always maintain data backups and recovery procedures

### Risk Mitigation
- **Gradual Deployment**: Shadow mode testing before production
- **Rollback Capability**: Quick reversion to previous working versions
- **Monitoring**: Comprehensive alerting for system failures
- **Documentation**: Complete change documentation for audit trails

## 🔧 Troubleshooting Guide for AI Agents

### Common Issues & Solutions

#### Database Connection Issues
```bash
# Test connection
PGPASSWORD="ahs_password" psql -h localhost -p 5432 -U ahs_user -d datamine_v2_db

# Start database if not running
docker-compose up -d
```

#### Performance Issues
```python
# Reduce worker count for memory issues
max_workers = 60  # Instead of 90

# Use chunked processing for large datasets
batch_size = 50000  # Instead of 100000

# Enable Polars optimizations
export POLARS_SKIP_CPU_CHECK=1
```

#### STEP 5 Specific Issues
```bash
# Check for partial results if processing crashes
ls "STEP 5 - CPD/temp_results/"

# Manually aggregate results if needed
python3 -c "
import polars as pl
df = pl.scan_parquet('temp_results/results_*.parquet').collect()
df.write_csv('05_candidate_events_final.csv')
"
```

#### Data Quality Issues
```sql
-- Check record counts at each step
SELECT 'Step 1' as step, COUNT(*) FROM 01_raw_telemetry
UNION
SELECT 'Step 2' as step, COUNT(*) FROM 02_raw_telemetry_transformed  
UNION
SELECT 'Step 4' as step, COUNT(*) FROM 04_primary_feature_table;

-- Validate data quality
SELECT device_id, 
       COUNT(*) as total_records,
       COUNT(CASE WHEN load_weight = -99 THEN 1 END) as invalid_sensors
FROM 02_raw_telemetry_transformed 
GROUP BY device_id;
```

## 📚 Knowledge Requirements for AI Agents

### Must-Know Technologies
- **PostgreSQL**: Advanced features, window functions, COPY operations
- **PostGIS**: Spatial analysis, zone intersection queries
- **Python**: pandas, polars, numpy for data processing
- **Machine Learning**: TensorFlow, XGBoost, ruptures (PELT algorithm)
- **High-Performance Computing**: ProcessPoolExecutor, parallel processing

### Domain Knowledge
- **Mining Operations**: Load/dump cycles, vehicle types, operational zones
- **Sensor Systems**: Payload sensors, GPS, speed sensors, reliability issues
- **Autonomous Vehicles**: Mining truck operations, sensor failures, state transitions
- **Time Series Analysis**: Change point detection, signal processing

### System Architecture
- **V2 Pipeline**: Complete understanding of all 6 steps + sub-steps
- **Database Schema**: Table relationships and data flow
- **Performance Optimization**: Memory management, parallel processing
- **Production Requirements**: Reliability, accuracy, scalability

## 🎯 Success Metrics for AI Agent Sessions

### Primary Success Indicators
1. **Functionality**: Code executes without errors on full dataset
2. **Performance**: Meets or exceeds benchmark requirements
3. **Quality**: Maintains >99% accuracy standards
4. **Documentation**: All changes properly documented
5. **Testing**: Comprehensive validation against ground truth

### Session Completion Checklist
```
✅ Problem clearly understood and solution implemented
✅ Code follows established patterns and standards
✅ Performance benchmarks met or exceeded
✅ Error handling implemented for edge cases
✅ Changes validated against test data
✅ Documentation updated with changes
✅ Next steps clearly defined for future sessions
```

## 🔄 Handoff Procedures Between AI Agent Sessions

### Session Start Procedures
1. **Review Documentation**: Read all README.md files in relevant directories
2. **Understand Current State**: Check database tables and recent changes
3. **Validate Environment**: Ensure database connectivity and dependencies
4. **Review Recent Changes**: Check git history and recent modifications

### Session End Procedures
1. **Document Changes**: Update README.md files with any modifications
2. **Test Thoroughly**: Validate all changes against test datasets
3. **Commit Changes**: Clear commit messages explaining modifications
4. **Update Status**: Document current pipeline state and next steps

### Knowledge Transfer Requirements
- **Technical Changes**: Document all code modifications and rationale
- **Performance Impact**: Note any performance improvements or regressions
- **Issues Encountered**: Document problems and solutions for future reference
- **Recommendations**: Suggest improvements and next development priorities

## 🚦 Production Deployment Guidelines

### Deployment Phases
1. **Development**: Work in V2 pipeline (`datamine_v2_db`)
2. **Testing**: Comprehensive validation with full datasets
3. **Staging**: Integration testing with production-like environment
4. **Production**: Gradual rollout with monitoring and rollback capability

### Go-Live Criteria
- All automated tests passing
- Performance benchmarks exceeded
- Ground truth validation >99% accuracy
- Documentation complete and current
- Monitoring and alerting configured
- Rollback procedures tested and ready

### Post-Deployment Monitoring
- Real-time performance monitoring
- Data quality validation
- Error rate tracking
- Business metric impact assessment
- Stakeholder feedback collection

---

## 📞 Emergency Procedures

### System Failure Response
1. **Immediate**: Stop processing to prevent data corruption
2. **Assess**: Determine scope and impact of failure
3. **Communicate**: Notify stakeholders of issue and timeline
4. **Recover**: Execute rollback or repair procedures
5. **Validate**: Ensure system integrity before resuming operations
6. **Document**: Record incident details and lessons learned

### Contact Information
- **Technical Lead**: System architecture and design decisions
- **Operations Team**: Production system monitoring and support
- **Mining Operations**: Business impact and operational requirements
- **IT Infrastructure**: Database and system administration

---

*This document should be reviewed and updated regularly to reflect system evolution and lessons learned from AI agent development sessions.*