"""
DataMine v2 - Database Pipeline Module

This module provides functions for loading, preprocessing, and feature engineering
telemetry data from the PostgreSQL database for the hybrid haul cycle detection framework.

Key Features:
- Database connectivity and query execution
- Data preprocessing with null handling and type conversion
- Feature engineering for both 605 (working sensors) and 775G (broken sensors) trucks
- PostGIS geography data extraction
- ENUM type handling

Author: DataMine Development Team
Database: datamine_v2_db
Table: 02_raw_telemetry_transformed
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from scipy.signal import savgol_filter
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from typing import Dict, Optional, Tuple, List
import warnings
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabasePipeline:
    """
    Main pipeline class for DataMine v2 telemetry data processing.
    Handles database connections, data loading, preprocessing, and feature engineering.
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the pipeline with database configuration.
        
        Args:
            db_config: Dictionary with keys: host, port, database, user, password
        """
        self.db_config = db_config
        self.engine = None
        self.label_encoders = {}
        self.connect_to_database()
        
    def connect_to_database(self) -> None:
        """Establish connection to PostgreSQL database."""
        try:
            connection_string = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def load_data_from_db(
        self, 
        device_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        truck_series: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Load telemetry data from database with optional filtering.
        
        Args:
            device_id: Specific device to load (optional)
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional)
            limit: Maximum number of records to return (optional)
            truck_series: '605' or '775G' to filter by truck type (optional)
            
        Returns:
            pandas.DataFrame: Loaded telemetry data with timestamp as index
        """
        logger.info(f"Loading data from database with filters: device_id={device_id}, "
                   f"start_date={start_date}, end_date={end_date}, limit={limit}, truck_series={truck_series}")
        
        # Build base query
        query = '''
        SELECT 
            timestamp,
            device_id,
            system_engaged,
            parking_brake_applied,
            ST_X(current_position::geometry) as longitude,
            ST_Y(current_position::geometry) as latitude,  
            ST_Z(current_position::geometry) as altitude,
            current_speed,
            load_weight,
            state,
            software_state,
            prndl,
            extras
        FROM "02_raw_telemetry_transformed"
        WHERE 1=1
        '''
        
        params = {}
        
        # Add filters
        if device_id:
            query += " AND device_id = :device_id"
            params['device_id'] = device_id
            
        if truck_series:
            if truck_series == '605':
                query += " AND device_id NOT LIKE '%775g%'"
            elif truck_series == '775G':
                query += " AND device_id LIKE '%775g%'"
            else:
                raise ValueError("truck_series must be '605' or '775G'")
                
        if start_date:
            query += " AND timestamp >= :start_date"
            params['start_date'] = start_date
            
        if end_date:
            query += " AND timestamp <= :end_date"
            params['end_date'] = end_date
            
        query += " ORDER BY device_id, timestamp"
        
        if limit:
            query += f" LIMIT {limit}"
            
        try:
            # Execute query and load into DataFrame
            df = pd.read_sql_query(text(query), self.engine, params=params)
            
            if df.empty:
                logger.warning("Query returned no data")
                return df
            
            # Set timestamp as index and ensure it's datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # Add truck series identifier
            df['truck_series'] = df['device_id'].apply(
                lambda x: '775G' if '775g' in x.lower() else '605'
            )
            
            logger.info(f"Loaded {len(df):,} records from {df['device_id'].nunique()} devices")
            logger.info(f"Date range: {df.index.min()} to {df.index.max()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load data from database: {e}")
            raise
    
    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess the raw telemetry data.
        
        Args:
            df: Raw telemetry DataFrame
            
        Returns:
            pandas.DataFrame: Preprocessed data
        """
        logger.info("Starting data preprocessing")
        
        if df.empty:
            logger.warning("Empty DataFrame provided for preprocessing")
            return df
        
        df_processed = df.copy()
        
        # Handle missing values
        df_processed = self._handle_missing_values(df_processed)
        
        # Handle categorical encoding
        df_processed = self._encode_categorical_variables(df_processed)
        
        # Handle geospatial data
        df_processed = self._process_geospatial_data(df_processed)
        
        # Data validation and cleaning
        df_processed = self._clean_and_validate_data(df_processed)
        
        logger.info(f"Preprocessing complete. Final shape: {df_processed.shape}")
        
        return df_processed
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values based on EDA findings."""
        logger.info("Handling missing values")
        
        # For numerical columns, use forward fill then backward fill
        numerical_cols = ['current_speed', 'load_weight', 'longitude', 'latitude', 'altitude']
        for col in numerical_cols:
            if col in df.columns:
                # Fill missing values
                df[col] = df[col].fillna(method='ffill').fillna(method='bfill')
                
        # For boolean columns, fill with False (safer assumption)
        boolean_cols = ['system_engaged', 'parking_brake_applied']
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].fillna(False)
                
        # For categorical columns, use mode or 'unknown'
        categorical_cols = ['state', 'software_state', 'prndl']
        for col in categorical_cols:
            if col in df.columns:
                mode_value = df[col].mode()
                fill_value = mode_value[0] if len(mode_value) > 0 else 'unknown'
                df[col] = df[col].fillna(fill_value)
        
        return df
    
    def _encode_categorical_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode categorical variables (ENUM types)."""
        logger.info("Encoding categorical variables")
        
        # Convert boolean columns to integers
        boolean_cols = ['system_engaged', 'parking_brake_applied']
        for col in boolean_cols:
            if col in df.columns:
                df[f"{col}_int"] = df[col].astype(int)
        
        # Label encode ENUM columns for ordinal relationships
        enum_cols = ['state', 'software_state', 'prndl']
        for col in enum_cols:
            if col in df.columns:
                if col not in self.label_encoders:
                    self.label_encoders[col] = LabelEncoder()
                    df[f"{col}_encoded"] = self.label_encoders[col].fit_transform(df[col].astype(str))
                else:
                    # Handle unseen categories
                    try:
                        df[f"{col}_encoded"] = self.label_encoders[col].transform(df[col].astype(str))
                    except ValueError:
                        # Add new categories to the encoder
                        unique_values = df[col].unique()
                        known_values = self.label_encoders[col].classes_
                        new_values = [val for val in unique_values if val not in known_values]
                        
                        if new_values:
                            all_values = list(known_values) + list(new_values)
                            self.label_encoders[col] = LabelEncoder()
                            self.label_encoders[col].fit(all_values)
                        
                        df[f"{col}_encoded"] = self.label_encoders[col].transform(df[col].astype(str))
        
        return df
    
    def _process_geospatial_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process geospatial data extracted from PostGIS."""
        logger.info("Processing geospatial data")
        
        # Validate coordinate ranges
        if 'longitude' in df.columns and 'latitude' in df.columns:
            # Remove obviously invalid coordinates
            valid_coords = (
                (df['longitude'] >= -180) & (df['longitude'] <= 180) &
                (df['latitude'] >= -90) & (df['latitude'] <= 90)
            )
            
            invalid_count = (~valid_coords).sum()
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} invalid coordinate pairs, setting to NaN")
                df.loc[~valid_coords, ['longitude', 'latitude']] = np.nan
        
        # Create additional geospatial features
        if 'longitude' in df.columns and 'latitude' in df.columns:
            # Calculate distance from center point (simple approximation)
            center_lon = df['longitude'].median()
            center_lat = df['latitude'].median()
            
            df['distance_from_center'] = np.sqrt(
                (df['longitude'] - center_lon)**2 + 
                (df['latitude'] - center_lat)**2
            )
        
        return df
    
    def _clean_and_validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate data based on business rules."""
        logger.info("Cleaning and validating data")
        
        # Speed validation
        if 'current_speed' in df.columns:
            # Remove negative speeds
            negative_speeds = df['current_speed'] < 0
            if negative_speeds.any():
                logger.warning(f"Setting {negative_speeds.sum()} negative speeds to 0")
                df.loc[negative_speeds, 'current_speed'] = 0
            
            # Cap extremely high speeds (>50 m/s ≈ 180 km/h)
            extreme_speeds = df['current_speed'] > 50
            if extreme_speeds.any():
                logger.warning(f"Capping {extreme_speeds.sum()} extreme speeds to 50 m/s")
                df.loc[extreme_speeds, 'current_speed'] = 50
        
        # Load weight validation (only for 605 trucks)
        if 'load_weight' in df.columns and 'truck_series' in df.columns:
            truck_605_mask = df['truck_series'] == '605'
            
            # Remove negative weights for 605 trucks
            negative_weights = (df['load_weight'] < 0) & truck_605_mask
            if negative_weights.any():
                logger.warning(f"Setting {negative_weights.sum()} negative load weights to 0 for 605 trucks")
                df.loc[negative_weights, 'load_weight'] = 0
            
            # Cap extremely high weights (>150,000 kg)
            extreme_weights = (df['load_weight'] > 150000) & truck_605_mask
            if extreme_weights.any():
                logger.warning(f"Capping {extreme_weights.sum()} extreme load weights to 150000 kg")
                df.loc[extreme_weights, 'load_weight'] = 150000
        
        return df
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create engineered features for the hybrid detection framework.
        
        Args:
            df: Preprocessed DataFrame
            
        Returns:
            pandas.DataFrame: DataFrame with engineered features
        """
        logger.info("Starting feature engineering")
        
        if df.empty:
            logger.warning("Empty DataFrame provided for feature engineering")
            return df
        
        df_features = df.copy()
        
        # Basic kinematic features
        df_features = self._create_kinematic_features(df_features)
        
        # Load weight features (for 605 trucks)
        df_features = self._create_load_weight_features(df_features)
        
        # Time-based features
        df_features = self._create_temporal_features(df_features)
        
        # State transition features
        df_features = self._create_state_transition_features(df_features)
        
        # Geospatial features
        df_features = self._create_geospatial_features(df_features)
        
        logger.info(f"Feature engineering complete. Features added: {df_features.shape[1] - df.shape[1]}")
        
        return df_features
    
    def _create_kinematic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create kinematic features from speed and position."""
        logger.info("Creating kinematic features")
        
        if 'current_speed' in df.columns:
            # Is stationary feature (critical for framework)
            df['is_stationary'] = (df['current_speed'] <= 0.5).astype(int)
            
            # Rolling speed statistics
            for window in [5, 10, 30]:  # Different time windows
                df[f'speed_rolling_mean_{window}'] = df['current_speed'].rolling(window=window, min_periods=1).mean()
                df[f'speed_rolling_std_{window}'] = df['current_speed'].rolling(window=window, min_periods=1).std()
            
            # Speed change features
            df['speed_change'] = df['current_speed'].diff()
            df['speed_acceleration'] = df['speed_change'].diff()  # Second derivative
            
            # Speed categories
            df['speed_category'] = pd.cut(
                df['current_speed'], 
                bins=[-np.inf, 0.5, 5, 15, np.inf], 
                labels=['stationary', 'slow', 'medium', 'fast']
            )
            
            # Convert to numeric for ML
            df['speed_category_encoded'] = pd.Categorical(df['speed_category']).codes
        
        return df
    
    def _create_load_weight_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create load weight features (primarily for 605 trucks)."""
        logger.info("Creating load weight features")
        
        if 'load_weight' in df.columns:
            # Smooth load weight signal using Savitzky-Golay filter
            try:
                # Only smooth where we have valid load weight data
                valid_weight_mask = df['load_weight'].notna()
                if valid_weight_mask.any():
                    # Apply smoothing in chunks to handle NaN values
                    df['load_weight_smoothed'] = df['load_weight'].copy()
                    
                    for device_id in df['device_id'].unique():
                        device_mask = df['device_id'] == device_id
                        device_data = df.loc[device_mask, 'load_weight'].dropna()
                        
                        if len(device_data) > 10:  # Need minimum points for smoothing
                            smoothed = savgol_filter(device_data, window_length=min(11, len(device_data)), polyorder=2)
                            df.loc[device_mask & valid_weight_mask, 'load_weight_smoothed'] = smoothed
                            
            except Exception as e:
                logger.warning(f"Failed to smooth load weight: {e}")
                df['load_weight_smoothed'] = df['load_weight']
            
            # Rate of change (critical for CPD stage)
            df['rate_of_change_weight'] = df['load_weight_smoothed'].diff()
            
            # Rolling statistics
            for window in [5, 10, 30]:
                df[f'weight_rolling_mean_{window}'] = df['load_weight_smoothed'].rolling(window=window, min_periods=1).mean()
                df[f'weight_rolling_std_{window}'] = df['load_weight_smoothed'].rolling(window=window, min_periods=1).std()
            
            # Load status categories
            df['load_status'] = pd.cut(
                df['load_weight_smoothed'],
                bins=[-np.inf, 1000, 20000, 50000, np.inf],
                labels=['empty', 'light', 'medium', 'heavy']
            )
            df['load_status_encoded'] = pd.Categorical(df['load_status']).codes
            
            # Binary loaded/unloaded feature
            df['is_loaded'] = (df['load_weight_smoothed'] > 5000).astype(int)  # 5 ton threshold
            
        return df
    
    def _create_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create time-based features."""
        logger.info("Creating temporal features")
        
        # Extract temporal components
        df['hour'] = df.index.hour
        df['day_of_week'] = df.index.dayofweek
        df['day_of_year'] = df.index.dayofyear
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # Shift categories (based on mining operations)
        df['shift'] = pd.cut(
            df['hour'],
            bins=[-1, 6, 14, 22, 24],
            labels=['night', 'day', 'evening', 'night']
        )
        df['shift_encoded'] = pd.Categorical(df['shift']).codes
        
        # Time since last gear/state changes
        for col in ['prndl_encoded', 'state_encoded']:
            if col in df.columns:
                df[f'time_since_last_{col.replace("_encoded", "")}_change'] = (
                    df.groupby('device_id')[col].apply(
                        lambda x: (x != x.shift()).cumsum()
                    ).groupby(level=0).cumcount()
                )
        
        return df
    
    def _create_state_transition_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create features based on state transitions."""
        logger.info("Creating state transition features")
        
        # Previous state features
        state_cols = ['state_encoded', 'software_state_encoded', 'prndl_encoded']
        for col in state_cols:
            if col in df.columns:
                df[f'{col}_prev'] = df.groupby('device_id')[col].shift(1)
                df[f'{col}_changed'] = (df[col] != df[f'{col}_prev']).astype(int)
        
        # Sequence features
        if 'state_encoded' in df.columns:
            # State persistence (how long in current state)
            df['state_persistence'] = df.groupby(['device_id', 'state_encoded']).cumcount() + 1
            
            # State sequence pattern (last 3 states)
            df['state_seq_1'] = df.groupby('device_id')['state_encoded'].shift(1)
            df['state_seq_2'] = df.groupby('device_id')['state_encoded'].shift(2)
            
        return df
    
    def _create_geospatial_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create geospatial features for zone identification."""
        logger.info("Creating geospatial features")
        
        if all(col in df.columns for col in ['longitude', 'latitude']):
            # Movement features
            df['position_change'] = np.sqrt(
                (df.groupby('device_id')['longitude'].diff())**2 +
                (df.groupby('device_id')['latitude'].diff())**2
            )
            
            # Altitude change
            if 'altitude' in df.columns:
                df['altitude_change'] = df.groupby('device_id')['altitude'].diff()
            
            # Distance from depot/center (placeholder - would need actual depot coordinates)
            center_lon = df['longitude'].median()
            center_lat = df['latitude'].median()
            
            df['distance_from_center'] = np.sqrt(
                (df['longitude'] - center_lon)**2 + 
                (df['latitude'] - center_lat)**2
            )
            
            # Geospatial clustering placeholder (would implement DBSCAN for operational zones)
            # For now, create simple grid-based zones
            lon_bins = pd.qcut(df['longitude'], q=5, labels=False, duplicates='drop')
            lat_bins = pd.qcut(df['latitude'], q=5, labels=False, duplicates='drop') 
            df['grid_zone'] = lon_bins * 5 + lat_bins
        
        return df
    
    def create_pipeline_summary(self, df: pd.DataFrame) -> Dict:
        """
        Create a summary of the pipeline processing results.
        
        Args:
            df: Final processed DataFrame
            
        Returns:
            Dict: Summary statistics and information
        """
        summary = {
            'processing_timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'devices': df['device_id'].nunique() if 'device_id' in df.columns else 0,
            'date_range': {
                'start': df.index.min().isoformat() if len(df) > 0 else None,
                'end': df.index.max().isoformat() if len(df) > 0 else None,
                'days': (df.index.max() - df.index.min()).days if len(df) > 0 else 0
            },
            'truck_breakdown': df['truck_series'].value_counts().to_dict() if 'truck_series' in df.columns else {},
            'features_created': df.shape[1],
            'data_quality': {
                'missing_values': df.isnull().sum().sum(),
                'completeness_percent': (1 - df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
            }
        }
        
        return summary


# Convenience functions for common operations
def create_default_db_config() -> Dict[str, str]:
    """Create default database configuration for DataMine v2."""
    return {
        'host': 'localhost',
        'port': '5432',
        'database': 'datamine_v2_db',
        'user': 'ahs_user',
        'password': 'ahs_password'
    }

def load_and_process_data(
    device_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    truck_series: Optional[str] = None
) -> Tuple[pd.DataFrame, Dict]:
    """
    Convenience function to load and fully process data in one call.
    
    Returns:
        Tuple[pd.DataFrame, Dict]: Processed data and pipeline summary
    """
    # Create pipeline
    db_config = create_default_db_config()
    pipeline = DatabasePipeline(db_config)
    
    # Load data
    raw_data = pipeline.load_data_from_db(
        device_id=device_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        truck_series=truck_series
    )
    
    if raw_data.empty:
        return raw_data, {'message': 'No data loaded'}
    
    # Process data
    processed_data = pipeline.preprocess_data(raw_data)
    feature_data = pipeline.engineer_features(processed_data)
    
    # Create summary
    summary = pipeline.create_pipeline_summary(feature_data)
    
    return feature_data, summary

if __name__ == "__main__":
    # Example usage
    print("DataMine v2 Database Pipeline")
    print("=============================")
    
    # Test with a small sample
    try:
        data, summary = load_and_process_data(limit=1000, truck_series='605')
        print(f"\\nProcessed {summary['total_records']:,} records from {summary['devices']} devices")
        print(f"Features created: {summary['features_created']}")
        print(f"Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
        print(f"Data completeness: {summary['data_quality']['completeness_percent']:.2f}%")
        
        # Show sample of features
        if not data.empty:
            print(f"\\nSample features:")
            feature_cols = [col for col in data.columns if any(keyword in col for keyword in 
                          ['is_', 'rate_', 'rolling_', 'change', 'encoded', 'smoothed'])]
            print(data[feature_cols].head())
            
    except Exception as e:
        print(f"Error during pipeline test: {e}")
        print("Please ensure database connection is available.")