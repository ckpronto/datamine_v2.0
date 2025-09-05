#!/usr/bin/env python3
"""
XGBoost Load/Dump Event Detection V1.3 - Stable Velocity Features
AHS Analytics Engine

Train XGBoost on the 3-class load/dump detection task with STABLE velocity features.
This version replaces np.gradient() with 10-step rolling differences to eliminate
the alternating prediction issue caused by gradient oscillations.

Classes:
- background (0): Normal operations  
- load_event (1): Truck being loaded
- dump_event (2): Truck dumping

Author: AHS Analytics Engine
Created: 2025-09-02
Version: 1.3 - Stable Velocity Features (Fixed np.gradient oscillations)
"""

import json
import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import xgboost as xgb
import psycopg2
from scipy.signal import savgol_filter
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("xgboost_v1.3_stable_features_training.log", mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class StableLoadDumpXGBoostTrainer:
    """V1.3 XGBoost trainer for 3-class load/dump event detection with stable velocity features."""
    
    def __init__(self):
        """Initialize the trainer."""
        self.model = None
        self.label_encoder = None
        self.feature_scaler = None
        self.feature_names = None
        self.results = {}
        
        # Database connection parameters
        self.db_params = {
            'host': 'localhost',
            'port': '5432',
            'database': 'datamine_main_db',
            'user': 'ahs_user',
            'password': 'ahs_password'
        }
        
        # Enhanced XGBoost hyperparameters
        self.xgb_params = {
            'objective': 'multi:softprob',
            'num_class': 3,  # 3 classes: background, load_event, dump_event
            'max_depth': 8,  # Increased for more complex patterns with state
            'learning_rate': 0.1,
            'n_estimators': 500,  # More trees for better performance
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42,
            'tree_method': 'hist',
            'enable_categorical': False,
            'n_jobs': 100,  # Use all 100 CPUs
        }
        
    def extract_training_data(self) -> pd.DataFrame:
        """Extract training data from the recombined labeled dataset including ALL label files."""
        logger.info("V1.3: Extracting training data with ALL Label Studio files from database...")
        
        query = """
        SELECT 
            timestamp,
            device_id,
            ml_event_label,
            ST_X(current_position::geometry) as longitude,
            ST_Y(current_position::geometry) as latitude, 
            ST_Z(current_position::geometry) as altitude,
            current_speed,
            load_weight,
            prndl::text as prndl_text,
            system_engaged,
            parking_brake_applied,
            state,
            software_state
        FROM "02.1_labeled_training_data_loaddump"
        WHERE current_speed IS NOT NULL 
        AND load_weight IS NOT NULL
        AND ml_event_label IS NOT NULL
        AND state IS NOT NULL
        AND software_state IS NOT NULL
        ORDER BY device_id, timestamp;
        """
        
        try:
            conn = psycopg2.connect(**self.db_params)
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            logger.info(f"V1.3 Extracted {len(df):,} samples from ALL label files")
            logger.info("V1.3 Class distribution:")
            class_counts = df['ml_event_label'].value_counts()
            for label, count in class_counts.items():
                percentage = (count / len(df)) * 100
                logger.info(f"  - {label}: {count:,} ({percentage:.1f}%)")
            
            # Show state distributions
            logger.info("\nState distribution:")
            state_counts = df['state'].value_counts()
            for state, count in state_counts.items():
                percentage = (count / len(df)) * 100
                logger.info(f"  - {state}: {count:,} ({percentage:.1f}%)")
                
            logger.info("\nSoftware state distribution:")
            sw_state_counts = df['software_state'].value_counts()
            for sw_state, count in sw_state_counts.items():
                percentage = (count / len(df)) * 100
                logger.info(f"  - {sw_state}: {count:,} ({percentage:.1f}%)")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            raise
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for load/dump detection INCLUDING state features."""
        logger.info("Engineering features for load/dump detection WITH STATE...")
        
        df = df.copy()
        
        # Sort by device and timestamp to ensure proper time series operations
        df = df.sort_values(['device_id', 'timestamp'])
        
        # Apply Savitzky-Golay filter to load_weight (per device)
        df['load_weight_filtered'] = df.groupby('device_id')['load_weight'].transform(
            lambda x: savgol_filter(x, window_length=min(15, len(x)), polyorder=3) if len(x) >= 5 else x
        )
        
        # Calculate load weight velocity using 10-step rolling difference (stable, non-oscillating)
        # This replaces np.gradient which caused alternating predictions in inference
        df['load_weight_velocity'] = df.groupby('device_id')['load_weight_filtered'].transform(
            lambda x: x.diff(periods=10)  # 10 timesteps = ~5 seconds at 2Hz
        )
        
        # Calculate altitude change velocity using 10-step rolling difference
        df['altitude_velocity'] = df.groupby('device_id')['altitude'].transform(
            lambda x: x.diff(periods=10)  # 10 timesteps = ~5 seconds at 2Hz
        )
        
        # Create is_stationary feature (speed below threshold)
        df['is_stationary'] = (df['current_speed'] < 5.0).astype(int)
        
        # Create squared features
        df['speed_squared'] = df['current_speed'] ** 2
        df['load_squared'] = df['load_weight_filtered'] ** 2
        
        # One-hot encode prndl
        prndl_dummies = pd.get_dummies(df['prndl_text'], prefix='prndl')
        df = pd.concat([df, prndl_dummies], axis=1)
        
        # **NEW: One-hot encode state - THIS IS THE KEY ADDITION**
        logger.info("🚀 ONE-HOT ENCODING STATE - The Missing Feature!")
        state_dummies = pd.get_dummies(df['state'], prefix='state')
        df = pd.concat([df, state_dummies], axis=1)
        
        # **NEW: One-hot encode software_state**
        logger.info("🚀 ONE-HOT ENCODING SOFTWARE_STATE")
        sw_state_dummies = pd.get_dummies(df['software_state'], prefix='sw_state')
        df = pd.concat([df, sw_state_dummies], axis=1)
        
        # Convert boolean columns to integers
        df['system_engaged'] = df['system_engaged'].astype(int)
        df['parking_brake_applied'] = df['parking_brake_applied'].astype(int)
        
        logger.info(f"V1.3 feature engineering complete. Total features: {df.shape[1]}")
        return df
    
    def prepare_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Prepare features and labels for training WITH STATE FEATURES."""
        logger.info("Preparing features and labels WITH STATE...")
        
        # Base feature columns (same as before)
        feature_cols = [
            'longitude', 'latitude', 'altitude', 'current_speed', 'load_weight_filtered',
            'load_weight_velocity', 'altitude_velocity', 'is_stationary', 
            'speed_squared', 'load_squared', 'system_engaged', 'parking_brake_applied'
        ]
        
        # Add prndl one-hot encoded columns
        prndl_cols = [col for col in df.columns if col.startswith('prndl_') and col != 'prndl_text']
        feature_cols.extend(prndl_cols)
        
        # **NEW: Add state one-hot encoded columns - THE GAME CHANGER**
        state_cols = [col for col in df.columns if col.startswith('state_')]
        feature_cols.extend(state_cols)
        logger.info(f"🎯 Added STATE features: {state_cols}")
        
        # **NEW: Add software_state one-hot encoded columns**
        sw_state_cols = [col for col in df.columns if col.startswith('sw_state_')]
        feature_cols.extend(sw_state_cols)
        logger.info(f"🎯 Added SOFTWARE_STATE features: {sw_state_cols}")
        
        self.feature_names = feature_cols
        logger.info(f"ENHANCED feature set: {len(feature_cols)} features")
        logger.info(f"State-related features: {len(state_cols + sw_state_cols)} out of {len(feature_cols)}")
        
        # Extract features
        X = df[feature_cols].values
        
        # Handle missing values
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        # Encode labels
        self.label_encoder = LabelEncoder()
        y = self.label_encoder.fit_transform(df['ml_event_label'])
        
        logger.info(f"Label mapping: {dict(zip(self.label_encoder.classes_, range(len(self.label_encoder.classes_))))})")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        logger.info(f"Data split: {X_train.shape[0]:,} train, {X_test.shape[0]:,} test")
        
        return X_train, X_test, y_train, y_test
    
    def train_model(self, X_train: np.ndarray, y_train: np.ndarray, X_test: np.ndarray, y_test: np.ndarray):
        """Train Enhanced XGBoost model."""
        logger.info("🚀 Training ENHANCED XGBoost model with STATE features...")
        logger.info(f"Enhanced hyperparameters: {self.xgb_params}")
        
        start_time = datetime.now()
        
        # Initialize and train model
        self.model = xgb.XGBClassifier(**self.xgb_params)
        self.model.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=False
        )
        
        training_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Enhanced training completed in {training_time:.2f} seconds")
        
        # Store training metadata
        self.results.update({
            'training_time_seconds': training_time,
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'num_features': X_train.shape[1],
            'hyperparameters': self.xgb_params,
            'model_version': '1.1_all_labels',
            'enhancement': 'Dynamic loading of all Label Studio files including stockpile operations'
        })
    
    def evaluate_model(self, X_test: np.ndarray, y_test: np.ndarray) -> Dict[str, Any]:
        """Evaluate the enhanced model."""
        logger.info("Evaluating ENHANCED model with STATE features...")
        
        # Make predictions
        y_pred = self.model.predict(X_test)
        y_pred_proba = self.model.predict_proba(X_test)
        
        # Calculate accuracy
        test_accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"🎉 ENHANCED Test Accuracy: {test_accuracy:.4f} ({test_accuracy*100:.2f}%)")
        
        # Generate classification report
        class_names = self.label_encoder.classes_
        class_report = classification_report(
            y_test, y_pred, 
            target_names=class_names, 
            output_dict=True,
            zero_division=0
        )
        
        # Generate confusion matrix
        conf_matrix = confusion_matrix(y_test, y_pred)
        
        # Compare to baselines
        original_model_accuracy = 0.9555  # 95.55%
        state_machine_baseline = 0.6035  # 60.35%
        improvement_vs_original = test_accuracy - original_model_accuracy
        improvement_vs_baseline = test_accuracy - state_machine_baseline
        
        logger.info(f"\n🎯 ENHANCED MODEL COMPARISON:")
        logger.info(f"  Enhanced XGBoost: {test_accuracy:.4f} ({test_accuracy*100:.2f}%)")
        logger.info(f"  Original XGBoost:  {original_model_accuracy:.4f} ({original_model_accuracy*100:.2f}%)")
        logger.info(f"  State Machine:     {state_machine_baseline:.4f} ({state_machine_baseline*100:.2f}%)")
        logger.info(f"  Improvement vs Original: {improvement_vs_original:+.4f} ({improvement_vs_original*100:+.2f} pp)")
        logger.info(f"  Improvement vs Baseline: {improvement_vs_baseline:+.4f} ({improvement_vs_baseline*100:+.2f} pp)")
        
        if test_accuracy > original_model_accuracy:
            logger.info("🎉 SUCCESS: Enhanced model exceeds original XGBoost!")
        else:
            logger.info("⚠️ Enhanced model does not exceed original (unexpected)")
            
        if test_accuracy > 0.99:
            logger.info("🏆 EXCEPTIONAL: Model achieves >99% accuracy with state features!")
        
        # Log per-class performance
        logger.info("\nEnhanced Per-class Performance:")
        for class_name in class_names:
            if class_name in class_report:
                metrics = class_report[class_name]
                logger.info(f"  {class_name}:")
                logger.info(f"    Precision: {metrics['precision']:.4f}")
                logger.info(f"    Recall:    {metrics['recall']:.4f}")
                logger.info(f"    F1-Score:  {metrics['f1-score']:.4f}")
                logger.info(f"    Support:   {metrics['support']}")
        
        return {
            'test_accuracy': float(test_accuracy),
            'classification_report': class_report,
            'confusion_matrix': conf_matrix.tolist(),
            'baseline_comparison': {
                'state_machine_baseline': state_machine_baseline,
                'original_xgboost': original_model_accuracy,
                'improvement_vs_original': float(improvement_vs_original),
                'improvement_vs_baseline': float(improvement_vs_baseline),
                'exceeds_original': bool(test_accuracy > original_model_accuracy),
                'exceeds_baseline': bool(test_accuracy > state_machine_baseline)
            },
            'predictions_sample': {
                'true_labels': y_test[:10].tolist(),
                'predicted_labels': y_pred[:10].tolist(),
                'prediction_probabilities': y_pred_proba[:10].tolist()
            }
        }
    
    def generate_feature_importance_plot(self):
        """Generate enhanced feature importance visualization."""
        logger.info("Generating ENHANCED feature importance plot...")
        
        importance_scores = self.model.feature_importances_
        feature_importance_data = list(zip(self.feature_names, importance_scores))
        feature_importance_data.sort(key=lambda x: x[1], reverse=True)
        
        # Create plot
        plt.figure(figsize=(14, 10))
        top_features = feature_importance_data[:20]  # Top 20 features (more due to state features)
        feature_names_top, importance_scores_top = zip(*top_features)
        
        # Color code features by type
        colors = []
        for feature in feature_names_top:
            if 'state_' in feature:
                colors.append('red')  # State features in red
            elif 'sw_state_' in feature:
                colors.append('orange')  # Software state in orange  
            elif 'prndl_' in feature:
                colors.append('blue')  # PRNDL in blue
            else:
                colors.append('green')  # Other features in green
        
        bars = plt.barh(range(len(feature_names_top)), importance_scores_top, color=colors)
        
        plt.yticks(range(len(feature_names_top)), feature_names_top)
        plt.xlabel('Feature Importance Score')
        plt.title('Enhanced XGBoost Feature Importance - WITH STATE FEATURES\\nAHS Analytics Engine v2.0')
        plt.gca().invert_yaxis()
        
        # Add legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='red', label='State Features'),
            Patch(facecolor='orange', label='Software State'),
            Patch(facecolor='blue', label='PRNDL Features'),
            Patch(facecolor='green', label='Sensor Features')
        ]
        plt.legend(handles=legend_elements, loc='lower right')
        
        # Add value labels
        for i, (bar, score) in enumerate(zip(bars, importance_scores_top)):
            plt.text(bar.get_width() + max(importance_scores_top) * 0.01, 
                     bar.get_y() + bar.get_height()/2, 
                     f'{score:.4f}', va='center', ha='left', fontsize=8)
        
        plt.tight_layout()
        plt.savefig('xgboost_enhanced_feature_importance.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # Log top features
        logger.info("🎯 Top 15 Most Important Features (Enhanced Model):")
        for i, (feature_name, importance) in enumerate(top_features[:15], 1):
            feature_type = "🔴 STATE" if 'state_' in feature_name else "🟠 SW_STATE" if 'sw_state_' in feature_name else "🔵 PRNDL" if 'prndl_' in feature_name else "🟢 SENSOR"
            logger.info(f"  {i:2d}. {feature_type} {feature_name}: {importance:.6f}")
    
    def generate_confusion_matrix_plot(self, conf_matrix: np.ndarray):
        """Generate enhanced confusion matrix visualization."""
        logger.info("Generating enhanced confusion matrix plot...")
        
        plt.figure(figsize=(10, 8))
        sns.heatmap(conf_matrix, 
                    annot=True, 
                    fmt='d',
                    cmap='Blues',
                    xticklabels=self.label_encoder.classes_,
                    yticklabels=self.label_encoder.classes_,
                    cbar_kws={'label': 'Count'})
        
        plt.title('Enhanced XGBoost Confusion Matrix - WITH STATE FEATURES\\nAHS Analytics Engine v2.0')
        plt.xlabel('Predicted Class')
        plt.ylabel('True Class')
        plt.xticks(rotation=45, ha='right')
        plt.yticks(rotation=0)
        
        plt.tight_layout()
        plt.savefig('xgboost_enhanced_confusion_matrix.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def save_results(self, evaluation_results: Dict[str, Any]):
        """Save comprehensive enhanced results."""
        final_results = {
            'experiment_name': 'XGBoost V1.3 Load/Dump Event Detection - Stable Velocity Features',
            'experiment_date': datetime.now().isoformat(),
            'model_version': '1.3_stable_velocity',
            'model_type': 'XGBoost Classifier (3-class: background, load_event, dump_event)',
            'task': 'Load/Dump Event Detection',
            'enhancement': 'Dynamic loading of all Label Studio files including stockpile operations',
            'data_source': 'All labelstudio_output/*.json files + 00_ground_truth_xgboost_training_data',
            'classes': self.label_encoder.classes_.tolist(),
            'feature_names': self.feature_names,
            'num_features': len(self.feature_names),
            'state_features_included': True,
            **self.results,
            **evaluation_results
        }
        
        # Save results
        with open('xgboost_enhanced_results.json', 'w') as f:
            json.dump(final_results, f, indent=2, default=str)
        
        # Save enhanced model V1.1
        model_save_path = "/home/ck/DataMine/APPLICATION/NN MODELS/2.1 Load Dump XGBoost/xgboost_enhanced_model_v1.3.json"
        self.model.save_model(model_save_path)
        logger.info(f"Model V1.3 saved to: {model_save_path}")
        
        logger.info("🎉 V1.3 stable velocity model training completed successfully!")
        
        return final_results


def main():
    """Main execution function for enhanced training."""
    logger.info("🚀 Starting ENHANCED XGBoost Load/Dump Event Detection Training")
    logger.info("🎯 Objective: Include STATE features to dramatically improve accuracy")
    
    try:
        # Initialize enhanced trainer
        trainer = StableLoadDumpXGBoostTrainer()
        
        # Extract and prepare data
        df = trainer.extract_training_data()
        df = trainer.engineer_features(df)
        X_train, X_test, y_train, y_test = trainer.prepare_data(df)
        
        # Train enhanced model
        trainer.train_model(X_train, y_train, X_test, y_test)
        
        # Evaluate enhanced model
        evaluation_results = trainer.evaluate_model(X_test, y_test)
        
        # Generate visualizations
        trainer.generate_feature_importance_plot()
        trainer.generate_confusion_matrix_plot(np.array(evaluation_results['confusion_matrix']))
        
        # Save results
        final_results = trainer.save_results(evaluation_results)
        
        logger.info("🎉 ENHANCED XGBoost Load/Dump Detection training completed successfully!")
        logger.info(f"🏆 Final Enhanced Accuracy: {evaluation_results['test_accuracy']:.4f}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Enhanced training failed: {e}")
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)