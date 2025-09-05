#!/usr/bin/env python3
"""
model_comparison_validation.py

Head-to-Head XGBoost Model Validation Script

This script provides direct comparison between two XGBoost models on a given dataset.
Designed for A/B testing and validation of model performance improvements.

Usage:
    python model_comparison_validation.py \
        --model-a-path /path/to/model_v1.0.json \
        --model-b-path /path/to/model_v1.1.json \
        --dataset-path /path/to/validation_data.csv \
        --target-column event_label

Requirements:
    - pandas
    - xgboost
    - scikit-learn
    - numpy

Author: Claude Development Team
Date: August 31, 2025
"""

import argparse
import os
import sys
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from typing import Dict, Tuple, Any, List


def validate_file_exists(file_path: str, file_type: str) -> None:
    """Validate that a file exists and is accessible."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_type} file not found: {file_path}")
    if not os.path.isfile(file_path):
        raise ValueError(f"{file_type} path is not a file: {file_path}")


def load_dataset(dataset_path: str, target_column: str) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Load dataset and separate features from target variable.
    
    Args:
        dataset_path: Path to the dataset file (CSV or Parquet)
        target_column: Name of the target column
        
    Returns:
        Tuple of (X_features, y_target)
    """
    validate_file_exists(dataset_path, "Dataset")
    
    # Determine file type and load accordingly
    file_extension = os.path.splitext(dataset_path)[1].lower()
    
    if file_extension == '.csv':
        df = pd.read_csv(dataset_path)
    elif file_extension == '.parquet':
        df = pd.read_parquet(dataset_path)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}. Supported: .csv, .parquet")
    
    # Validate target column exists
    if target_column not in df.columns:
        raise ValueError(f"Target column '{target_column}' not found in dataset. "
                        f"Available columns: {list(df.columns)}")
    
    # Separate features and target
    y = df[target_column]
    X = df.drop(columns=[target_column])
    
    print(f"Dataset loaded: {df.shape[0]} rows, {X.shape[1]} features")
    print(f"Target column: {target_column}")
    print(f"Class distribution:\n{y.value_counts().to_string()}")
    
    return X, y


def get_expected_features() -> List[str]:
    """
    Get the expected feature names for AHS XGBoost models.
    This matches the feature engineering in 02.1.3_train_xgboost_model.py
    """
    # Base feature columns (same as training script)
    feature_cols = [
        'longitude', 'latitude', 'altitude', 'current_speed', 'load_weight_filtered',
        'load_weight_velocity', 'altitude_velocity', 'is_stationary', 
        'speed_squared', 'load_squared', 'system_engaged', 'parking_brake_applied'
    ]
    
    # Add prndl one-hot encoded columns (all possible values)
    prndl_cols = ['prndl_drive', 'prndl_neutral', 'prndl_reverse', 'prndl_unknown']
    feature_cols.extend(prndl_cols)
    
    # Add state one-hot encoded columns
    state_cols = ['state_dumping', 'state_hauling', 'state_idle', 'state_loading', 'state_stopped', 'state_unknown']
    feature_cols.extend(state_cols)
    
    # Add software state one-hot encoded columns  
    sw_state_cols = ['sw_state_autonomous', 'sw_state_fault', 'sw_state_intervention', 'sw_state_manual']
    feature_cols.extend(sw_state_cols)
    
    return feature_cols


def evaluate_model(model_path: str, X_test: pd.DataFrame, y_test: pd.Series) -> Dict[str, Any]:
    """
    Evaluate an XGBoost model on test data.
    
    Args:
        model_path: Path to the XGBoost model file
        X_test: Test features
        y_test: Test target labels
        
    Returns:
        Dictionary containing evaluation metrics
    """
    validate_file_exists(model_path, "Model")
    
    # Load XGBoost model first
    try:
        model = xgb.Booster()
        model.load_model(model_path)
    except Exception as e:
        raise ValueError(f"Failed to load XGBoost model from {model_path}: {str(e)}")
    
    # Get the feature names the model expects
    model_features = model.feature_names
    
    # If model doesn't have feature names (common with XGBoost), use expected features
    if model_features is None:
        model_features = get_expected_features()
        print(f"Model feature_names is None, using expected AHS features ({len(model_features)} features)")
        
        # Verify the number matches what the model expects
        if model.num_features() != len(model_features):
            raise ValueError(f"Model expects {model.num_features()} features, but expected feature list has {len(model_features)} features. "
                           f"This indicates a mismatch between the validation script and training script.")
    
    print(f"Model expects {len(model_features)} features: {model_features[:5]}..." if len(model_features) > 5 else f"Model expects features: {model_features}")
    
    # Ensure all required features are in the dataframe
    missing_features = set(model_features) - set(X_test.columns)
    if missing_features:
        available_features = list(X_test.columns)
        raise ValueError(f"Dataset is missing required features: {list(missing_features)}\n"
                        f"Available features in dataset: {available_features}")
    
    # Check for extra features in dataset (for informational purposes)
    extra_features = set(X_test.columns) - set(model_features)
    if extra_features:
        print(f"Dataset contains {len(extra_features)} extra features that will be ignored: {list(extra_features)[:5]}..." if len(extra_features) > 5 else f"Extra features ignored: {list(extra_features)}")
    
    # Filter and reorder the dataframe to match the model's expectations
    X_test_filtered = X_test[model_features]
    print(f"Filtered dataset shape: {X_test_filtered.shape}")
    
    # Prepare data for XGBoost with correctly filtered and ordered features
    try:
        dtest = xgb.DMatrix(X_test_filtered)
    except Exception as e:
        raise ValueError(f"Failed to create DMatrix from filtered features: {str(e)}")
    
    # Make predictions
    try:
        y_pred_proba = model.predict(dtest)
        
        # Handle multi-class predictions
        if len(y_pred_proba.shape) > 1:
            y_pred = np.argmax(y_pred_proba, axis=1)
        else:
            # Binary classification
            y_pred = (y_pred_proba > 0.5).astype(int)
            
    except Exception as e:
        raise ValueError(f"Failed to make predictions with model {model_path}: {str(e)}")
    
    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    class_report = classification_report(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)
    
    return {
        'accuracy': accuracy,
        'classification_report': class_report,
        'confusion_matrix': conf_matrix,
        'predictions': y_pred,
        'features_used': model_features
    }


def format_confusion_matrix(conf_matrix: np.ndarray) -> str:
    """Format confusion matrix for display."""
    matrix_str = "[\n"
    for row in conf_matrix:
        matrix_str += f" {row}\n"
    matrix_str += "]"
    return matrix_str


def print_model_results(model_name: str, model_path: str, results: Dict[str, Any]) -> None:
    """Print formatted results for a single model."""
    print(f"\n{'-'*50}")
    print(f"{model_name}: {os.path.basename(model_path)}")
    print(f"{'-'*50}")
    print(f"Accuracy: {results['accuracy']:.1%}")
    print(f"\nClassification Report:")
    print(results['classification_report'])
    print(f"Confusion Matrix:")
    print(format_confusion_matrix(results['confusion_matrix']))


def compare_models(results_a: Dict[str, Any], results_b: Dict[str, Any]) -> None:
    """Print comparison summary between two models."""
    print(f"\n{'='*50}")
    print("COMPARISON SUMMARY")
    print(f"{'='*50}")
    
    acc_a = results_a['accuracy']
    acc_b = results_b['accuracy']
    
    print(f"Model A Accuracy: {acc_a:.1%}")
    print(f"Model B Accuracy: {acc_b:.1%}")
    
    if acc_b > acc_a:
        improvement = acc_b - acc_a
        print(f"✅ Model B is BETTER by {improvement:.1%} ({improvement*100:.2f} percentage points)")
    elif acc_a > acc_b:
        decline = acc_a - acc_b
        print(f"❌ Model B is WORSE by {decline:.1%} ({decline*100:.2f} percentage points)")
    else:
        print(f"🤝 Models have EQUAL accuracy")
    
    print(f"{'='*50}")


def main():
    """Main function to handle command-line execution."""
    parser = argparse.ArgumentParser(
        description="Compare two XGBoost models on a validation dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python model_comparison_validation.py \\
        --model-a-path /path/to/model_v1.0.json \\
        --model-b-path /path/to/model_v1.1.json \\
        --dataset-path /path/to/validation_data.csv \\
        --target-column event_label
        """
    )
    
    parser.add_argument(
        '--model-a-path',
        type=str,
        required=True,
        help='Absolute file path to the first model (e.g., v1.0.json)'
    )
    
    parser.add_argument(
        '--model-b-path',
        type=str,
        required=True,
        help='Absolute file path to the second model (e.g., v1.1.json)'
    )
    
    parser.add_argument(
        '--dataset-path',
        type=str,
        required=True,
        help='Path to the CSV or Parquet file containing the validation dataset'
    )
    
    parser.add_argument(
        '--target-column',
        type=str,
        required=True,
        help='Name of the column in the dataset that contains the ground truth labels'
    )
    
    args = parser.parse_args()
    
    try:
        print("="*50)
        print("Model Comparison Report")
        print("="*50)
        print(f"Dataset: {args.dataset_path}")
        
        # Load dataset
        print("\nLoading dataset...")
        X, y = load_dataset(args.dataset_path, args.target_column)
        
        # Evaluate Model A
        print(f"\nEvaluating Model A: {args.model_a_path}")
        results_a = evaluate_model(args.model_a_path, X, y)
        
        # Evaluate Model B
        print(f"\nEvaluating Model B: {args.model_b_path}")
        results_b = evaluate_model(args.model_b_path, X, y)
        
        # Print results
        print_model_results("MODEL A", args.model_a_path, results_a)
        print_model_results("MODEL B", args.model_b_path, results_b)
        
        # Print comparison
        compare_models(results_a, results_b)
        
        print(f"\n✅ Model comparison completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during model comparison: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()