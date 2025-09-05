#!/usr/bin/env python3
"""
Test script for Two-Phase Validation Strategy Implementation
TICKET-122 PHASE 2: Validate all components work correctly
"""

import sys
import os
import time

# Import all functions from the main script
exec(open('/home/ck/DataMine/APPLICATION/V2/STEP 5 - CPD/05_cpd_parameter_tuning.py').read())

def test_dynamic_data_source():
    """Test dynamic data source parameter functionality"""
    print("\n" + "="*60)
    print("TEST 1: DYNAMIC DATA SOURCE PARAMETER")
    print("="*60)
    
    device_id = 'lake-605-8-0896'
    
    # Test loaddump source
    print("\n[TEST 1.1] Testing 'loaddump' data source...")
    loaddump_events = fetch_ground_truth_events(device_id, limit=10, source='loaddump')
    print(f"Loaddump events retrieved: {len(loaddump_events)}")
    if len(loaddump_events) > 0:
        print(f"  Event types: {loaddump_events['event_label'].value_counts().to_dict()}")
        print(f"  Sources: {loaddump_events['source'].value_counts().to_dict()}")
    
    # Test hand_labeled source
    print("\n[TEST 1.2] Testing 'hand_labeled' data source...")
    hand_labeled_events = fetch_ground_truth_events(device_id, limit=10, source='hand_labeled')
    print(f"Hand-labeled events retrieved: {len(hand_labeled_events)}")
    if len(hand_labeled_events) > 0:
        print(f"  Event types: {hand_labeled_events['event_label'].value_counts().to_dict()}")
        print(f"  Sources: {hand_labeled_events['source'].value_counts().to_dict()}")
    
    return len(loaddump_events) > 0, len(hand_labeled_events) > 0

def test_candidate_selection():
    """Test optimal penalty candidate selection"""
    print("\n" + "="*60)
    print("TEST 2: CANDIDATE SELECTION LOGIC")
    print("="*60)
    
    # Create mock results for testing
    mock_results = [
        {'penalty': 0.1, 'recall': 0.85, 'num_change_points': 15, 'detected_events': 8, 'total_events': 10},
        {'penalty': 0.2, 'recall': 0.90, 'num_change_points': 12, 'detected_events': 9, 'total_events': 10},
        {'penalty': 0.3, 'recall': 0.95, 'num_change_points': 10, 'detected_events': 10, 'total_events': 10},
        {'penalty': 0.4, 'recall': 0.92, 'num_change_points': 8, 'detected_events': 9, 'total_events': 10},
        {'penalty': 0.5, 'recall': 0.88, 'num_change_points': 6, 'detected_events': 8, 'total_events': 10},
        {'penalty': 1.0, 'recall': 0.75, 'num_change_points': 4, 'detected_events': 7, 'total_events': 10},
        {'penalty': 2.0, 'recall': 0.65, 'num_change_points': 2, 'detected_events': 6, 'total_events': 10},
    ]
    
    print(f"[TEST 2.1] Testing with {len(mock_results)} mock results...")
    candidates = identify_top_penalty_candidates(mock_results, top_k=3, min_recall_threshold=0.8)
    print(f"Selected candidates: {candidates}")
    
    expected_top_3 = [0.3, 0.4, 0.2]  # Based on recall ranking
    success = all(c in expected_top_3 for c in candidates[:3])
    print(f"Test result: {'PASS' if success else 'FAIL'}")
    
    return success

def test_minimal_two_phase_execution():
    """Test minimal two-phase execution with limited data"""
    print("\n" + "="*60)
    print("TEST 3: MINIMAL TWO-PHASE EXECUTION")
    print("="*60)
    
    device_id = 'lake-605-8-0896'
    
    try:
        # Test coarse phase with minimal penalty range
        print("\n[TEST 3.1] Testing coarse phase...")
        coarse_penalties = [0.1, 0.5, 1.0]  # Minimal range for testing
        
        coarse_results, coarse_feature_df, coarse_event_windows = tune_penalty_parameter_parallel(
            device_id=device_id,
            pen_range=coarse_penalties,
            sample_limit=5000,  # Reduced for testing
            max_workers=2,      # Limited workers for testing
            data_source='loaddump',
            phase_name='COARSE_TEST'
        )
        
        print(f"Coarse phase results: {len(coarse_results)} successful tests")
        
        if coarse_results:
            # Test candidate selection
            print("\n[TEST 3.2] Testing candidate selection...")
            candidates = identify_top_penalty_candidates(coarse_results, top_k=2, min_recall_threshold=0.0)
            print(f"Selected candidates: {candidates}")
            
            # Test fine-grained phase if candidates available
            if candidates:
                print("\n[TEST 3.3] Testing fine-grained phase...")
                fine_results, fine_feature_df, fine_event_windows = tune_penalty_parameter_parallel(
                    device_id=device_id,
                    pen_range=candidates,
                    sample_limit=5000,
                    max_workers=2,
                    data_source='hand_labeled',
                    phase_name='FINE_TEST'
                )
                
                print(f"Fine-grained phase results: {len(fine_results)} successful tests")
                
                if fine_results:
                    final_optimal = find_optimal_penalty(fine_results)
                    print(f"Final optimal penalty: {final_optimal['penalty']:.3f} (recall: {final_optimal['recall']:.3f})")
                    return True
        
        return False
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run comprehensive test suite"""
    print("TICKET-122 PHASE 2: TWO-PHASE VALIDATION STRATEGY")
    print("Comprehensive Test Suite")
    print("=" * 80)
    
    test_start = time.time()
    
    # Run tests
    test1_loaddump, test1_handlabeled = test_dynamic_data_source()
    test2_success = test_candidate_selection()
    test3_success = test_minimal_two_phase_execution()
    
    test_time = time.time() - test_start
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUITE SUMMARY")
    print("="*80)
    print(f"Total execution time: {test_time:.2f} seconds")
    print(f"")
    print(f"TEST 1 - Dynamic Data Source:")
    print(f"  Loaddump source: {'PASS' if test1_loaddump else 'FAIL'}")
    print(f"  Hand-labeled source: {'PASS' if test1_handlabeled else 'FAIL'}")
    print(f"TEST 2 - Candidate Selection: {'PASS' if test2_success else 'FAIL'}")
    print(f"TEST 3 - Two-Phase Execution: {'PASS' if test3_success else 'FAIL'}")
    
    all_pass = test1_loaddump and test1_handlabeled and test2_success and test3_success
    print(f"\nOVERALL RESULT: {'ALL TESTS PASS - IMPLEMENTATION READY' if all_pass else 'SOME TESTS FAILED - REVIEW NEEDED'}")
    
    if all_pass:
        print("\n" + "="*80)
        print("READY FOR PRODUCTION EXECUTION")
        print("Run: python3 05_cpd_parameter_tuning.py")
        print("="*80)

if __name__ == "__main__":
    main()