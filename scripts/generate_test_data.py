#!/usr/bin/env python3
"""
Test Data Generator for Leaf Application

This script generates comprehensive test data with various distributions,
time columns (both sequential and non-sequential), and different data types
for testing the leaf data analysis application.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import argparse
from typing import List, Dict, Any

def generate_sequential_timestamps(n_rows: int, start_date: str = "2024-01-01", 
                                 interval_minutes: int = 5) -> List[str]:
    """Generate sequential timestamps."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    timestamps = []
    for i in range(n_rows):
        timestamp = start + timedelta(minutes=i * interval_minutes)
        timestamps.append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
    return timestamps

def generate_non_sequential_timestamps(n_rows: int, start_date: str = "2024-01-01") -> List[str]:
    """Generate non-sequential timestamps with gaps and random intervals."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    timestamps = []
    current_time = start
    
    for i in range(n_rows):
        # Add random interval between 1 minute and 2 hours
        interval = random.randint(1, 120)
        current_time += timedelta(minutes=interval)
        
        # Occasionally add large gaps (1-3 days)
        if random.random() < 0.05:  # 5% chance of large gap
            gap_days = random.randint(1, 3)
            current_time += timedelta(days=gap_days)
        
        timestamps.append(current_time.strftime("%Y-%m-%d %H:%M:%S"))
    
    return timestamps

def generate_unix_timestamps(n_rows: int, start_timestamp: int = 1704067200) -> List[int]:
    """Generate Unix timestamps."""
    timestamps = []
    for i in range(n_rows):
        # Add random intervals between 1-3600 seconds
        interval = random.randint(1, 3600)
        timestamp = start_timestamp + (i * interval)
        timestamps.append(timestamp)
    return timestamps

def generate_time_only_data(n_rows: int) -> List[str]:
    """Generate time-only data (HH:MM:SS format)."""
    times = []
    for i in range(n_rows):
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        times.append(time_str)
    return times

def generate_distributions(n_rows: int) -> Dict[str, List[Any]]:
    """Generate data with various statistical distributions."""
    
    # Normal distribution
    normal_data = np.random.normal(100, 20, n_rows)
    
    # Exponential distribution
    exponential_data = np.random.exponential(50, n_rows)
    
    # Uniform distribution
    uniform_data = np.random.uniform(0, 200, n_rows)
    
    # Log-normal distribution
    lognormal_data = np.random.lognormal(4, 1, n_rows)
    
    # Poisson distribution
    poisson_data = np.random.poisson(30, n_rows)
    
    # Gamma distribution
    gamma_data = np.random.gamma(2, 25, n_rows)
    
    # Beta distribution
    beta_data = np.random.beta(2, 5, n_rows) * 100
    
    # Weibull distribution
    weibull_data = np.random.weibull(2, n_rows) * 50
    
    return {
        'normal_dist': normal_data.tolist(),
        'exponential_dist': exponential_data.tolist(),
        'uniform_dist': uniform_data.tolist(),
        'lognormal_dist': lognormal_data.tolist(),
        'poisson_dist': poisson_data.tolist(),
        'gamma_dist': gamma_data.tolist(),
        'beta_dist': beta_data.tolist(),
        'weibull_dist': weibull_data.tolist()
    }

def generate_categorical_data(n_rows: int) -> Dict[str, List[str]]:
    """Generate categorical data."""
    
    # Categories with different frequencies
    categories = ['A', 'B', 'C', 'D', 'E']
    category_weights = [0.4, 0.25, 0.2, 0.1, 0.05]  # Uneven distribution
    
    categorical_data = np.random.choice(categories, n_rows, p=category_weights)
    
    # Binary data
    binary_data = np.random.choice(['Yes', 'No'], n_rows, p=[0.6, 0.4])
    
    # Status data
    status_options = ['Active', 'Inactive', 'Pending', 'Completed']
    status_weights = [0.5, 0.2, 0.2, 0.1]
    status_data = np.random.choice(status_options, n_rows, p=status_weights)
    
    # Region data
    regions = ['North', 'South', 'East', 'West', 'Central']
    region_weights = [0.25, 0.25, 0.2, 0.2, 0.1]
    region_data = np.random.choice(regions, n_rows, p=region_weights)
    
    return {
        'category': categorical_data.tolist(),
        'binary_flag': binary_data.tolist(),
        'status': status_data.tolist(),
        'region': region_data.tolist()
    }

def generate_sequential_data(n_rows: int) -> Dict[str, List[Any]]:
    """Generate sequential data with trends and patterns."""
    
    # Linear trend with noise
    x = np.linspace(0, 100, n_rows)
    linear_trend = 10 + 2 * x + np.random.normal(0, 5, n_rows)
    
    # Cyclical data (sine wave with noise)
    cyclical_data = 50 + 20 * np.sin(2 * np.pi * x / 20) + np.random.normal(0, 3, n_rows)
    
    # Step function with noise
    step_data = []
    for i in range(n_rows):
        if i < n_rows // 3:
            step_data.append(20 + np.random.normal(0, 2))
        elif i < 2 * n_rows // 3:
            step_data.append(40 + np.random.normal(0, 2))
        else:
            step_data.append(60 + np.random.normal(0, 2))
    
    # ID column
    id_data = list(range(1, n_rows + 1))
    
    return {
        'id': id_data,
        'linear_trend': linear_trend.tolist(),
        'cyclical_data': cyclical_data.tolist(),
        'step_function': step_data
    }

def generate_outlier_data(n_rows: int) -> Dict[str, List[float]]:
    """Generate data with outliers."""
    
    # Normal data with some outliers
    normal_with_outliers = np.random.normal(100, 20, n_rows)
    
    # Add outliers
    outlier_indices = random.sample(range(n_rows), n_rows // 20)  # 5% outliers
    for idx in outlier_indices:
        if random.random() < 0.5:
            normal_with_outliers[idx] = random.uniform(200, 300)  # High outliers
        else:
            normal_with_outliers[idx] = random.uniform(-50, 0)    # Low outliers
    
    # Data with missing values
    data_with_nulls = normal_with_outliers.copy()
    null_indices = random.sample(range(n_rows), n_rows // 10)  # 10% nulls
    for idx in null_indices:
        data_with_nulls[idx] = np.nan
    
    return {
        'data_with_outliers': normal_with_outliers.tolist(),
        'data_with_nulls': data_with_nulls.tolist()
    }

def generate_test_data(n_rows: int = 1000, output_dir: str = "test_data") -> None:
    """Generate comprehensive test data and save to CSV files."""
    
    print(f"Generating {n_rows} rows of test data...")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate all data components
    print("Generating distributions...")
    distributions = generate_distributions(n_rows)
    
    print("Generating categorical data...")
    categorical = generate_categorical_data(n_rows)
    
    print("Generating sequential data...")
    sequential = generate_sequential_data(n_rows)
    
    print("Generating outlier data...")
    outlier_data = generate_outlier_data(n_rows)
    
    print("Generating time data...")
    # Time data
    sequential_timestamps = generate_sequential_timestamps(n_rows)
    non_sequential_timestamps = generate_non_sequential_timestamps(n_rows)
    unix_timestamps = generate_unix_timestamps(n_rows)
    time_only_data = generate_time_only_data(n_rows)
    
    # Create different datasets for different testing scenarios
    
    # Dataset 1: Comprehensive dataset with all features
    print("Creating comprehensive dataset...")
    comprehensive_data = {
        'id': sequential['id'],
        'timestamp_sequential': sequential_timestamps,
        'timestamp_non_sequential': non_sequential_timestamps,
        'unix_timestamp': unix_timestamps,
        'time_only': time_only_data,
        'normal_dist': distributions['normal_dist'],
        'exponential_dist': distributions['exponential_dist'],
        'uniform_dist': distributions['uniform_dist'],
        'category': categorical['category'],
        'binary_flag': categorical['binary_flag'],
        'status': categorical['status'],
        'region': categorical['region'],
        'linear_trend': sequential['linear_trend'],
        'cyclical_data': sequential['cyclical_data'],
        'data_with_outliers': outlier_data['data_with_outliers'],
        'data_with_nulls': outlier_data['data_with_nulls']
    }
    
    df_comprehensive = pd.DataFrame(comprehensive_data)
    df_comprehensive.to_csv(f"{output_dir}/comprehensive_test_data.csv", index=False)
    
    # Dataset 2: Time series focused dataset
    print("Creating time series dataset...")
    time_series_data = {
        'timestamp': sequential_timestamps,
        'value': distributions['normal_dist'],
        'category': categorical['category'],
        'trend': sequential['linear_trend']
    }
    
    df_timeseries = pd.DataFrame(time_series_data)
    df_timeseries.to_csv(f"{output_dir}/time_series_data.csv", index=False)
    
    # Dataset 3: Data with gaps (for testing time grouping)
    print("Creating dataset with time gaps...")
    gap_data = {
        'timestamp': non_sequential_timestamps,
        'value': distributions['exponential_dist'],
        'category': categorical['category']
    }
    
    df_gaps = pd.DataFrame(gap_data)
    df_gaps.to_csv(f"{output_dir}/data_with_gaps.csv", index=False)
    
    # Dataset 4: Simple dataset for basic testing
    print("Creating simple dataset...")
    simple_data = {
        'id': list(range(1, n_rows + 1)),
        'timestamp': sequential_timestamps,
        'value': distributions['uniform_dist'],
        'category': categorical['category']
    }
    
    df_simple = pd.DataFrame(simple_data)
    df_simple.to_csv(f"{output_dir}/simple_test_data.csv", index=False)
    
    # Dataset 5: Data with various null patterns
    print("Creating dataset with null patterns...")
    null_pattern_data = {
        'id': sequential['id'],
        'timestamp': sequential_timestamps,
        'value1': outlier_data['data_with_nulls'],
        'value2': distributions['normal_dist'],
        'category': categorical['category']
    }
    
    df_nulls = pd.DataFrame(null_pattern_data)
    df_nulls.to_csv(f"{output_dir}/data_with_nulls.csv", index=False)
    
    # Dataset 6: High-frequency time data
    print("Creating high-frequency time dataset...")
    high_freq_timestamps = generate_sequential_timestamps(n_rows, interval_minutes=1)
    high_freq_data = {
        'timestamp': high_freq_timestamps,
        'value': distributions['poisson_dist'],
        'category': categorical['category']
    }
    
    df_high_freq = pd.DataFrame(high_freq_data)
    df_high_freq.to_csv(f"{output_dir}/high_frequency_data.csv", index=False)
    
    print(f"\n✅ Generated {len(os.listdir(output_dir))} test datasets:")
    for file in os.listdir(output_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(output_dir, file)
            size = os.path.getsize(file_path)
            print(f"  - {file} ({size:,} bytes)")
    
    print(f"\n📁 Files saved to: {output_dir}/")
    print("🎯 Ready for testing the Leaf application!")

def test_script():
    """Test the script with a small dataset."""
    print("🧪 Testing data generation script...")
    
    # Generate a small test dataset
    generate_test_data(n_rows=100, output_dir="test_data_small")
    
    # Verify files were created
    test_files = os.listdir("test_data_small")
    csv_files = [f for f in test_files if f.endswith('.csv')]
    
    if len(csv_files) == 6:
        print("✅ Test successful! All 6 datasets generated.")
        
        # Load and display sample data
        sample_df = pd.read_csv("test_data_small/comprehensive_test_data.csv")
        print(f"\n📊 Sample data shape: {sample_df.shape}")
        print(f"📋 Columns: {list(sample_df.columns)}")
        print("\n🔍 First 5 rows:")
        print(sample_df.head())
        
        return True
    else:
        print(f"❌ Test failed! Expected 6 files, got {len(csv_files)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test data for Leaf application")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate")
    parser.add_argument("--output-dir", type=str, default="test_data", help="Output directory")
    parser.add_argument("--test", action="store_true", help="Run test with small dataset")
    
    args = parser.parse_args()
    
    if args.test:
        success = test_script()
        if success:
            print("\n🎉 Script test passed! Ready for production use.")
        else:
            print("\n💥 Script test failed!")
            exit(1)
    else:
        generate_test_data(n_rows=args.rows, output_dir=args.output_dir) 