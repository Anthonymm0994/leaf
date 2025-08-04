#!/usr/bin/env python3
"""
Test Data Generator for Leaf Application - Strictly within 24 hours

This script generates synthetic CSV test data following specific requirements
for testing data inference and processing capabilities, ensuring all timestamps
stay within a single 24-hour period while generating the exact number of requested rows.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta, time
import argparse
import base64
import sys
from typing import List, Dict, Any, Tuple

# Set UTF-8 encoding for Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# Maximum time in seconds for a single day (23:59:59.999)
MAX_TIME_SECONDS = 24 * 3600 - 0.001

def calculate_time_budget(n_rows: int, n_groups: int, n_hour_gaps: int) -> Dict[str, float]:
    """
    Calculate how to budget time across the dataset to fit within 24 hours.
    
    Returns dict with:
    - avg_increment: average time increment between unique times
    - hour_gap_size: size of each hour gap
    - normal_gap_size: size of normal gaps between groups
    """
    # For larger datasets, we need to use the full 24 hours more efficiently
    # Reserve time for hour gaps - scale down for larger datasets
    if n_rows <= 10000:
        hour_gap_size = 3600  # Full hour gaps for small datasets
    elif n_rows <= 100000:
        hour_gap_size = 1800  # 30 minute gaps for medium datasets
    elif n_rows <= 1000000:
        hour_gap_size = 600   # 10 minute gaps for large datasets
    else:
        hour_gap_size = 300   # 5 minute gaps for very large datasets
    
    hour_gap_budget = n_hour_gaps * hour_gap_size
    
    # Normal gap sizes also scale with dataset size
    if n_rows <= 10000:
        normal_gap_size = 10
    elif n_rows <= 100000:
        normal_gap_size = 2
    elif n_rows <= 1000000:
        normal_gap_size = 0.5
    else:
        normal_gap_size = 0.1
    
    # Calculate normal gaps
    normal_gaps = n_groups - n_hour_gaps - 1  # -1 because no gap after last group
    normal_gap_budget = normal_gaps * normal_gap_size
    
    # Remaining time for data
    remaining_time = MAX_TIME_SECONDS - hour_gap_budget - normal_gap_budget
    
    # Ensure we don't exceed 24 hours
    if remaining_time < 0:
        # Scale everything down proportionally
        scale_factor = MAX_TIME_SECONDS / (hour_gap_budget + normal_gap_budget + MAX_TIME_SECONDS * 0.5)
        hour_gap_size *= scale_factor
        hour_gap_budget *= scale_factor
        normal_gap_size *= scale_factor
        normal_gap_budget *= scale_factor
        remaining_time = MAX_TIME_SECONDS - hour_gap_budget - normal_gap_budget
    
    # Estimate unique time points (accounting for duplicates)
    avg_duplicates = 3  # Average duplicates per time
    unique_times = n_rows / avg_duplicates
    
    # Average increment between unique times
    avg_increment = remaining_time / unique_times if unique_times > 0 else 1.0
    
    return {
        'avg_increment': avg_increment,
        'hour_gap_size': hour_gap_size,
        'normal_gap_size': normal_gap_size,
        'data_time_budget': remaining_time,
        'total_budget': MAX_TIME_SECONDS
    }

def generate_time_with_duplicates(start_seconds: float, n_rows: int, time_budget: float) -> Tuple[List[str], List[int]]:
    """
    Generate good_time values with duplicates (1-5 times each).
    Uses the allocated time budget to ensure we don't exceed it.
    """
    times = []
    group_indices = []
    current_seconds = start_seconds
    group_idx = 0
    
    # Calculate average time per row
    avg_time_per_row = time_budget / n_rows if n_rows > 0 else 1.0
    
    i = 0
    while i < n_rows:
        # Determine how many times to duplicate this time value (1-5)
        duplicates = random.randint(1, 5)
        actual_duplicates = min(duplicates, n_rows - i)
        
        # Convert seconds to time string
        hours = int(current_seconds // 3600) % 24
        minutes = int((current_seconds % 3600) // 60)
        seconds = current_seconds % 60
        milliseconds = int((seconds % 1) * 1000)
        seconds = int(seconds)
        
        time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"
        
        # Add the duplicated times
        for _ in range(actual_duplicates):
            times.append(time_str)
            group_indices.append(group_idx)
            i += 1
        
        # Calculate increment for next time
        # Use more time for fewer duplicates, less time for more duplicates
        time_used = actual_duplicates * avg_time_per_row
        increment = random.uniform(time_used * 0.5, time_used * 1.5)
        
        current_seconds += increment
        group_idx += 1
    
    return times, group_indices

def generate_dumb_time(good_time_str: str, is_first_in_group: bool) -> str:
    """
    Generate dumb_time that is 1-5 minutes after good_time.
    First row in each group gets empty value.
    """
    if is_first_in_group:
        return ""
    
    # Parse good_time
    time_parts = good_time_str.split(':')
    hours = int(time_parts[0])
    minutes = int(time_parts[1])
    seconds_ms = float(time_parts[2])
    
    # Convert to total seconds
    good_seconds = hours * 3600 + minutes * 60 + seconds_ms
    
    # Add 1-5 minutes (60-300 seconds) plus some random seconds
    offset_seconds = random.randint(60, 300) + random.uniform(0, 59.999)
    dumb_seconds = good_seconds + offset_seconds
    
    # Ensure we don't exceed 24 hours
    if dumb_seconds >= MAX_TIME_SECONDS:
        # Cap at just before midnight
        dumb_seconds = MAX_TIME_SECONDS - 0.001
    
    # Convert back to time string
    hours = int(dumb_seconds // 3600) % 24
    minutes = int((dumb_seconds % 3600) // 60)
    seconds = dumb_seconds % 60
    milliseconds = int((seconds % 1) * 1000)
    seconds = int(seconds)
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"

def generate_group(group_size: int, start_seconds: float, time_budget: float) -> Tuple[pd.DataFrame, float]:
    """
    Generate a single group of data within the time budget.
    
    Returns:
        DataFrame with the group data and the ending time in seconds
    """
    # Time columns
    good_times, time_group_indices = generate_time_with_duplicates(start_seconds, group_size, time_budget)
    
    # First row of the entire group gets empty dumb_time
    dumb_times = []
    for i in range(group_size):
        if i == 0:  # Only the very first row of the group
            dumb_times.append("")
        else:
            dumb_times.append(generate_dumb_time(good_times[i], False))
    
    # Numerical columns
    width = [f"{random.uniform(1.00, 200.00):.2f}" for _ in range(group_size)]
    height = [f"{random.uniform(0.2, 4.8):.1f}" for _ in range(group_size)]
    angle = [f"{random.uniform(0.00, 360.00):.2f}" for _ in range(group_size)]
    
    # Categorical columns
    categories = {}
    for i in range(3, 11):
        cat_values = [chr(ord('a') + j) for j in range(i)]
        categories[f'category_{i}'] = [random.choice(cat_values) for _ in range(group_size)]
    
    # Boolean columns
    booleans = {
        'isGood': [random.choice([True, False]) for _ in range(group_size)],
        'isOld': [random.choice([True, False]) for _ in range(group_size)],
        'isWhat': [random.choice([True, False]) for _ in range(group_size)],
        'isEnabled': [random.choice([True, False]) for _ in range(group_size)],
        'isFlagged': [random.choice([True, False]) for _ in range(group_size)]
    }
    
    # Inference stress test columns
    inference_cols = {}
    
    # Helper function to add random blanks or dashes
    def add_missing_values(data: List[Any], missing_type: str, missing_rate: float = 0.1) -> List[Any]:
        result = data.copy()
        n_missing = int(len(data) * missing_rate)
        if n_missing > 0:
            indices = random.sample(range(len(data)), n_missing)
            for idx in indices:
                if missing_type == 'blank':
                    result[idx] = None  # Will become empty in CSV
                else:  # dash
                    result[idx] = "-"
        return result
    
    # Integer inference columns
    int_data = [random.randint(1, 1000) for _ in range(group_size)]
    inference_cols['integer_infer_blank'] = add_missing_values(int_data, 'blank')
    inference_cols['integer_infer_dash'] = add_missing_values(int_data.copy(), 'dash')
    
    # Real inference columns
    real_data = [round(random.uniform(0.0, 100.0), 3) for _ in range(group_size)]
    inference_cols['real_infer_blank'] = add_missing_values(real_data, 'blank')
    inference_cols['real_infer_dash'] = add_missing_values(real_data.copy(), 'dash')
    
    # Text inference columns
    text_data = [f"text_{random.randint(1, 100)}" for _ in range(group_size)]
    inference_cols['text_infer_blank'] = add_missing_values(text_data, 'blank')
    inference_cols['text_infer_dash'] = add_missing_values(text_data.copy(), 'dash')
    
    # Boolean inference columns
    bool_data = [random.choice([True, False]) for _ in range(group_size)]
    inference_cols['boolean_infer_blank'] = add_missing_values(bool_data, 'blank')
    inference_cols['boolean_infer_dash'] = add_missing_values(bool_data.copy(), 'dash')
    
    # Date inference columns (YYYY-MM-DD)
    date_data = [(datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d") 
                 for _ in range(group_size)]
    inference_cols['date_infer_blank'] = add_missing_values(date_data, 'blank')
    inference_cols['date_infer_dash'] = add_missing_values(date_data.copy(), 'dash')
    
    # DateTime inference columns (YYYY-MM-DD HH:MM:SS)
    datetime_data = [(datetime(2024, 1, 1) + timedelta(
        days=random.randint(0, 365),
        seconds=random.randint(0, 86399)
    )).strftime("%Y-%m-%d %H:%M:%S") for _ in range(group_size)]
    inference_cols['datetime_infer_blank'] = add_missing_values(datetime_data, 'blank')
    inference_cols['datetime_infer_dash'] = add_missing_values(datetime_data.copy(), 'dash')
    
    # Time columns with different precisions
    # TimeSeconds (HH:MM:SS)
    time_sec_data = [f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}" 
                     for _ in range(group_size)]
    inference_cols['timeseconds_infer_blank'] = add_missing_values(time_sec_data, 'blank')
    inference_cols['timeseconds_infer_dash'] = add_missing_values(time_sec_data.copy(), 'dash')
    
    # TimeMilliseconds (HH:MM:SS.sss)
    time_ms_data = [f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}.{random.randint(0,999):03d}" 
                    for _ in range(group_size)]
    inference_cols['timemilliseconds_infer_blank'] = add_missing_values(time_ms_data, 'blank')
    inference_cols['timemilliseconds_infer_dash'] = add_missing_values(time_ms_data.copy(), 'dash')
    
    # TimeMicroseconds (HH:MM:SS.ssssss)
    time_us_data = [f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}.{random.randint(0,999999):06d}" 
                    for _ in range(group_size)]
    inference_cols['timemicroseconds_infer_blank'] = add_missing_values(time_us_data, 'blank')
    inference_cols['timemicroseconds_infer_dash'] = add_missing_values(time_us_data.copy(), 'dash')
    
    # TimeNanoseconds (HH:MM:SS.sssssssss)
    time_ns_data = [f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}.{random.randint(0,999999999):09d}" 
                    for _ in range(group_size)]
    inference_cols['timenanoseconds_infer_blank'] = add_missing_values(time_ns_data, 'blank')
    inference_cols['timenanoseconds_infer_dash'] = add_missing_values(time_ns_data.copy(), 'dash')
    
    # Blob inference columns (base64 encoded data)
    blob_data = [base64.b64encode(f"blob_data_{i}".encode()).decode() for i in range(group_size)]
    inference_cols['blob_infer_blank'] = add_missing_values(blob_data, 'blank')
    inference_cols['blob_infer_dash'] = add_missing_values(blob_data.copy(), 'dash')
    
    # Multi-value column (tags)
    tag_options = ["", "a", "a,b", "a,b,c"]
    tags = [random.choice(tag_options) for _ in range(group_size)]
    
    # Distribution columns
    # Bimodal distribution
    bimodal = []
    for _ in range(group_size):
        if random.random() < 0.5:
            bimodal.append(np.random.normal(30, 5))
        else:
            bimodal.append(np.random.normal(70, 5))
    
    # Linear over time (increases with position in dataset)
    linear_over_time = [10 + (i / group_size) * 80 + np.random.normal(0, 2) for i in range(group_size)]
    
    # Exponential distribution
    exponential = np.random.exponential(20, group_size).tolist()
    
    # Uniform distribution
    uniform = np.random.uniform(0, 100, group_size).tolist()
    
    # Normal distribution
    normal = np.random.normal(50, 15, group_size).tolist()
    
    # Combine all data
    data = {
        'good_time': good_times,
        'dumb_time': dumb_times,
        'width': width,
        'height': height,
        'angle': angle,
        **categories,
        **booleans,
        **inference_cols,
        'tags': tags,
        'bimodal': bimodal,
        'linear_over_time': linear_over_time,
        'exponential': exponential,
        'uniform': uniform,
        'normal': normal
    }
    
    # Calculate ending time
    if good_times:
        last_time = good_times[-1]
        time_parts = last_time.split(':')
        hours = int(time_parts[0])
        minutes = int(time_parts[1])
        seconds_ms = float(time_parts[2])
        end_seconds = hours * 3600 + minutes * 60 + seconds_ms
    else:
        end_seconds = start_seconds
    
    return pd.DataFrame(data), end_seconds

def generate_test_data(n_rows: int = 10000) -> pd.DataFrame:
    """Generate the complete test dataset staying within 24 hours."""
    
    # Pre-plan all groups to handle hour gaps properly
    group_plans = []
    current_rows = 0
    
    # Step 1: Plan all groups and their sizes
    while current_rows < n_rows:
        group_size = random.randint(200, 500)
        if current_rows + group_size > n_rows:
            group_size = n_rows - current_rows
        
        # Determine duplication
        rand = random.random()
        if rand < 0.80:  # 80% unique
            group_plans.append({
                'size': group_size,
                'duplicates': 0,
                'rows': group_size
            })
            current_rows += group_size
        elif rand < 0.95:  # 15% duplicated once
            total_rows = group_size * 2
            if current_rows + total_rows <= n_rows:
                group_plans.append({
                    'size': group_size,
                    'duplicates': 1,
                    'rows': total_rows
                })
                current_rows += total_rows
            else:
                # Not enough room for duplicate
                group_plans.append({
                    'size': group_size,
                    'duplicates': 0,
                    'rows': group_size
                })
                current_rows += group_size
        else:  # 5% duplicated twice
            total_rows = group_size * 3
            if current_rows + total_rows <= n_rows:
                group_plans.append({
                    'size': group_size,
                    'duplicates': 2,
                    'rows': total_rows
                })
                current_rows += total_rows
            else:
                # Not enough room for all duplicates
                group_plans.append({
                    'size': group_size,
                    'duplicates': 0,
                    'rows': group_size
                })
                current_rows += group_size
    
    # Step 2: Determine which groups should have hour gaps after them
    # Select ~10% of groups for hour gaps
    num_groups = len(group_plans)
    num_gaps = max(1, int(num_groups * 0.1))
    
    # Randomly select groups to have gaps after them (not the last group)
    gap_indices = set()
    if num_groups > 1:
        possible_indices = list(range(num_groups - 1))  # Don't put gap after last group
        gap_indices = set(random.sample(possible_indices, min(num_gaps, len(possible_indices))))
    
    # Step 3: Calculate time budget
    n_hour_gaps = len(gap_indices)
    time_budget = calculate_time_budget(n_rows, num_groups, n_hour_gaps)
    
    print(f"  Time budget: {time_budget['data_time_budget']/3600:.1f} hours for data, "
          f"{n_hour_gaps} hour gaps of {time_budget['hour_gap_size']/3600:.2f} hours each")
    
    # Calculate time budget per group
    total_group_count = num_groups + sum(plan['duplicates'] for plan in group_plans)
    time_per_group = time_budget['data_time_budget'] / total_group_count
    
    # Step 4: Generate groups with planned gaps
    all_groups = []
    current_seconds = 0.0  # Start at midnight
    
    print(f"  Generating {len(group_plans)} groups (with duplicates: {total_group_count} total)...")
    
    for group_idx, plan in enumerate(group_plans):
        # Show progress for large datasets
        if n_rows > 10000:
            if group_idx % 50 == 0:
                print(f"  Progress: {group_idx}/{len(group_plans)} groups generated...")
            elif n_rows > 1000000 and group_idx % 500 == 0:
                # More frequent updates for very large datasets
                rows_so_far = sum(len(g) for g in all_groups)
                print(f"  Progress: {group_idx}/{len(group_plans)} groups, {rows_so_far:,}/{n_rows:,} rows...")
        
        # Generate base group
        group_time_budget = time_per_group
        group_df, end_seconds = generate_group(plan['size'], current_seconds, group_time_budget)
        all_groups.append(group_df)
        current_seconds = end_seconds
        
        # Generate duplicates if needed
        for dup_num in range(plan['duplicates']):
            # Small gap before duplicate
            current_seconds += time_budget['normal_gap_size']
            
            dup_df, end_seconds = generate_group(plan['size'], current_seconds, time_per_group)
            all_groups.append(dup_df)
            current_seconds = end_seconds
        
        # Add gap after this group (either hour gap or normal gap)
        if group_idx < len(group_plans) - 1:  # Don't add gap after last group
            if group_idx in gap_indices:
                # Add hour gap
                current_seconds += time_budget['hour_gap_size']
                print(f"  Added {time_budget['hour_gap_size']/3600:.2f}-hour gap after group {group_idx + 1}")
            else:
                # Normal gap between groups
                current_seconds += time_budget['normal_gap_size']
    
    # Combine all groups
    final_df = pd.concat(all_groups, ignore_index=True)
    
    # Trim to exact row count (shouldn't be necessary with proper planning)
    if len(final_df) > n_rows:
        final_df = final_df.iloc[:n_rows]
    elif len(final_df) < n_rows:
        print(f"⚠️  Warning: Generated {len(final_df)} rows instead of {n_rows}")
    
    # Report final time span
    if len(final_df) > 0:
        first_time = final_df['good_time'].iloc[0]
        last_time = final_df['good_time'].iloc[-1]
        print(f"✅ Final time span: {first_time} to {last_time}")
    
    return final_df

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic test data CSV (strictly within 24 hours)')
    parser.add_argument('--rows', type=int, default=10000, 
                       help='Number of rows to generate (default: 10000)')
    parser.add_argument('--output', type=str, default='test_data.csv',
                       help='Output filename (default: test_data.csv)')
    args = parser.parse_args()
    
    print(f"Generating {args.rows} rows of test data (strictly within 24 hours)...")
    
    # Generate the data
    df = generate_test_data(args.rows)
    
    # Save to CSV
    df.to_csv(args.output, index=False)
    
    print(f"✅ Generated {len(df)} rows")
    print(f"✅ Saved to {args.output}")
    print(f"📊 Columns: {len(df.columns)}")
    print(f"📏 File size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB (in memory)")

if __name__ == "__main__":
    main()