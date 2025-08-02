#!/usr/bin/env python3
"""
Test Data Generator for Leaf Application

This script generates synthetic CSV test data following specific requirements
for testing data inference and processing capabilities.
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

def generate_time_with_duplicates(start_time: datetime, n_rows: int, max_time: datetime = None) -> Tuple[List[str], List[int]]:
    """
    Generate good_time values with duplicates (1-5 times each).
    Returns both the times and group indices.
    """
    times = []
    group_indices = []
    current_time = start_time
    group_idx = 0
    
    i = 0
    while i < n_rows:
        # Determine how many times to duplicate this time value (1-5)
        duplicates = random.randint(1, 5)
        
        # Format time as HH:MM:SS.sss (only time part, allowing wraparound)
        time_only = current_time.time()
        time_str = time_only.strftime("%H:%M:%S.%f")[:-3]  # Milliseconds only
        
        # Add the duplicated times
        for _ in range(min(duplicates, n_rows - i)):
            times.append(time_str)
            group_indices.append(group_idx)
            i += 1
        
        # Advance time for next value (random increment)
        increment = random.uniform(0.001, 60)  # 1ms to 60s
        current_time += timedelta(seconds=increment)
        group_idx += 1
    
    return times[:n_rows], group_indices[:n_rows]

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
    seconds = int(seconds_ms)
    milliseconds = int((seconds_ms - seconds) * 1000)
    
    # Create datetime for easier manipulation
    base_date = datetime(2024, 1, 1)
    good_datetime = datetime(2024, 1, 1, hours, minutes, seconds, milliseconds * 1000)
    
    # Add 1-5 minutes (60-300 seconds) plus some random seconds
    offset_seconds = random.randint(60, 300) + random.uniform(0, 59.999)
    dumb_datetime = good_datetime + timedelta(seconds=offset_seconds)
    
    # Handle day overflow - wrap around to stay within 24h
    if dumb_datetime.date() > base_date.date():
        # Calculate how much we've gone over midnight
        overflow_seconds = (dumb_datetime - datetime(2024, 1, 2, 0, 0, 0)).total_seconds()
        # Create new time starting from 00:00:00 plus the overflow
        dumb_datetime = datetime(2024, 1, 1, 0, 0, 0) + timedelta(seconds=overflow_seconds)
    
    return dumb_datetime.strftime("%H:%M:%S.%f")[:-3]

def generate_group(group_size: int, start_time: datetime, max_time: datetime = None) -> pd.DataFrame:
    """Generate a single group of data."""
    # Time columns
    good_times, time_group_indices = generate_time_with_duplicates(start_time, group_size, max_time)
    
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
    
    return pd.DataFrame(data)

def generate_test_data(n_rows: int = 10000) -> pd.DataFrame:
    """Generate the complete test dataset."""
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
    
    # Step 3: Generate groups with planned gaps
    all_groups = []
    current_time = datetime(2024, 1, 1, 0, 0, 0, 0)
    
    print(f"  Generating {len(group_plans)} groups...")
    
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
        group_df = generate_group(plan['size'], current_time, None)
        all_groups.append(group_df)
        
        # Update current time
        last_good_time = group_df['good_time'].iloc[-1]
        time_parts = last_good_time.split(':')
        hours = int(time_parts[0])
        minutes = int(time_parts[1])
        seconds_ms = float(time_parts[2])
        
        # Keep track of days elapsed based on current_time
        days_elapsed = (current_time - datetime(2024, 1, 1, 0, 0, 0)).days
        current_time = datetime(2024, 1, 1, hours, minutes, int(seconds_ms), 
                               int((seconds_ms - int(seconds_ms)) * 1000000)) + timedelta(days=days_elapsed)
        
        # Generate duplicates if needed
        for dup_num in range(plan['duplicates']):
            current_time += timedelta(seconds=random.uniform(60, 300))
                
            dup_df = group_df.copy()
            
            # Update times in duplicate
            new_good_times, _ = generate_time_with_duplicates(current_time, len(dup_df), None)
            dup_df['good_time'] = new_good_times
            
            # Regenerate dumb_times
            dup_df['dumb_time'] = [generate_dumb_time(gt, i == 0) 
                                   for i, gt in enumerate(new_good_times)]
            
            all_groups.append(dup_df)
            
            # Update current time
            if new_good_times:
                last_time = new_good_times[-1]
                time_parts = last_time.split(':')
                # Keep track of days elapsed
                days_elapsed = (current_time - datetime(2024, 1, 1, 0, 0, 0)).days
                current_time = datetime(2024, 1, 1, int(time_parts[0]), int(time_parts[1]), 
                                      int(float(time_parts[2]))) + timedelta(days=days_elapsed)
        
        # Add gap after this group (either hour gap or normal gap)
        if group_idx in gap_indices:
            # Add exactly 1 hour gap
            current_time += timedelta(hours=1)
            print(f"  Added 1-hour gap after group {group_idx + 1} of {len(group_plans)}")
        else:
            # Normal gap between groups (1-60 seconds)
            current_time += timedelta(seconds=random.uniform(1, 60))
    
    # Combine all groups
    final_df = pd.concat(all_groups, ignore_index=True)
    
    # Trim to exact row count
    if len(final_df) > n_rows:
        final_df = final_df.iloc[:n_rows]
    
    return final_df

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic test data CSV')
    parser.add_argument('--rows', type=int, default=10000, 
                       help='Number of rows to generate (default: 10000)')
    args = parser.parse_args()
    
    print(f"Generating {args.rows} rows of test data...")
    
    # Generate the data
    df = generate_test_data(args.rows)
    
    # Save to CSV
    output_file = 'test_data.csv'
    df.to_csv(output_file, index=False)
    
    print(f"✅ Generated {len(df)} rows")
    print(f"✅ Saved to {output_file}")
    print(f"📊 Columns: {len(df.columns)}")
    print(f"📏 File size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB (in memory)")

if __name__ == "__main__":
    main()