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

def generate_block_values():
    """Generate values for a block that will be repeated across multiple rows."""
    values = {
        'width': f"{random.uniform(1.00, 200.00):.2f}",
        'height': f"{random.uniform(0.2, 4.8):.1f}",
        'angle': f"{random.uniform(0.00, 360.00):.2f}",
    }
    
    # Categorical columns
    for i in range(3, 11):
        cat_values = [chr(ord('a') + j) for j in range(i)]
        values[f'category_{i}'] = random.choice(cat_values)
    
    # Boolean columns
    for col in ['isGood', 'isOld', 'isWhat', 'isEnabled', 'isFlagged']:
        values[col] = random.choice([True, False])
    
    # Inference columns
    values['integer_infer_blank'] = random.randint(1, 1000) if random.random() > 0.1 else None
    values['integer_infer_dash'] = random.randint(1, 1000) if random.random() > 0.1 else "-"
    values['real_infer_blank'] = round(random.uniform(0.0, 100.0), 3) if random.random() > 0.1 else None
    values['real_infer_dash'] = round(random.uniform(0.0, 100.0), 3) if random.random() > 0.1 else "-"
    values['text_infer_blank'] = f"text_{random.randint(1, 100)}" if random.random() > 0.1 else None
    values['text_infer_dash'] = f"text_{random.randint(1, 100)}" if random.random() > 0.1 else "-"
    values['boolean_infer_blank'] = random.choice([True, False]) if random.random() > 0.1 else None
    values['boolean_infer_dash'] = random.choice([True, False]) if random.random() > 0.1 else "-"
    
    # Date columns
    date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
    values['date_infer_blank'] = date.strftime("%Y-%m-%d") if random.random() > 0.1 else None
    values['date_infer_dash'] = date.strftime("%Y-%m-%d") if random.random() > 0.1 else "-"
    
    # DateTime columns
    dt = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365), seconds=random.randint(0, 86399))
    values['datetime_infer_blank'] = dt.strftime("%Y-%m-%d %H:%M:%S") if random.random() > 0.1 else None
    values['datetime_infer_dash'] = dt.strftime("%Y-%m-%d %H:%M:%S") if random.random() > 0.1 else "-"
    
    # Time columns
    values['timeseconds_infer_blank'] = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}" if random.random() > 0.1 else None
    values['timeseconds_infer_dash'] = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}" if random.random() > 0.1 else "-"
    values['timemilliseconds_infer_blank'] = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}.{random.randint(0,999):03d}" if random.random() > 0.1 else None
    values['timemilliseconds_infer_dash'] = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}.{random.randint(0,999):03d}" if random.random() > 0.1 else "-"
    
    # Blob column
    values['blob_infer_blank'] = f"blob_{random.randint(1, 1000)}" if random.random() > 0.1 else None
    values['blob_infer_dash'] = f"blob_{random.randint(1, 1000)}" if random.random() > 0.1 else "-"
    
    # Tags
    values['tags'] = f"tag{random.randint(0, 9)}"
    
    # Distribution columns
    values['bimodal'] = round(random.choice([random.gauss(30, 5), random.gauss(70, 5)]), 2)
    values['exponential'] = round(random.expovariate(1/50), 2)
    values['uniform'] = round(random.uniform(0, 100), 2)
    values['normal'] = round(random.gauss(50, 15), 2)
    
    return values

def generate_group(group_size: int, start_time: datetime, max_time: datetime = None) -> pd.DataFrame:
    """Generate a single group of data with blocks of identical values."""
    # Time columns
    good_times, time_group_indices = generate_time_with_duplicates(start_time, group_size, max_time)
    
    # First row of the entire group gets empty dumb_time
    dumb_times = []
    for i in range(group_size):
        if i == 0:  # Only the very first row of the group
            dumb_times.append("")
        else:
            dumb_times.append(generate_dumb_time(good_times[i], False))
    
    # Generate data in blocks
    rows_data = []
    i = 0
    while i < group_size:
        # Determine block size (5-20 rows)
        block_size = min(random.randint(5, 20), group_size - i)
        
        # Generate values for this block
        block_values = generate_block_values()
        
        # Apply these values to all rows in the block
        for j in range(block_size):
            rows_data.append(block_values)
        
        i += block_size
    
    # Create DataFrame from block data
    data = {
        'good_time': good_times,
        'dumb_time': dumb_times,
    }
    
    # Add all the block data columns
    for key in rows_data[0].keys():
        data[key] = [row[key] for row in rows_data]
    
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
        if rand < 0.85:  # 85% unique
            group_plans.append({
                'size': group_size,
                'duplicates': 0,
                'rows': group_size
            })
            current_rows += group_size
        elif rand < 0.95:  # 10% duplicated once
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