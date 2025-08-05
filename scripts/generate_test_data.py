#!/usr/bin/env python3
"""
Generate test data with the correct duplication pattern:
- Mini groups: rows with same good_time but different data values
- Major groups: sequences ending when dumb_time is null, these can be duplicated
"""

import pandas as pd
import numpy as np
import random
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

def generate_dumb_time(good_time: str, is_first_row: bool) -> str:
    """Generate dumb_time based on good_time with random offset."""
    if is_first_row:
        return None  # None/NaN for first row of major group
    
    # Parse good_time (HH:MM:SS.sss format)
    time_parts = good_time.split(':')
    hours = int(time_parts[0])
    minutes = int(time_parts[1])
    seconds_ms = float(time_parts[2])
    
    # Add random offset between 0.1 and 10 seconds (always after good_time)
    total_seconds = hours * 3600 + minutes * 60 + seconds_ms
    offset = random.uniform(0.1, 10.0)
    new_total_seconds = total_seconds + offset
    
    # Handle wraparound for 24-hour format
    if new_total_seconds < 0:
        new_total_seconds += 86400
    elif new_total_seconds >= 86400:
        new_total_seconds -= 86400
    
    # Format back to HH:MM:SS.sss
    new_hours = int(new_total_seconds // 3600) % 24
    new_minutes = int((new_total_seconds % 3600) // 60)
    new_seconds = new_total_seconds % 60
    
    return f"{new_hours:02d}:{new_minutes:02d}:{new_seconds:06.3f}"

def generate_time_sequence(start_time: datetime, n_rows: int) -> List[str]:
    """Generate good_time sequence with 1-5 duplicates."""
    times = []
    current_time = start_time
    
    i = 0
    while i < n_rows:
        # Decide how many rows will share this timestamp (1-5)
        duplicate_count = min(random.randint(1, 5), n_rows - i)
        
        # Format time as HH:MM:SS.sss
        time_str = current_time.strftime("%H:%M:%S.%f")[:-3]
        
        # Add this time for duplicate_count rows
        for _ in range(duplicate_count):
            times.append(time_str)
            i += 1
        
        # Advance time by 1 second for next mini group
        current_time += timedelta(seconds=1)
    
    return times

def generate_row_data() -> Dict[str, Any]:
    """Generate random data for a single row."""
    data = {
        'width': f"{random.uniform(1.00, 200.00):.2f}",
        'height': f"{random.uniform(0.2, 4.8):.1f}",
        'angle': f"{random.uniform(0.00, 360.00):.2f}",
    }
    
    # Categorical columns
    for i in range(3, 11):
        cat_values = [chr(ord('a') + j) for j in range(i)]
        data[f'category_{i}'] = random.choice(cat_values)
    
    # Boolean columns
    for col in ['isGood', 'isOld', 'isWhat', 'isEnabled', 'isFlagged']:
        data[col] = random.choice([True, False])
    
    # Inference columns with missing values
    data['integer_infer_blank'] = random.randint(1, 1000) if random.random() > 0.1 else None
    data['integer_infer_dash'] = random.randint(1, 1000) if random.random() > 0.1 else "-"
    data['real_infer_blank'] = round(random.uniform(0.0, 100.0), 3) if random.random() > 0.1 else None
    data['real_infer_dash'] = round(random.uniform(0.0, 100.0), 3) if random.random() > 0.1 else "-"
    data['text_infer_blank'] = f"text_{random.randint(1, 100)}" if random.random() > 0.1 else None
    data['text_infer_dash'] = f"text_{random.randint(1, 100)}" if random.random() > 0.1 else "-"
    data['boolean_infer_blank'] = random.choice([True, False]) if random.random() > 0.1 else None
    data['boolean_infer_dash'] = random.choice([True, False]) if random.random() > 0.1 else "-"
    
    # Date columns
    date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
    data['date_infer_blank'] = date.strftime("%Y-%m-%d") if random.random() > 0.1 else None
    data['date_infer_dash'] = date.strftime("%Y-%m-%d") if random.random() > 0.1 else "-"
    
    # DateTime columns
    dt = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365), seconds=random.randint(0, 86399))
    data['datetime_infer_blank'] = dt.strftime("%Y-%m-%d %H:%M:%S") if random.random() > 0.1 else None
    data['datetime_infer_dash'] = dt.strftime("%Y-%m-%d %H:%M:%S") if random.random() > 0.1 else "-"
    
    # Time columns
    data['timeseconds_infer_blank'] = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}" if random.random() > 0.1 else None
    data['timeseconds_infer_dash'] = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}" if random.random() > 0.1 else "-"
    data['timemilliseconds_infer_blank'] = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}.{random.randint(0,999):03d}" if random.random() > 0.1 else None
    data['timemilliseconds_infer_dash'] = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}.{random.randint(0,999):03d}" if random.random() > 0.1 else "-"
    
    # Blob column
    data['blob_infer_blank'] = f"blob_{random.randint(1, 1000)}" if random.random() > 0.1 else None
    data['blob_infer_dash'] = f"blob_{random.randint(1, 1000)}" if random.random() > 0.1 else "-"
    
    # Tags
    data['tags'] = f"tag{random.randint(0, 9)}"
    
    # Distribution columns
    data['bimodal'] = round(random.choice([random.gauss(30, 5), random.gauss(70, 5)]), 2)
    data['exponential'] = round(random.expovariate(1/50), 2)
    data['uniform'] = round(random.uniform(0, 100), 2)
    data['normal'] = round(random.gauss(50, 15), 2)
    
    return data

def generate_major_group(group_size: int, start_time: datetime) -> pd.DataFrame:
    """Generate a major group with unique data for each row."""
    # Generate time sequence
    good_times = generate_time_sequence(start_time, group_size)
    
    # Generate dumb times
    dumb_times = []
    for i in range(group_size):
        dumb_times.append(generate_dumb_time(good_times[i], i == 0))
    
    # Generate unique data for each row
    rows = []
    for i in range(group_size):
        row_data = generate_row_data()
        row_data['good_time'] = good_times[i]
        row_data['dumb_time'] = dumb_times[i]
        rows.append(row_data)
    
    return pd.DataFrame(rows)

def generate_test_data(n_rows: int = 10000) -> pd.DataFrame:
    """Generate test data with proper major group duplication."""
    # Plan major groups
    group_plans = []
    current_rows = 0
    
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
                'total_rows': group_size
            })
            current_rows += group_size
        elif rand < 0.95:  # 10% duplicated once
            total_rows = group_size * 2
            if current_rows + total_rows <= n_rows:
                group_plans.append({
                    'size': group_size,
                    'duplicates': 1,
                    'total_rows': total_rows
                })
                current_rows += total_rows
            else:
                # Not enough room
                group_plans.append({
                    'size': group_size,
                    'duplicates': 0,
                    'total_rows': group_size
                })
                current_rows += group_size
        else:  # 5% duplicated twice (triplicated)
            total_rows = group_size * 3
            if current_rows + total_rows <= n_rows:
                group_plans.append({
                    'size': group_size,
                    'duplicates': 2,
                    'total_rows': total_rows
                })
                current_rows += total_rows
            else:
                # Not enough room
                group_plans.append({
                    'size': group_size,
                    'duplicates': 0,
                    'total_rows': group_size
                })
                current_rows += group_size
    
    # Generate groups
    all_groups = []
    current_time = datetime(2024, 1, 1, 0, 0, 0)
    
    print(f"Generating {len(group_plans)} major groups...")
    
    for idx, plan in enumerate(group_plans):
        if n_rows > 10000 and idx % 50 == 0:
            print(f"  Progress: {idx}/{len(group_plans)} groups")
        
        # Generate base group
        base_group = generate_major_group(plan['size'], current_time)
        all_groups.append(base_group)
        
        # Update time for next group
        last_time_str = base_group['good_time'].iloc[-1]
        time_parts = last_time_str.split(':')
        hours = int(time_parts[0])
        minutes = int(time_parts[1])
        seconds = float(time_parts[2])
        
        # Keep track of days
        days_elapsed = (current_time - datetime(2024, 1, 1)).days
        current_time = datetime(2024, 1, 1, hours, minutes, int(seconds)) + timedelta(days=days_elapsed)
        
        # Generate duplicates if needed
        for dup_num in range(plan['duplicates']):
            # Add gap before duplicate
            current_time += timedelta(seconds=random.uniform(60, 300))
            
            # Generate duplicate with new times but same data sequence
            dup_group = base_group.copy()
            
            # Update times in duplicate
            new_good_times = generate_time_sequence(current_time, len(dup_group))
            dup_group['good_time'] = new_good_times
            
            # Regenerate dumb_times
            new_dumb_times = []
            for i in range(len(dup_group)):
                new_dumb_times.append(generate_dumb_time(new_good_times[i], i == 0))
            dup_group['dumb_time'] = new_dumb_times
            
            all_groups.append(dup_group)
            
            # Update current time
            last_time_str = new_good_times[-1]
            time_parts = last_time_str.split(':')
            current_time = datetime(2024, 1, 1, int(time_parts[0]), int(time_parts[1]), 
                                  int(float(time_parts[2]))) + timedelta(days=days_elapsed)
        
        # Add gap after group (with occasional 1-hour gaps)
        if random.random() < 0.1:  # 10% chance of hour gap
            current_time += timedelta(hours=1)
            print(f"  Added 1-hour gap after group {idx + 1}")
        else:
            current_time += timedelta(seconds=random.uniform(1, 60))
    
    # Combine all groups
    final_df = pd.concat(all_groups, ignore_index=True)
    
    # Trim to exact size
    if len(final_df) > n_rows:
        final_df = final_df.iloc[:n_rows]
    
    # Report statistics
    print(f"\n✅ Generated {len(final_df)} rows")
    
    # Count actual duplicates
    duplicate_count = 0
    triplicate_count = 0
    for plan in group_plans:
        if plan['duplicates'] == 1:
            duplicate_count += 1
        elif plan['duplicates'] == 2:
            triplicate_count += 1
    
    total_groups = len(group_plans)
    print(f"📊 Major groups: {total_groups}")
    print(f"   - Unique: {total_groups - duplicate_count - triplicate_count}")
    print(f"   - Duplicated (2x): {duplicate_count} ({duplicate_count/total_groups*100:.1f}%)")
    print(f"   - Triplicated (3x): {triplicate_count} ({triplicate_count/total_groups*100:.1f}%)")
    
    return final_df

def main():
    parser = argparse.ArgumentParser(description='Generate test data with correct duplication pattern')
    parser.add_argument('--rows', type=int, default=10000, 
                       help='Number of rows to generate (default: 10000)')
    parser.add_argument('--output', type=str, default='test_data.csv',
                       help='Output filename (default: test_data.csv)')
    args = parser.parse_args()
    
    print(f"Generating {args.rows} rows of test data...")
    
    # Generate the data
    df = generate_test_data(args.rows)
    
    # Save to CSV
    df.to_csv(args.output, index=False)
    print(f"✅ Saved to {args.output}")
    print(f"📊 Columns: {len(df.columns)}")
    print(f"📏 File size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB (in memory)")

if __name__ == "__main__":
    main()