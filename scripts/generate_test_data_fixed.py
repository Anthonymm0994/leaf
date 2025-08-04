#!/usr/bin/env python3
"""
Generate test data with proper duplicate blocks.

Within each group:
- Rows are divided into blocks (e.g., 5-10 consecutive rows)
- Within a block, all non-time values are identical
- 10% of blocks are duplicated (appear twice)
- 5% of blocks are triplicated (appear three times)
- Time columns (good_time, dumb_time) always progress
"""

import pandas as pd
import numpy as np
import random
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

def generate_dumb_time(good_time: str, is_first_row: bool) -> str:
    """Generate dumb_time with random offset from good_time."""
    if is_first_row:
        return ""
    
    # Parse good_time
    time_parts = good_time.split(':')
    hours = int(time_parts[0])
    minutes = int(time_parts[1])
    seconds_ms = float(time_parts[2])
    
    # Add random offset (-5 to +5 seconds)
    total_seconds = hours * 3600 + minutes * 60 + seconds_ms
    offset = random.uniform(-5, 5)
    new_total_seconds = total_seconds + offset
    
    # Handle wraparound
    if new_total_seconds < 0:
        new_total_seconds += 86400
    elif new_total_seconds >= 86400:
        new_total_seconds -= 86400
    
    # Convert back to HH:MM:SS.sss
    new_hours = int(new_total_seconds // 3600) % 24
    new_minutes = int((new_total_seconds % 3600) // 60)
    new_seconds = new_total_seconds % 60
    
    return f"{new_hours:02d}:{new_minutes:02d}:{new_seconds:06.3f}"

def generate_block_values() -> Dict[str, Any]:
    """Generate a set of values for a block (all rows in block will have these values)."""
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
    
    # Other columns
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

def generate_block(block_size: int, start_time: datetime, block_values: Dict[str, Any], is_first_block: bool = False) -> pd.DataFrame:
    """Generate a block of rows with identical values (except time)."""
    rows = []
    current_time = start_time
    
    for i in range(block_size):
        row = block_values.copy()
        
        # Add time columns
        row['good_time'] = current_time.strftime("%H:%M:%S.%f")[:-3]  # HH:MM:SS.sss
        row['dumb_time'] = generate_dumb_time(row['good_time'], is_first_block and i == 0)
        
        rows.append(row)
        
        # Increment time (1-5 duplicates within block for time values)
        if i == 0 or random.random() > 0.8:  # 20% chance to change time
            current_time += timedelta(seconds=1)
    
    return pd.DataFrame(rows), current_time

def generate_test_data(n_rows: int) -> pd.DataFrame:
    """Generate test data with duplicate blocks."""
    all_data = []
    current_time = datetime(2024, 1, 1, 0, 0, 0)
    current_rows = 0
    
    print(f"Generating {n_rows} rows with duplicate blocks...")
    
    while current_rows < n_rows:
        # Determine block size (5-20 rows)
        block_size = random.randint(5, 20)
        if current_rows + block_size > n_rows:
            block_size = n_rows - current_rows
        
        # Generate block values
        block_values = generate_block_values()
        
        # Generate the block
        block_df, new_time = generate_block(block_size, current_time, block_values, len(all_data) == 0)
        all_data.append(block_df)
        current_rows += len(block_df)
        current_time = new_time
        
        # Determine if this block should be duplicated
        rand = random.random()
        if rand < 0.10 and current_rows + block_size <= n_rows:  # 10% duplicate
            # Add gap before duplicate
            current_time += timedelta(seconds=random.uniform(60, 300))
            
            # Generate duplicate block (same values, different times)
            dup_df, new_time = generate_block(block_size, current_time, block_values)
            all_data.append(dup_df)
            current_rows += len(dup_df)
            current_time = new_time
            
        elif rand < 0.15 and current_rows + block_size * 2 <= n_rows:  # 5% triplicate
            # Add first duplicate
            current_time += timedelta(seconds=random.uniform(60, 300))
            dup1_df, new_time = generate_block(block_size, current_time, block_values)
            all_data.append(dup1_df)
            current_rows += len(dup1_df)
            current_time = new_time
            
            # Add second duplicate
            current_time += timedelta(seconds=random.uniform(60, 300))
            dup2_df, new_time = generate_block(block_size, current_time, block_values)
            all_data.append(dup2_df)
            current_rows += len(dup2_df)
            current_time = new_time
        
        # Add gap between different blocks
        current_time += timedelta(seconds=random.uniform(5, 60))
        
        # Progress indicator
        if current_rows % 10000 == 0 and current_rows > 0:
            print(f"  Generated {current_rows:,} rows...")
    
    # Combine all data
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Trim to exact size
    if len(final_df) > n_rows:
        final_df = final_df.iloc[:n_rows]
    
    print(f"Generated {len(final_df):,} total rows")
    
    # Count duplicates
    non_time_cols = [col for col in final_df.columns if 'time' not in col.lower()]
    
    # Find consecutive duplicate blocks
    duplicate_blocks = 0
    triplicate_blocks = 0
    i = 0
    
    while i < len(final_df) - 1:
        # Find end of current block
        j = i + 1
        while j < len(final_df) and all(final_df.iloc[j][col] == final_df.iloc[i][col] for col in non_time_cols):
            j += 1
        
        block_size = j - i
        
        # Look for duplicates of this block
        k = j
        duplicates_found = 0
        
        while k < len(final_df):
            # Check if we have a matching block starting at k
            if k + block_size <= len(final_df):
                is_duplicate = True
                for offset in range(block_size):
                    if not all(final_df.iloc[k + offset][col] == final_df.iloc[i + offset][col] for col in non_time_cols):
                        is_duplicate = False
                        break
                
                if is_duplicate:
                    duplicates_found += 1
                    k += block_size
                else:
                    k += 1
            else:
                break
        
        if duplicates_found == 1:
            duplicate_blocks += 1
        elif duplicates_found == 2:
            triplicate_blocks += 1
        
        i = j
    
    print(f"  Blocks duplicated once: {duplicate_blocks}")
    print(f"  Blocks triplicated: {triplicate_blocks}")
    
    return final_df

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic test data CSV with duplicate blocks')
    parser.add_argument('--rows', type=int, default=10000, 
                       help='Number of rows to generate (default: 10000)')
    parser.add_argument('--output', type=str, default='test_data.csv',
                       help='Output filename (default: test_data.csv)')
    args = parser.parse_args()
    
    # Generate the data
    df = generate_test_data(args.rows)
    
    # Save to CSV
    df.to_csv(args.output, index=False)
    print(f"\nSaved to {args.output}")

if __name__ == "__main__":
    main()