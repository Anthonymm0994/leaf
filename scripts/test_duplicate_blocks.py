#!/usr/bin/env python3
"""
Test script to verify duplicate block detection functionality.
Creates a small test dataset with known duplicate blocks.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def create_test_data_with_duplicates():
    """Create a test dataset with known duplicate blocks."""
    
    data = []
    current_time = datetime(2024, 1, 1, 0, 0, 0)
    group_id = 1
    
    # Create 10 groups with some duplicates
    for i in range(10):
        # Create a base block of 5 rows
        base_block = []
        for j in range(5):
            row = {
                'group_id': group_id,
                'good_time': current_time.strftime("%H:%M:%S.%f")[:-3],  # HH:MM:SS.sss
                'dumb_time': (current_time + timedelta(seconds=np.random.uniform(-5, 5))).strftime("%H:%M:%S.%f")[:-3],
                'width': f"{50.0 + i:.2f}",  # Same for all rows in block
                'height': f"{2.0 + i * 0.1:.1f}",  # Same for all rows in block
                'angle': f"{i * 10.0:.2f}",  # Same for all rows in block
                'category_3': chr(ord('a') + (i % 3)),
                'isGood': i % 2 == 0,
                'value': f"value_{i}",
                'tags': f"tag{i % 4}"
            }
            base_block.append(row)
            current_time += timedelta(seconds=1)
            
        data.extend(base_block)
        
        # Add time gap
        current_time += timedelta(seconds=30)
        
        # For some groups, create duplicates
        if i in [2, 5, 8]:  # Duplicate groups 3, 6, and 9
            # Create duplicate block with updated times only
            dup_block = []
            for row in base_block:
                dup_row = row.copy()
                dup_row['good_time'] = current_time.strftime("%H:%M:%S.%f")[:-3]
                dup_row['dumb_time'] = (current_time + timedelta(seconds=np.random.uniform(-5, 5))).strftime("%H:%M:%S.%f")[:-3]
                dup_block.append(dup_row)
                current_time += timedelta(seconds=1)
                
            data.extend(dup_block)
            
            # Add another time gap
            current_time += timedelta(seconds=30)
            
            # For group 5, create a triple (second duplicate)
            if i == 5:
                dup_block2 = []
                for row in base_block:
                    dup_row = row.copy()
                    dup_row['good_time'] = current_time.strftime("%H:%M:%S.%f")[:-3]
                    dup_row['dumb_time'] = (current_time + timedelta(seconds=np.random.uniform(-5, 5))).strftime("%H:%M:%S.%f")[:-3]
                    dup_block2.append(dup_row)
                    current_time += timedelta(seconds=1)
                    
                data.extend(dup_block2)
                current_time += timedelta(seconds=30)
        
        group_id += 1
    
    df = pd.DataFrame(data)
    return df

def analyze_duplicates(df):
    """Analyze the duplicate blocks in the dataframe."""
    print("=== Duplicate Block Analysis ===")
    print(f"Total rows: {len(df)}")
    
    # Group by group_id to see the structure
    group_counts = df['group_id'].value_counts().sort_index()
    print("\nRows per group_id:")
    print(group_counts)
    
    # Check for duplicate blocks (excluding time columns)
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    
    # Group by all non-time columns
    duplicates = df.groupby(non_time_cols).size().reset_index(name='count')
    duplicates = duplicates[duplicates['count'] > 1].sort_values('count', ascending=False)
    
    print(f"\nDuplicate blocks found: {len(duplicates)}")
    print("\nDuplicate block details:")
    for idx, row in duplicates.iterrows():
        print(f"\nBlock with group_id={row['group_id']} appears {row['count']} times")
        # Find all occurrences
        mask = True
        for col in non_time_cols:
            mask = mask & (df[col] == row[col])
        occurrences = df[mask]
        print(f"  Row indices: {list(occurrences.index)}")
        print(f"  Time ranges: {occurrences.groupby(occurrences.index // 5)['good_time'].agg(['first', 'last']).values.tolist()}")

def main():
    # Create test data
    print("Creating test data with known duplicate blocks...")
    df = create_test_data_with_duplicates()
    
    # Save to CSV
    output_file = 'test_duplicate_blocks.csv'
    df.to_csv(output_file, index=False)
    print(f"\nSaved test data to {output_file}")
    
    # Analyze the data
    analyze_duplicates(df)
    
    # Now test with Leaf
    print("\n=== Testing with Leaf ===")
    print("1. Import test_duplicate_blocks.csv into Leaf")
    print("2. Use 'Detect Duplicate Blocks' tool")
    print("3. Select 'group_id' as the group column")
    print("4. Ignore 'good_time' and 'dumb_time' columns")
    print("5. Run detection - should find 3 duplicate groups:")
    print("   - Group 3 (duplicated once)")
    print("   - Group 6 (duplicated once)")
    print("   - Group 9 (duplicated twice)")

if __name__ == "__main__":
    main()