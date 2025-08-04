#!/usr/bin/env python3
"""
Analyze the correctly generated test data to verify:
1. Mini groups have same good_time but different data
2. Major groups (sequences) are duplicated as whole units
"""

import pandas as pd
import numpy as np
from collections import defaultdict

def analyze_data_pattern(filename):
    """Analyze the data pattern in the file."""
    print(f"Loading {filename}...")
    df = pd.read_csv(filename)
    print(f"Total rows: {len(df):,}")
    
    # 1. Analyze mini groups (same good_time)
    print("\n=== Mini Group Analysis ===")
    good_time_counts = df['good_time'].value_counts()
    print(f"Unique good_time values: {len(good_time_counts):,}")
    print(f"Mini group sizes: min={good_time_counts.min()}, max={good_time_counts.max()}, avg={good_time_counts.mean():.1f}")
    
    # Check that mini groups have different data
    print("\nChecking data uniqueness within mini groups (first 5):")
    for i, (time, count) in enumerate(good_time_counts.head().items()):
        if count > 1:
            mini_group = df[df['good_time'] == time]
            unique_values = {
                'width': mini_group['width'].nunique(),
                'category_3': mini_group['category_3'].nunique(),
                'isGood': mini_group['isGood'].nunique()
            }
            print(f"  {time} ({count} rows): unique values = {unique_values}")
    
    # 2. Identify major groups (by null dumb_time)
    print("\n=== Major Group Analysis ===")
    major_group_starts = df[df['dumb_time'].isna()].index.tolist()
    print(f"Major groups found: {len(major_group_starts)}")
    
    # Build major groups
    major_groups = []
    for i, start_idx in enumerate(major_group_starts):
        # Find end of this major group
        if i < len(major_group_starts) - 1:
            end_idx = major_group_starts[i + 1] - 1
        else:
            end_idx = len(df) - 1
        
        major_groups.append({
            'start': start_idx,
            'end': end_idx,
            'size': end_idx - start_idx + 1,
            'data': df.iloc[start_idx:end_idx+1]
        })
    
    print(f"Major group sizes: min={min(g['size'] for g in major_groups)}, "
          f"max={max(g['size'] for g in major_groups)}, "
          f"avg={sum(g['size'] for g in major_groups)/len(major_groups):.1f}")
    
    # 3. Look for duplicate major groups
    print("\n=== Duplicate Major Group Detection ===")
    
    # Create signatures for each major group (excluding time columns)
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    group_signatures = {}
    
    for i, group in enumerate(major_groups):
        # Create a signature from the sequence of values
        # We'll use a hash of all non-time values in sequence
        values_str = ""
        for _, row in group['data'].iterrows():
            row_values = [str(row[col]) for col in non_time_cols]
            values_str += "|".join(row_values) + "\n"
        
        signature = hash(values_str)
        
        if signature not in group_signatures:
            group_signatures[signature] = []
        group_signatures[signature].append(i)
    
    # Find duplicates
    duplicate_signatures = {sig: indices for sig, indices in group_signatures.items() if len(indices) > 1}
    
    print(f"Unique major group patterns: {len(group_signatures)}")
    print(f"Duplicate major group patterns: {len(duplicate_signatures)}")
    
    # Show examples
    if duplicate_signatures:
        print("\nExample duplicate major groups:")
        for i, (sig, indices) in enumerate(list(duplicate_signatures.items())[:3]):
            print(f"\n{i+1}. Pattern appears {len(indices)} times:")
            print(f"   Major group indices: {indices}")
            for idx in indices[:3]:
                group = major_groups[idx]
                print(f"   Group {idx}: rows {group['start']}-{group['end']} (size: {group['size']})")
                # Show time range
                print(f"     Time range: {group['data']['good_time'].iloc[0]} to {group['data']['good_time'].iloc[-1]}")
    
    # Calculate duplication statistics
    total_groups = len(major_groups)
    duplicated_once = sum(1 for indices in duplicate_signatures.values() if len(indices) == 2)
    triplicated = sum(1 for indices in duplicate_signatures.values() if len(indices) == 3)
    
    print(f"\n=== Duplication Statistics ===")
    print(f"Total major groups: {total_groups}")
    print(f"Groups appearing 2x: {duplicated_once} ({duplicated_once/total_groups*100:.1f}%)")
    print(f"Groups appearing 3x: {triplicated} ({triplicated/total_groups*100:.1f}%)")

if __name__ == "__main__":
    import sys
    filename = sys.argv[1] if len(sys.argv) > 1 else "test_data_300k_correct.csv"
    analyze_data_pattern(filename)