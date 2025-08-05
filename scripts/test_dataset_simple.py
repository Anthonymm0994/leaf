#!/usr/bin/env python3
"""
Simple test to validate the dataset is correct.
"""

import pandas as pd
import numpy as np

def test_dataset():
    print("=== Simple Dataset Test ===\n")
    
    # Load the dataset
    df = pd.read_csv('test_data_300k_correct.csv')
    print(f"✅ Loaded {len(df):,} rows")
    
    # Check time ordering
    print("\n1. Checking time ordering...")
    issues = 0
    for i in range(1, len(df)):
        if pd.notna(df.loc[i, 'dumb_time']):
            # Parse times
            good_curr = df.loc[i, 'good_time']
            dumb_curr = df.loc[i, 'dumb_time']
            
            good_parts = good_curr.split(':')
            good_sec = int(good_parts[0]) * 3600 + int(good_parts[1]) * 60 + float(good_parts[2])
            
            dumb_parts = dumb_curr.split(':')
            dumb_sec = int(dumb_parts[0]) * 3600 + int(dumb_parts[1]) * 60 + float(dumb_parts[2])
            
            # Check if dumb > good (accounting for wraparound)
            diff = dumb_sec - good_sec
            if diff < 0 and abs(diff) < 43200:
                issues += 1
                if issues <= 3:
                    print(f"   Issue at row {i}: good={good_curr}, dumb={dumb_curr}")
    
    print(f"✅ Time ordering issues: {issues}")
    
    # Check mini groups
    print("\n2. Checking mini groups (same good_time)...")
    good_time_groups = df.groupby('good_time').size()
    print(f"   Unique good_time values: {len(good_time_groups):,}")
    print(f"   Group sizes: min={good_time_groups.min()}, max={good_time_groups.max()}, avg={good_time_groups.mean():.1f}")
    
    # Check data uniqueness in mini groups
    print("\n3. Checking data uniqueness within mini groups...")
    sample_times = good_time_groups[good_time_groups > 1].head(5).index
    for time in sample_times:
        mini_group = df[df['good_time'] == time]
        unique_widths = mini_group['width'].nunique()
        print(f"   {time} ({len(mini_group)} rows): {unique_widths} unique width values")
    
    # Check major groups
    print("\n4. Checking major groups...")
    major_group_starts = df[df['dumb_time'].isna()].index.tolist()
    print(f"   Major groups: {len(major_group_starts)}")
    
    # Check for duplicates
    print("\n5. Checking for duplicate major groups...")
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    
    group_hashes = {}
    for i, start in enumerate(major_group_starts):
        end = major_group_starts[i+1] if i < len(major_group_starts)-1 else len(df)
        group_data = df.iloc[start:end][non_time_cols]
        
        # Create hash of group content
        group_str = group_data.to_csv(index=False)
        group_hash = hash(group_str)
        
        if group_hash not in group_hashes:
            group_hashes[group_hash] = []
        group_hashes[group_hash].append(i)
    
    duplicates = {h: indices for h, indices in group_hashes.items() if len(indices) > 1}
    print(f"   Unique patterns: {len(group_hashes)}")
    print(f"   Duplicate patterns: {len(duplicates)}")
    
    # Show duplication stats
    dup_counts = {}
    for indices in duplicates.values():
        count = len(indices)
        dup_counts[count] = dup_counts.get(count, 0) + 1
    
    for count, freq in sorted(dup_counts.items()):
        pct = freq / len(major_group_starts) * 100
        print(f"   {count}x: {freq} groups ({pct:.1f}%)")
    
    print("\n✅ Dataset validation complete!")

if __name__ == "__main__":
    test_dataset()