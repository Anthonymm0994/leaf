#!/usr/bin/env python3
"""
Debug the duplicate detection by analyzing what should be detected.
"""

import pandas as pd
import subprocess
import os

def analyze_test_data():
    """Analyze the test data to understand what duplicates exist."""
    
    # Load the test data
    df = pd.read_csv('test_duplicate_groups.csv')
    
    print("=== Test Data Analysis ===")
    print(f"Total rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    
    # Group by group_id
    groups = df.groupby('group_id')
    print(f"\nTotal groups: {len(groups)}")
    
    # For each group, get its signature (first row's non-time values)
    non_time_cols = [col for col in df.columns if 'time' not in col.lower() and col != 'group_id']
    print(f"\nNon-time columns: {non_time_cols}")
    
    # Create signatures for each group
    group_signatures = {}
    for group_id, group_df in groups:
        # Use first row as signature
        sig_values = []
        for col in non_time_cols:
            val = group_df.iloc[0][col]
            sig_values.append(f"{col}={val}")
        signature = ", ".join(sig_values)
        group_signatures[group_id] = {
            'signature': signature,
            'row_count': len(group_df),
            'rows': list(group_df.index)
        }
    
    # Find duplicates
    sig_to_groups = {}
    for group_id, info in group_signatures.items():
        sig = info['signature']
        if sig not in sig_to_groups:
            sig_to_groups[sig] = []
        sig_to_groups[sig].append((group_id, info['row_count']))
    
    print("\n=== Expected Duplicate Groups ===")
    dup_count = 0
    for sig, group_list in sig_to_groups.items():
        if len(group_list) > 1:
            dup_count += 1
            print(f"\nDuplicate Set {dup_count}:")
            print(f"  Groups: {[g[0] for g in group_list]}")
            print(f"  Row counts: {[g[1] for g in group_list]}")
            print(f"  Signature (first 3 fields): {', '.join(sig.split(', ')[:3])}")
    
    if dup_count == 0:
        print("\nNo duplicates found in test data!")
    
    # Now let's see what the detector is actually doing
    print("\n=== Running Leaf Import ===")
    
    # Import into Leaf
    result = subprocess.run([
        'cargo', 'run', '--release', '--bin', 'test_duplicate_detection'
    ], capture_output=True, text=True)
    
    print("STDOUT:")
    print(result.stdout)
    if result.stderr:
        print("\nSTDERR:")
        print(result.stderr)

def check_original_data():
    """Check if the original 300k data has the expected duplicate pattern."""
    
    csv_path = 'data_gen_scripts/test_data_300k.csv'
    if not os.path.exists(csv_path):
        csv_path = 'test_data/test_data_300k.csv'
        if not os.path.exists(csv_path):
            print("Cannot find test_data_300k.csv")
            return
    
    print("\n\n=== Checking Original 300k Data ===")
    df = pd.read_csv(csv_path)
    
    # Look for consecutive rows with identical non-time values
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    
    # Check first 1000 rows for patterns
    duplicate_blocks = []
    i = 0
    while i < min(1000, len(df) - 1):
        # Check if current row equals next row (excluding time)
        if all(df.iloc[i][col] == df.iloc[i+1][col] for col in non_time_cols):
            # Found start of duplicate block
            j = i + 1
            while j < len(df) and all(df.iloc[i][col] == df.iloc[j][col] for col in non_time_cols):
                j += 1
            duplicate_blocks.append((i, j-1))
            i = j
        else:
            i += 1
    
    print(f"Found {len(duplicate_blocks)} duplicate blocks in first 1000 rows")
    if duplicate_blocks:
        print("\nFirst 5 duplicate blocks:")
        for start, end in duplicate_blocks[:5]:
            print(f"  Rows {start}-{end} (size: {end-start+1})")

if __name__ == "__main__":
    analyze_test_data()
    check_original_data()