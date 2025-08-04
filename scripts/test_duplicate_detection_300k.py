#!/usr/bin/env python3
"""
Test duplicate detection with the 300k dataset.
This script imports the CSV, adds a group ID column, and tests duplicate detection.
"""

import pandas as pd
import numpy as np
import subprocess
import os
import time

def add_group_id_column(input_file, output_file):
    """Add a group ID column based on changes in non-time columns."""
    print(f"Loading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Non-time columns
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    
    # Create group IDs based on value changes
    group_ids = []
    current_group = 1
    
    for i in range(len(df)):
        if i == 0:
            group_ids.append(f"G{current_group:06d}")
        else:
            # Check if any non-time value changed
            if any(df.iloc[i][col] != df.iloc[i-1][col] for col in non_time_cols):
                current_group += 1
            group_ids.append(f"G{current_group:06d}")
    
    df['group_id'] = group_ids
    
    # Move group_id to the front
    cols = ['group_id'] + [col for col in df.columns if col != 'group_id']
    df = df[cols]
    
    print(f"Added group_id column with {current_group} unique groups")
    
    # Save
    df.to_csv(output_file, index=False)
    print(f"Saved to {output_file}")
    
    return current_group

def test_with_rust_binary():
    """Test using the Rust binary."""
    print("\n=== Testing with Rust Binary ===")
    
    # Build and run the test
    cmd = ["cargo", "run", "--release", "--bin", "test_duplicate_detection"]
    
    # First update the test to use our file
    test_file = "src/bin/test_duplicate_detection.rs"
    with open(test_file, 'r') as f:
        content = f.read()
    
    # Update to use our test file
    updated_content = content.replace(
        'let csv_path = Path::new("test_duplicate_groups.csv");',
        'let csv_path = Path::new("test_data_300k_with_groups.csv");'
    ).replace(
        'db.stream_insert_csv_with_header_row(\n        "test_duplicate_groups",',
        'db.stream_insert_csv_with_header_row(\n        "test_data_300k_groups",'
    ).replace(
        'let batch = db.get_table_arrow_batch("test_duplicate_groups")?;',
        'let batch = db.get_table_arrow_batch("test_data_300k_groups")?;'
    ).replace(
        '"test_duplicate_groups"',
        '"test_data_300k_groups"'
    )
    
    with open(test_file, 'w') as f:
        f.write(updated_content)
    
    # Run the test
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    # Restore original
    with open(test_file, 'w') as f:
        f.write(content)

def main():
    # Add group IDs to the data
    input_file = "test_data_300k_blocks.csv"
    output_file = "test_data_300k_with_groups.csv"
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Run generate_test_data.py first.")
        return
    
    # Add group IDs
    num_groups = add_group_id_column(input_file, output_file)
    
    # Analyze the groups
    print("\n=== Analyzing Groups ===")
    df = pd.read_csv(output_file)
    
    # Count rows per group
    group_counts = df['group_id'].value_counts()
    
    # Find groups that appear multiple times (by checking consecutive groups)
    non_time_cols = [col for col in df.columns if 'time' not in col.lower() and col != 'group_id']
    
    # Create signatures for each group
    group_signatures = {}
    for group_id in df['group_id'].unique():
        group_df = df[df['group_id'] == group_id]
        # Use first row as signature
        sig = tuple(group_df.iloc[0][non_time_cols].values)
        if sig not in group_signatures:
            group_signatures[sig] = []
        group_signatures[sig].append(group_id)
    
    # Find duplicates
    duplicate_sigs = {sig: groups for sig, groups in group_signatures.items() if len(groups) > 1}
    
    print(f"\nTotal groups: {num_groups}")
    print(f"Groups with duplicate content: {len(duplicate_sigs)}")
    print(f"Total duplicate group instances: {sum(len(groups) for groups in duplicate_sigs.values())}")
    
    # Show examples
    print("\nExample duplicate groups:")
    for i, (sig, groups) in enumerate(list(duplicate_sigs.items())[:5]):
        print(f"\n{i+1}. Groups with same content: {groups[:5]}...")
        # Show row counts
        for g in groups[:3]:
            count = len(df[df['group_id'] == g])
            print(f"   {g}: {count} rows")
    
    # Test with Rust
    test_with_rust_binary()

if __name__ == "__main__":
    main()