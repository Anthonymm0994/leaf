#!/usr/bin/env python3
"""
Remove duplicate major groups from CSV files.

This script:
1. Identifies major groups (sequences ending when dumb_time is null)
2. Detects duplicate major groups based on non-time column values
3. Removes duplicate groups, keeping only the first occurrence
4. Saves the cleaned data to a new file
"""

import pandas as pd
import numpy as np
from collections import defaultdict
import argparse
from datetime import datetime
import hashlib

def identify_major_groups(df):
    """Identify major groups based on null dumb_time values."""
    major_group_starts = df[df['dumb_time'].isna()].index.tolist()
    
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
            'group_index': i
        })
    
    return major_groups

def create_group_signature(df, start_idx, end_idx, non_time_cols):
    """Create a signature for a major group based on non-time columns."""
    group_data = df.iloc[start_idx:end_idx+1][non_time_cols]
    
    # Create a string representation of all values
    values_str = ""
    for _, row in group_data.iterrows():
        row_values = [str(row[col]) for col in non_time_cols]
        values_str += "|".join(row_values) + "\n"
    
    # Return hash of the string
    return hashlib.md5(values_str.encode()).hexdigest()

def find_duplicate_groups(df, major_groups):
    """Find duplicate major groups based on their content."""
    # Get non-time columns
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    
    # Create signatures for each major group
    group_signatures = defaultdict(list)
    
    for group in major_groups:
        signature = create_group_signature(df, group['start'], group['end'], non_time_cols)
        group_signatures[signature].append(group)
    
    # Identify duplicates
    unique_groups = []
    duplicate_groups = []
    groups_to_keep = set()
    
    for signature, groups in group_signatures.items():
        # Keep the first occurrence
        groups_to_keep.add(groups[0]['group_index'])
        unique_groups.append(groups[0])
        
        # Mark the rest as duplicates
        for group in groups[1:]:
            duplicate_groups.append(group)
    
    return unique_groups, duplicate_groups, groups_to_keep

def remove_duplicate_groups(df, groups_to_keep, major_groups):
    """Remove duplicate major groups from the dataframe."""
    # Create a mask for rows to keep
    keep_mask = pd.Series([False] * len(df))
    
    for group in major_groups:
        if group['group_index'] in groups_to_keep:
            keep_mask.iloc[group['start']:group['end']+1] = True
    
    # Return filtered dataframe
    return df[keep_mask].reset_index(drop=True)

def analyze_duplicates(df, unique_groups, duplicate_groups):
    """Print analysis of duplicate groups."""
    total_groups = len(unique_groups) + len(duplicate_groups)
    
    print(f"\n=== Duplicate Analysis ===")
    print(f"Total major groups: {total_groups}")
    print(f"Unique major groups: {len(unique_groups)}")
    print(f"Duplicate major groups: {len(duplicate_groups)}")
    print(f"Duplication rate: {len(duplicate_groups)/total_groups*100:.1f}%")
    
    # Count duplication frequency
    dup_counts = defaultdict(int)
    all_groups = unique_groups + duplicate_groups
    
    # Group by signature
    sig_groups = defaultdict(list)
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    
    for group in all_groups:
        sig = create_group_signature(df, group['start'], group['end'], non_time_cols)
        sig_groups[sig].append(group)
    
    for groups in sig_groups.values():
        dup_counts[len(groups)] += 1
    
    print("\nDuplication frequency:")
    for count, freq in sorted(dup_counts.items()):
        print(f"  {count}x: {freq} patterns")

def main():
    parser = argparse.ArgumentParser(description='Remove duplicate major groups from CSV files')
    parser.add_argument('input_file', help='Input CSV file')
    parser.add_argument('-o', '--output', help='Output CSV file (default: input_cleaned.csv)')
    parser.add_argument('-a', '--analyze-only', action='store_true', 
                       help='Only analyze duplicates without removing them')
    parser.add_argument('-v', '--verbose', action='store_true', 
                       help='Show detailed information')
    
    args = parser.parse_args()
    
    # Set output filename
    if not args.output:
        base_name = args.input_file.rsplit('.', 1)[0]
        args.output = f"{base_name}_cleaned.csv"
    
    print(f"Loading {args.input_file}...")
    df = pd.read_csv(args.input_file)
    print(f"Loaded {len(df):,} rows")
    
    # Identify major groups
    print("\nIdentifying major groups...")
    major_groups = identify_major_groups(df)
    print(f"Found {len(major_groups)} major groups")
    
    # Find duplicates
    print("\nFinding duplicate groups...")
    unique_groups, duplicate_groups, groups_to_keep = find_duplicate_groups(df, major_groups)
    
    # Analyze duplicates
    analyze_duplicates(df, unique_groups, duplicate_groups)
    
    if args.analyze_only:
        print("\nAnalysis complete (no changes made)")
        return
    
    # Remove duplicates
    print(f"\nRemoving {len(duplicate_groups)} duplicate groups...")
    df_cleaned = remove_duplicate_groups(df, groups_to_keep, major_groups)
    
    # Save cleaned data
    print(f"\nSaving cleaned data to {args.output}...")
    df_cleaned.to_csv(args.output, index=False)
    
    # Summary
    rows_removed = len(df) - len(df_cleaned)
    print(f"\n=== Summary ===")
    print(f"Original rows: {len(df):,}")
    print(f"Cleaned rows: {len(df_cleaned):,}")
    print(f"Rows removed: {rows_removed:,} ({rows_removed/len(df)*100:.1f}%)")
    print(f"Output saved to: {args.output}")

if __name__ == "__main__":
    main()