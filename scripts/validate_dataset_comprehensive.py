#!/usr/bin/env python3
"""
Comprehensive validation of the test dataset to ensure:
1. Time columns are correctly formatted and ordered
2. Mini groups have correct structure
3. Major groups have correct duplication patterns
4. Data integrity is maintained
"""

import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import hashlib

def validate_dataset(filename):
    """Comprehensive validation of the dataset."""
    print(f"Loading {filename}...")
    df = pd.read_csv(filename)
    print(f"✅ Loaded {len(df):,} rows with {len(df.columns)} columns\n")
    
    # 1. Validate time columns
    print("=== TIME VALIDATION ===")
    
    # Check good_time format
    print("Checking good_time format...")
    time_format_issues = 0
    for idx, time_str in enumerate(df['good_time']):
        if not isinstance(time_str, str) or not time_str.count(':') == 2:
            time_format_issues += 1
            if time_format_issues < 5:
                print(f"  Issue at row {idx}: {time_str}")
    print(f"✅ good_time format issues: {time_format_issues}")
    
    # Check dumb_time is after good_time
    print("\nChecking dumb_time > good_time...")
    time_order_issues = 0
    for idx, row in df.iterrows():
        if pd.notna(row['dumb_time']):
            good_parts = row['good_time'].split(':')
            good_seconds = int(good_parts[0]) * 3600 + int(good_parts[1]) * 60 + float(good_parts[2])
            
            dumb_parts = row['dumb_time'].split(':')
            dumb_seconds = int(dumb_parts[0]) * 3600 + int(dumb_parts[1]) * 60 + float(dumb_parts[2])
            
            diff = dumb_seconds - good_seconds
            if diff < 0 and abs(diff) < 43200:  # Not a wraparound
                time_order_issues += 1
                if time_order_issues < 5:
                    print(f"  Issue at row {idx}: good={row['good_time']}, dumb={row['dumb_time']}")
    print(f"✅ Time order issues: {time_order_issues}")
    
    # Check good_time is strictly increasing within major groups
    print("\nChecking good_time ordering within major groups...")
    major_group_starts = df[df['dumb_time'].isna()].index.tolist()
    ordering_issues = 0
    
    for i, start_idx in enumerate(major_group_starts):
        end_idx = major_group_starts[i + 1] - 1 if i < len(major_group_starts) - 1 else len(df) - 1
        
        prev_time = None
        for idx in range(start_idx, end_idx + 1):
            time_str = df.iloc[idx]['good_time']
            time_parts = time_str.split(':')
            time_seconds = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + float(time_parts[2])
            
            if prev_time is not None and time_seconds < prev_time:
                ordering_issues += 1
                if ordering_issues < 5:
                    print(f"  Group {i}, row {idx}: time went backwards")
            prev_time = time_seconds
    print(f"✅ Time ordering issues: {ordering_issues}")
    
    # 2. Validate mini groups
    print("\n=== MINI GROUP VALIDATION ===")
    good_time_counts = df['good_time'].value_counts()
    print(f"Unique good_time values: {len(good_time_counts):,}")
    print(f"Mini group sizes: min={good_time_counts.min()}, max={good_time_counts.max()}, avg={good_time_counts.mean():.1f}")
    
    # Check that mini groups have different data
    print("\nChecking data uniqueness within mini groups...")
    mini_group_issues = 0
    for time, count in list(good_time_counts.items())[:10]:
        if count > 1:
            mini_group = df[df['good_time'] == time]
            # Check a few columns for uniqueness
            for col in ['width', 'height', 'angle']:
                unique_vals = mini_group[col].nunique()
                if unique_vals < count:
                    mini_group_issues += 1
                    print(f"  {time}: {col} has only {unique_vals} unique values for {count} rows")
    print(f"✅ Mini group uniqueness issues: {mini_group_issues}")
    
    # 3. Validate major groups
    print("\n=== MAJOR GROUP VALIDATION ===")
    print(f"Major groups found: {len(major_group_starts)}")
    
    # Build major groups
    major_groups = []
    for i, start_idx in enumerate(major_group_starts):
        end_idx = major_group_starts[i + 1] - 1 if i < len(major_group_starts) - 1 else len(df) - 1
        major_groups.append({
            'start': start_idx,
            'end': end_idx,
            'size': end_idx - start_idx + 1,
            'data': df.iloc[start_idx:end_idx+1]
        })
    
    group_sizes = [g['size'] for g in major_groups]
    print(f"Major group sizes: min={min(group_sizes)}, max={max(group_sizes)}, avg={np.mean(group_sizes):.1f}")
    
    # 4. Check duplication patterns
    print("\n=== DUPLICATION PATTERN VALIDATION ===")
    
    # Create signatures for each major group
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    group_signatures = defaultdict(list)
    
    for i, group in enumerate(major_groups):
        # Create signature from non-time columns
        sig_data = []
        for _, row in group['data'].iterrows():
            row_values = [str(row[col]) for col in non_time_cols]
            sig_data.append('|'.join(row_values))
        signature = hashlib.md5('\n'.join(sig_data).encode()).hexdigest()
        group_signatures[signature].append(i)
    
    # Count duplication patterns
    duplication_counts = Counter(len(indices) for indices in group_signatures.values())
    print(f"Unique patterns: {len(group_signatures)}")
    print(f"Duplication distribution:")
    for count, freq in sorted(duplication_counts.items()):
        percentage = freq / len(major_groups) * 100
        print(f"  {count}x: {freq} groups ({percentage:.1f}%)")
    
    # Verify expected rates
    duplicated_once = duplication_counts.get(2, 0)
    triplicated = duplication_counts.get(3, 0)
    total_groups = len(major_groups)
    
    dup_rate = duplicated_once / total_groups * 100
    trip_rate = triplicated / total_groups * 100
    
    print(f"\n✅ Duplication rate: {dup_rate:.1f}% (target: ~10%)")
    print(f"✅ Triplication rate: {trip_rate:.1f}% (target: ~5%)")
    
    # 5. Check for hour gaps
    print("\n=== HOUR GAP VALIDATION ===")
    hour_gaps = 0
    for i in range(1, len(major_groups)):
        prev_group = major_groups[i-1]
        curr_group = major_groups[i]
        
        # Get last time of previous group
        prev_last_time = prev_group['data']['good_time'].iloc[-1]
        prev_parts = prev_last_time.split(':')
        prev_seconds = int(prev_parts[0]) * 3600 + int(prev_parts[1]) * 60 + float(prev_parts[2])
        
        # Get first time of current group
        curr_first_time = curr_group['data']['good_time'].iloc[0]
        curr_parts = curr_first_time.split(':')
        curr_seconds = int(curr_parts[0]) * 3600 + int(curr_parts[1]) * 60 + float(curr_parts[2])
        
        gap = curr_seconds - prev_seconds
        if gap < 0:
            gap += 86400  # Handle day wraparound
        
        if gap >= 3600:  # 1 hour or more
            hour_gaps += 1
    
    print(f"✅ Hour gaps found: {hour_gaps} ({hour_gaps/len(major_groups)*100:.1f}% of transitions)")
    
    # 6. Data integrity checks
    print("\n=== DATA INTEGRITY ===")
    
    # Check for expected columns
    expected_cols = ['good_time', 'dumb_time', 'width', 'height', 'angle'] + \
                   [f'category_{i}' for i in range(3, 11)] + \
                   ['isGood', 'isOld', 'isWhat', 'isEnabled', 'isFlagged'] + \
                   ['bimodal', 'exponential', 'uniform', 'normal', 'tags']
    
    missing_cols = set(expected_cols) - set(df.columns)
    extra_cols = set(df.columns) - set(expected_cols) - set([col for col in df.columns if 'infer' in col])
    
    print(f"✅ Missing columns: {missing_cols if missing_cols else 'None'}")
    print(f"✅ Extra columns: {extra_cols if extra_cols else 'None'}")
    
    # Check data types and ranges
    print("\nChecking data ranges...")
    issues = []
    
    # Numeric columns
    if df['width'].min() < 1 or df['width'].max() > 200:
        issues.append("width out of range")
    if df['height'].min() < 0.2 or df['height'].max() > 4.8:
        issues.append("height out of range")
    if df['angle'].min() < 0 or df['angle'].max() > 360:
        issues.append("angle out of range")
    
    # Boolean columns
    for col in ['isGood', 'isOld', 'isWhat', 'isEnabled', 'isFlagged']:
        unique_vals = df[col].unique()
        if not all(v in [True, False] for v in unique_vals):
            issues.append(f"{col} has non-boolean values")
    
    print(f"✅ Data range issues: {issues if issues else 'None'}")
    
    # Check inference columns have ~10% missing
    print("\nChecking inference column missing rates...")
    for col in df.columns:
        if 'infer' in col:
            if 'blank' in col:
                missing_rate = df[col].isna().sum() / len(df) * 100
            else:  # dash
                missing_rate = (df[col] == '-').sum() / len(df) * 100
            
            if abs(missing_rate - 10) > 5:  # More than 5% off target
                print(f"  {col}: {missing_rate:.1f}% missing (target: ~10%)")
    
    print("\n✅ Dataset validation complete!")
    return True

if __name__ == "__main__":
    import sys
    filename = sys.argv[1] if len(sys.argv) > 1 else "test_data_300k_correct.csv"
    validate_dataset(filename)