#!/usr/bin/env python3
"""
Analyze the 24-hour constrained test datasets to verify they stay within a single day.
"""

import pandas as pd
import os
import sys
from datetime import datetime, timedelta

# Set UTF-8 encoding for Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def parse_time_to_seconds(time_str: str) -> float:
    """Convert HH:MM:SS.sss to seconds since midnight."""
    parts = time_str.split(':')
    hours = int(parts[0])
    minutes = int(parts[1])
    seconds = float(parts[2])
    return hours * 3600 + minutes * 60 + seconds

def analyze_dataset(filename: str):
    """Analyze a single dataset for time span and other statistics."""
    print(f"\n{'='*60}")
    print(f"Analyzing: {filename}")
    print(f"{'='*60}")
    
    if not os.path.exists(filename):
        print(f"❌ File not found: {filename}")
        return None
    
    # Load dataset
    print("Loading dataset...")
    df = pd.read_csv(filename, dtype=str, keep_default_na=False)
    
    # Basic info
    print(f"\n📊 Basic Information:")
    print(f"  - Total Rows: {len(df):,}")
    print(f"  - Total Columns: {len(df.columns)}")
    print(f"  - File Size: {os.path.getsize(filename) / 1024 / 1024:.1f} MB")
    
    # Time analysis
    print(f"\n⏰ Time Analysis:")
    first_time = df['good_time'].iloc[0]
    last_time = df['good_time'].iloc[-1]
    
    first_seconds = parse_time_to_seconds(first_time)
    last_seconds = parse_time_to_seconds(last_time)
    
    print(f"  - First time: {first_time}")
    print(f"  - Last time: {last_time}")
    print(f"  - Time span in hours: {(last_seconds - first_seconds) / 3600:.2f}")
    
    # Check if times stay within 24 hours
    all_within_24h = True
    for time_str in df['good_time']:
        seconds = parse_time_to_seconds(time_str)
        if seconds >= 24 * 3600:
            all_within_24h = False
            print(f"  ❌ Found time exceeding 24 hours: {time_str}")
            break
    
    if all_within_24h:
        print(f"  ✅ All times stay within 24-hour period (00:00:00.000 - 23:59:59.999)")
    
    # Count groups (where dumb_time is empty)
    group_count = (df['dumb_time'] == '').sum()
    print(f"\n👥 Groups:")
    print(f"  - Total groups: {group_count}")
    print(f"  - Average rows per group: {len(df) / group_count:.1f}")
    
    # Time duplicate analysis
    print(f"\n🔄 Time Duplicates:")
    time_counts = df['good_time'].value_counts()
    duplicate_distribution = time_counts.value_counts().sort_index()
    for dup_count, occurrences in duplicate_distribution.items():
        print(f"  - {dup_count} duplicates: {occurrences} times")
    
    # Hour gap detection
    print(f"\n⏳ Gap Analysis:")
    gaps = []
    group_starts = [0] + [i for i in range(1, len(df)) if df['dumb_time'].iloc[i] == '']
    
    for i in range(1, len(group_starts)):
        prev_end = group_starts[i] - 1
        curr_start = group_starts[i]
        
        prev_time = parse_time_to_seconds(df['good_time'].iloc[prev_end])
        curr_time = parse_time_to_seconds(df['good_time'].iloc[curr_start])
        
        gap = curr_time - prev_time
        gaps.append(gap)
    
    if gaps:
        # Categorize gaps
        small_gaps = [g for g in gaps if g < 60]  # Less than 1 minute
        medium_gaps = [g for g in gaps if 60 <= g < 600]  # 1-10 minutes
        large_gaps = [g for g in gaps if g >= 600]  # 10+ minutes
        
        print(f"  - Small gaps (<1 min): {len(small_gaps)}")
        print(f"  - Medium gaps (1-10 min): {len(medium_gaps)}")
        print(f"  - Large gaps (10+ min): {len(large_gaps)}")
        
        if large_gaps:
            print(f"  - Largest gap: {max(large_gaps)/3600:.2f} hours")
    
    return {
        'filename': filename,
        'rows': len(df),
        'columns': len(df.columns),
        'file_size_mb': os.path.getsize(filename) / 1024 / 1024,
        'first_time': first_time,
        'last_time': last_time,
        'time_span_hours': (last_seconds - first_seconds) / 3600,
        'all_within_24h': all_within_24h,
        'groups': group_count,
        'avg_rows_per_group': len(df) / group_count
    }

def main():
    """Analyze all three datasets."""
    datasets = [
        'test_data_10k.csv',
        'test_data_300k.csv',
        'test_data_3m.csv'
    ]
    
    print("🔍 Analyzing 24-hour Constrained Test Datasets")
    print("=" * 60)
    
    results = []
    for dataset in datasets:
        result = analyze_dataset(dataset)
        if result:
            results.append(result)
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 SUMMARY")
    print(f"{'='*60}")
    
    print("\n| Dataset | Rows | Time Span | Within 24h | Groups |")
    print("|---------|------|-----------|------------|--------|")
    
    for r in results:
        print(f"| {r['filename'].replace('test_data_', '').replace('.csv', '')} | "
              f"{r['rows']:,} | "
              f"{r['first_time']} - {r['last_time']} ({r['time_span_hours']:.1f}h) | "
              f"{'✅' if r['all_within_24h'] else '❌'} | "
              f"{r['groups']} |")
    
    # Check if all datasets are within 24 hours
    all_good = all(r['all_within_24h'] for r in results)
    
    print(f"\n{'✅ SUCCESS: All datasets stay within 24-hour period!' if all_good else '❌ ERROR: Some datasets exceed 24 hours!'}")

if __name__ == "__main__":
    main()