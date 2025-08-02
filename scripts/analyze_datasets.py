#!/usr/bin/env python3
"""
Analyze test datasets and generate comprehensive statistics report.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys

# Set UTF-8 encoding for Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def analyze_dataset(filename):
    """Analyze a single dataset and return statistics."""
    print(f"\nAnalyzing {filename}...")
    
    # Get file stats
    file_size_bytes = os.path.getsize(filename)
    file_size_mb = file_size_bytes / (1024 * 1024)
    
    # Load data
    print("  Loading data...")
    df = pd.read_csv(filename, dtype=str, keep_default_na=False)
    
    stats = {
        'filename': filename,
        'file_size_mb': file_size_mb,
        'total_rows': len(df),
        'total_columns': len(df.columns),
    }
    
    # Count groups (rows where dumb_time is empty)
    stats['total_groups'] = sum(1 for val in df['dumb_time'] if val == '')
    
    # Analyze time columns
    print("  Analyzing time columns...")
    good_times = df['good_time'].tolist()
    stats['time_range'] = {
        'first': good_times[0],
        'last': good_times[-1]
    }
    
    # Count time duplicates
    time_counts = pd.Series(good_times).value_counts()
    duplicate_distribution = time_counts.value_counts().to_dict()
    stats['time_duplicate_distribution'] = duplicate_distribution
    
    # Count hour gaps
    hour_gaps = 0
    for i in range(1, len(df)):
        if df['dumb_time'].iloc[i] == '':  # New group
            # Compare with previous row's time
            try:
                prev_time = datetime.strptime(f"2024-01-01 {df['good_time'].iloc[i-1]}", "%Y-%m-%d %H:%M:%S.%f")
                curr_time = datetime.strptime(f"2024-01-01 {df['good_time'].iloc[i]}", "%Y-%m-%d %H:%M:%S.%f")
                
                # Handle day wraparound
                if curr_time < prev_time:
                    curr_time = datetime.strptime(f"2024-01-02 {df['good_time'].iloc[i]}", "%Y-%m-%d %H:%M:%S.%f")
                
                gap_seconds = (curr_time - prev_time).total_seconds()
                if 3540 <= gap_seconds <= 3660:  # Hour gap with tolerance
                    hour_gaps += 1
            except:
                pass
    
    stats['hour_gaps'] = hour_gaps
    stats['hour_gap_percentage'] = (hour_gaps / (stats['total_groups'] - 1) * 100) if stats['total_groups'] > 1 else 0
    
    # Analyze missing values
    print("  Analyzing missing values...")
    missing_stats = {}
    
    for col in df.columns:
        # Count empty strings
        empty_count = sum(1 for val in df[col] if val == '')
        # Count dashes
        dash_count = sum(1 for val in df[col] if val == '-')
        
        if empty_count > 0 or dash_count > 0:
            missing_stats[col] = {
                'empty': empty_count,
                'dash': dash_count,
                'total_missing': empty_count + dash_count,
                'percentage': (empty_count + dash_count) / len(df) * 100
            }
    
    stats['missing_values'] = missing_stats
    
    # Analyze categorical columns
    print("  Analyzing categorical columns...")
    categorical_stats = {}
    for i in range(3, 11):
        col = f'category_{i}'
        unique_values = df[col].unique()
        categorical_stats[col] = {
            'unique_count': len(unique_values),
            'values': sorted(unique_values.tolist())
        }
    
    stats['categorical_columns'] = categorical_stats
    
    # Analyze boolean columns
    print("  Analyzing boolean columns...")
    bool_cols = ['isGood', 'isOld', 'isWhat', 'isEnabled', 'isFlagged']
    boolean_stats = {}
    for col in bool_cols:
        value_counts = df[col].value_counts()
        boolean_stats[col] = {
            'True': value_counts.get('True', 0),
            'False': value_counts.get('False', 0)
        }
    
    stats['boolean_columns'] = boolean_stats
    
    # Analyze numerical columns
    print("  Analyzing numerical columns...")
    numerical_stats = {}
    
    # Convert to numeric for analysis
    for col in ['width', 'height', 'angle']:
        numeric_data = pd.to_numeric(df[col], errors='coerce')
        numerical_stats[col] = {
            'min': numeric_data.min(),
            'max': numeric_data.max(),
            'mean': numeric_data.mean(),
            'std': numeric_data.std()
        }
    
    stats['numerical_columns'] = numerical_stats
    
    # Analyze distribution columns
    print("  Analyzing distribution columns...")
    dist_cols = ['bimodal', 'linear_over_time', 'exponential', 'uniform', 'normal']
    distribution_stats = {}
    
    for col in dist_cols:
        if col in df.columns:
            numeric_data = pd.to_numeric(df[col], errors='coerce')
            distribution_stats[col] = {
                'min': numeric_data.min(),
                'max': numeric_data.max(),
                'mean': numeric_data.mean(),
                'std': numeric_data.std(),
                'nulls': numeric_data.isna().sum()
            }
    
    stats['distribution_columns'] = distribution_stats
    
    # Memory usage
    stats['memory_usage_mb'] = df.memory_usage(deep=True).sum() / (1024 * 1024)
    
    return stats

def generate_markdown_report(all_stats):
    """Generate a markdown report from statistics."""
    report = []
    report.append("# Test Dataset Statistics Report")
    report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("\n## Overview")
    
    # Summary table
    report.append("\n| Dataset | Rows | Columns | File Size | Groups | Hour Gaps |")
    report.append("|---------|------|---------|-----------|--------|-----------|")
    
    for stats in all_stats:
        name = stats['filename'].replace('test_data_', '').replace('.csv', '').upper()
        report.append(f"| {name} | {stats['total_rows']:,} | {stats['total_columns']} | "
                     f"{stats['file_size_mb']:.1f} MB | {stats['total_groups']:,} | "
                     f"{stats['hour_gaps']} ({stats['hour_gap_percentage']:.1f}%) |")
    
    # Detailed stats for each dataset
    for stats in all_stats:
        name = stats['filename'].replace('test_data_', '').replace('.csv', '').upper()
        report.append(f"\n## {name} Dataset Details")
        
        # Basic info
        report.append(f"\n### Basic Information")
        report.append(f"- **File**: `{stats['filename']}`")
        report.append(f"- **Total Rows**: {stats['total_rows']:,}")
        report.append(f"- **Total Columns**: {stats['total_columns']}")
        report.append(f"- **File Size**: {stats['file_size_mb']:.2f} MB")
        report.append(f"- **Memory Usage**: {stats['memory_usage_mb']:.2f} MB")
        report.append(f"- **Total Groups**: {stats['total_groups']:,}")
        
        # Time information
        report.append(f"\n### Time Information")
        report.append(f"- **Time Range**: {stats['time_range']['first']} to {stats['time_range']['last']}")
        report.append(f"- **Hour Gaps**: {stats['hour_gaps']} ({stats['hour_gap_percentage']:.1f}% of gaps)")
        
        # Time duplicate distribution
        report.append(f"\n### Time Duplicate Distribution")
        report.append("| Duplicates | Count |")
        report.append("|------------|-------|")
        for dup_count in sorted(stats['time_duplicate_distribution'].keys()):
            report.append(f"| {dup_count} | {stats['time_duplicate_distribution'][dup_count]:,} |")
        
        # Missing values summary
        report.append(f"\n### Missing Values Summary")
        missing_cols = [col for col, data in stats['missing_values'].items() if data['total_missing'] > 0]
        
        if missing_cols:
            report.append("| Column | Empty | Dash | Total | Percentage |")
            report.append("|--------|-------|------|-------|------------|")
            
            # Sort by column name, putting inference columns together
            for col in sorted(missing_cols):
                data = stats['missing_values'][col]
                report.append(f"| {col} | {data['empty']:,} | {data['dash']:,} | "
                             f"{data['total_missing']:,} | {data['percentage']:.2f}% |")
        
        # Numerical columns
        report.append(f"\n### Numerical Columns Statistics")
        report.append("| Column | Min | Max | Mean | Std Dev |")
        report.append("|--------|-----|-----|------|---------|")
        
        for col, data in stats['numerical_columns'].items():
            report.append(f"| {col} | {data['min']:.2f} | {data['max']:.2f} | "
                         f"{data['mean']:.2f} | {data['std']:.2f} |")
        
        # Boolean columns
        report.append(f"\n### Boolean Columns Distribution")
        report.append("| Column | True | False | True % |")
        report.append("|--------|------|-------|--------|")
        
        for col, data in stats['boolean_columns'].items():
            total = data['True'] + data['False']
            true_pct = (data['True'] / total * 100) if total > 0 else 0
            report.append(f"| {col} | {data['True']:,} | {data['False']:,} | {true_pct:.1f}% |")
        
        # Distribution columns
        report.append(f"\n### Distribution Columns Statistics")
        report.append("| Column | Min | Max | Mean | Std Dev |")
        report.append("|--------|-----|-----|------|---------|")
        
        for col, data in stats['distribution_columns'].items():
            report.append(f"| {col} | {data['min']:.2f} | {data['max']:.2f} | "
                         f"{data['mean']:.2f} | {data['std']:.2f} |")
    
    return '\n'.join(report)

def main():
    """Main function to analyze all datasets."""
    datasets = ['test_data_10k.csv', 'test_data_300k.csv', 'test_data_3m.csv']
    all_stats = []
    
    for dataset in datasets:
        if os.path.exists(dataset):
            stats = analyze_dataset(dataset)
            all_stats.append(stats)
        else:
            print(f"Warning: {dataset} not found")
    
    # Generate report
    print("\nGenerating markdown report...")
    report = generate_markdown_report(all_stats)
    
    # Save report
    with open('dataset_statistics.md', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("\n✅ Report saved to dataset_statistics.md")

if __name__ == "__main__":
    main()