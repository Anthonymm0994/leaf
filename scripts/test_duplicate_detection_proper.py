#!/usr/bin/env python3
"""
Test the duplicate detection as it's actually designed to work.
The tool finds groups (identified by a group column) that have identical content.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_test_data_for_duplicate_detection():
    """Create test data that matches how the duplicate detector works."""
    
    data = []
    current_time = datetime(2024, 1, 1, 0, 0, 0)
    
    # Create groups with some having identical content
    group_templates = []
    
    # Create 5 unique templates for groups
    for i in range(5):
        template = {
            'width': f"{50.0 + i * 10:.2f}",
            'height': f"{2.0 + i * 0.5:.1f}",
            'angle': f"{i * 45.0:.2f}",
            'category_3': chr(ord('a') + (i % 3)),
            'isGood': i % 2 == 0,
            'value': f"value_{i}",
            'tags': f"tag{i % 3}"
        }
        group_templates.append(template)
    
    # Create 20 groups, some using the same template
    group_assignments = [
        0, 1, 2, 3, 4,  # Groups 1-5: unique
        0, 1,           # Groups 6-7: duplicate of 1,2
        5, 6, 7,        # Groups 8-10: new unique
        2,              # Group 11: duplicate of 3
        8, 9,           # Groups 12-13: new unique
        0,              # Group 14: triplicate of 1
        10, 11, 12,     # Groups 15-17: new unique
        3,              # Group 18: duplicate of 4
        13, 14          # Groups 19-20: new unique
    ]
    
    # Use templates or create new ones
    for group_id, assignment in enumerate(group_assignments, 1):
        # Determine template
        if assignment < len(group_templates):
            template = group_templates[assignment].copy()
        else:
            # Create a new unique template
            template = {
                'width': f"{100.0 + assignment * 5:.2f}",
                'height': f"{3.0 + assignment * 0.2:.1f}",
                'angle': f"{assignment * 20.0:.2f}",
                'category_3': chr(ord('a') + (assignment % 3)),
                'isGood': assignment % 2 == 0,
                'value': f"value_{assignment}",
                'tags': f"tag{assignment % 4}"
            }
        
        # Create 5-10 rows for this group
        group_size = np.random.randint(5, 11)
        
        for j in range(group_size):
            row = template.copy()
            row['group_id'] = f"G{group_id:03d}"
            row['good_time'] = current_time.strftime("%H:%M:%S.%f")[:-3]
            row['dumb_time'] = (current_time + timedelta(seconds=np.random.uniform(-5, 5))).strftime("%H:%M:%S.%f")[:-3]
            
            data.append(row)
            current_time += timedelta(seconds=1)
        
        # Add gap between groups
        current_time += timedelta(seconds=30)
    
    df = pd.DataFrame(data)
    return df

def analyze_for_duplicates(df):
    """Analyze which groups have duplicate content."""
    print("=== Analyzing for Duplicate Groups ===")
    
    # Group by group_id
    groups = df.groupby('group_id')
    
    # For each group, create a signature of its non-time content
    group_signatures = {}
    non_time_cols = [col for col in df.columns if 'time' not in col.lower() and col != 'group_id']
    
    for group_id, group_df in groups:
        # Take first row's non-time values as signature
        signature = tuple(group_df.iloc[0][non_time_cols].values)
        if signature not in group_signatures:
            group_signatures[signature] = []
        group_signatures[signature].append(group_id)
    
    # Find duplicates
    print("\nGroups with identical content:")
    duplicate_count = 0
    for signature, group_ids in group_signatures.items():
        if len(group_ids) > 1:
            duplicate_count += 1
            print(f"\nDuplicate set {duplicate_count}:")
            print(f"  Groups: {', '.join(group_ids)} ({len(group_ids)} occurrences)")
            # Show the content
            example_group = groups.get_group(group_ids[0])
            print("  Content:")
            for col in non_time_cols[:5]:  # Show first 5 columns
                print(f"    {col}: {example_group.iloc[0][col]}")

def main():
    # Create test data
    print("Creating test data for duplicate group detection...")
    df = create_test_data_for_duplicate_detection()
    
    # Save to CSV
    output_file = 'test_duplicate_groups.csv'
    df.to_csv(output_file, index=False)
    print(f"\nSaved test data to {output_file}")
    print(f"Total rows: {len(df)}")
    print(f"Total groups: {df['group_id'].nunique()}")
    
    # Analyze
    analyze_for_duplicates(df)
    
    print("\n=== Expected Results in Leaf ===")
    print("When using 'Detect Duplicate Blocks':")
    print("1. Select 'group_id' as the group column")
    print("2. Ignore 'good_time' and 'dumb_time' columns")
    print("3. Should find duplicate groups as shown above")

if __name__ == "__main__":
    main()