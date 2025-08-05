#!/usr/bin/env python3
"""
Test duplicate detection on major groups in the 300k dataset.
First adds a major_group_id column, then uses that for duplicate detection.
"""

import pandas as pd
import numpy as np
import subprocess
import os
from datetime import datetime

def add_major_group_ids(input_file, output_file):
    """Add major_group_id column to identify major groups."""
    print(f"Loading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Identify major groups by null dumb_time
    major_group_starts = df[df['dumb_time'].isna()].index.tolist()
    print(f"Found {len(major_group_starts)} major groups")
    
    # Assign major group IDs
    major_group_ids = np.zeros(len(df), dtype=int)
    
    for i, start_idx in enumerate(major_group_starts):
        end_idx = major_group_starts[i + 1] if i < len(major_group_starts) - 1 else len(df)
        major_group_ids[start_idx:end_idx] = i + 1
    
    df['major_group_id'] = major_group_ids
    
    # Save with major group IDs
    df.to_csv(output_file, index=False)
    print(f"Saved to {output_file} with major_group_id column")
    
    # Return statistics
    group_counts = df['major_group_id'].value_counts()
    return {
        'total_rows': len(df),
        'total_groups': len(group_counts),
        'avg_group_size': group_counts.mean(),
        'min_group_size': group_counts.min(),
        'max_group_size': group_counts.max()
    }

def test_major_group_duplicates():
    """Test duplicate detection on major groups."""
    print("=== Testing Major Group Duplicate Detection ===\n")
    
    # Add major group IDs
    stats = add_major_group_ids(
        'test_data_300k_correct.csv',
        'test_data_300k_with_groups.csv'
    )
    
    print(f"\nDataset statistics:")
    print(f"  Total rows: {stats['total_rows']:,}")
    print(f"  Major groups: {stats['total_groups']}")
    print(f"  Avg group size: {stats['avg_group_size']:.1f}")
    print(f"  Group size range: {stats['min_group_size']}-{stats['max_group_size']}")
    
    # Create Rust test
    test_code = """
use leaf::core::{Database, duplicate_detector::{DuplicateDetector, DuplicateDetectionConfig}};
use std::sync::Arc;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    println!("\\nTesting duplicate detection on major groups...");
    let db = Arc::new(Database::new()?);
    
    // Import the CSV with major group IDs
    let csv_path = Path::new("test_data_300k_with_groups.csv");
    let table_name = db.import_csv_as_table(csv_path)?;
    println!("Imported table: {}", table_name);
    
    // Get the record batch
    let batch = db.get_table_arrow_batch(&table_name)?;
    println!("Loaded {} rows", batch.num_rows());
    
    // Configure duplicate detection for major groups
    let config = DuplicateDetectionConfig {
        group_column: "major_group_id".to_string(),
        ignore_columns: vec!["good_time".to_string(), "dumb_time".to_string(), "major_group_id".to_string()],
        null_equals_null: true,
    };
    
    let detector = DuplicateDetector::new(config);
    
    println!("\\nRunning duplicate detection...");
    let result = detector.detect_duplicates(&batch)?;
    
    println!("\\n=== Detection Results ===");
    println!("Total duplicate groups found: {}", result.duplicate_groups.len());
    println!("Total duplicate rows: {}", result.total_duplicate_rows);
    
    // Calculate percentages
    let total_major_groups = 855; // From our analysis
    let duplicate_major_groups = result.duplicate_groups.len();
    let duplicate_percentage = (duplicate_major_groups as f64 / total_major_groups as f64) * 100.0;
    
    println!("\\nMajor group statistics:");
    println!("  Total major groups: {}", total_major_groups);
    println!("  Duplicate major groups: {}", duplicate_major_groups);
    println!("  Percentage with duplicates: {:.1}%", duplicate_percentage);
    
    // Count duplication patterns
    let mut duplication_counts = std::collections::HashMap::new();
    for (_, group_info) in &result.duplicate_groups {
        *duplication_counts.entry(group_info.occurrences).or_insert(0) += 1;
    }
    
    println!("\\nDuplication patterns:");
    for (count, freq) in duplication_counts.iter() {
        let percentage = (*freq as f64 / total_major_groups as f64) * 100.0;
        println!("  {}x: {} groups ({:.1}%)", count, freq, percentage);
    }
    
    // Show examples
    if result.duplicate_groups.len() > 0 {
        println!("\\nExample duplicate major groups (first 3):");
        for (i, (_, group_info)) in result.duplicate_groups.iter().take(3).enumerate() {
            println!("\\nGroup {}: major_group_id={}", i + 1, group_info.group_id);
            println!("  Occurrences: {}", group_info.occurrences);
            println!("  Rows per occurrence: {}", group_info.group_size);
            println!("  Total rows: {}", group_info.total_rows);
            if let Some(rows) = &group_info.row_indices {
                println!("  First occurrence: rows {}-{}", 
                         rows.first().unwrap_or(&0), 
                         rows.first().unwrap_or(&0) + group_info.group_size - 1);
            }
        }
    }
    
    // Export clean data
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let output_filename = format!("test_data_300k_no_duplicates_{}.arrow", timestamp);
    let output_path = Path::new(&output_filename);
    
    println!("\\n=== Exporting Clean Data ===");
    let clean_batch = detector.create_clean_arrow_file(&batch, &table_name, output_path)?;
    println!("Created clean file: {}", output_filename);
    println!("Removed {} duplicate rows ({:.1}% of data)", 
             batch.num_rows() - clean_batch.num_rows(),
             ((batch.num_rows() - clean_batch.num_rows()) as f64 / batch.num_rows() as f64) * 100.0);
    
    Ok(())
}
"""
    
    # Write test code
    with open("src/bin/test_major_group_duplicates.rs", "w") as f:
        f.write(test_code)
    
    # Compile and run
    print("\nCompiling and running duplicate detection test...")
    result = subprocess.run(
        ["cargo", "run", "--bin", "test_major_group_duplicates", "--release"],
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.stderr and "warning" not in result.stderr.lower():
        print("STDERR:", result.stderr)
    
    return result.returncode == 0

if __name__ == "__main__":
    success = test_major_group_duplicates()
    if success:
        print("\n✅ Major group duplicate detection test completed successfully!")
    else:
        print("\n❌ Test failed!")