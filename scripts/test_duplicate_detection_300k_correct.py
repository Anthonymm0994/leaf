#!/usr/bin/env python3
"""
Test duplicate detection on the correctly generated 300k dataset.
This dataset has major groups (sequences) that are duplicated, not individual rows.
"""

import subprocess
import pandas as pd
import os
from datetime import datetime

def test_duplicate_detection():
    """Test the duplicate detection on 300k dataset."""
    print("=== Testing Duplicate Detection on 300k Dataset ===\n")
    
    # First, import the CSV into the database
    print("1. Importing test_data_300k_correct.csv...")
    result = subprocess.run(
        ["cargo", "run", "--bin", "leaf", "--release"],
        capture_output=True,
        text=True
    )
    print("   (Using existing Leaf instance to import)\n")
    
    # Now run the duplicate detection test
    print("2. Running duplicate detection test...")
    
    # Create a test script that will:
    # - Load the 300k dataset
    # - Use good_time as the group column (since mini groups share good_time)
    # - Ignore time columns
    # - Export clean data
    
    test_code = """
use leaf::core::{Database, duplicate_detector::{DuplicateDetector, DuplicateDetectionConfig}};
use std::sync::Arc;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    println!("Loading database and importing CSV...");
    let db = Arc::new(Database::new()?);
    
    // Import the CSV
    let csv_path = Path::new("test_data_300k_correct.csv");
    let table_name = db.import_csv_as_table(csv_path)?;
    println!("Imported as table: {}", table_name);
    
    // Get the record batch
    let batch = db.get_table_arrow_batch(&table_name)?;
    println!("Loaded {} rows", batch.num_rows());
    
    // Configure duplicate detection
    // Since we want to detect duplicate major groups, we need a column that identifies major groups
    // We'll use row index ranges for this in the analysis
    
    // For now, let's detect based on all non-time columns being identical
    let config = DuplicateDetectionConfig {
        group_column: "good_time".to_string(), // Use good_time as group identifier
        ignore_columns: vec!["good_time".to_string(), "dumb_time".to_string()],
        null_equals_null: true,
    };
    
    let detector = DuplicateDetector::new(config);
    
    println!("\\nRunning duplicate detection...");
    let result = detector.detect_duplicates(&batch)?;
    
    println!("\\n=== Detection Results ===");
    println!("Total duplicate groups found: {}", result.duplicate_groups.len());
    println!("Total duplicate rows: {}", result.total_duplicate_rows);
    println!("Percentage of data that is duplicate: {:.1}%", 
             (result.total_duplicate_rows as f64 / batch.num_rows() as f64) * 100.0);
    
    // Show some examples
    if result.duplicate_groups.len() > 0 {
        println!("\\nSample duplicate groups (first 5):");
        for (i, (group_hash, group_info)) in result.duplicate_groups.iter().take(5).enumerate() {
            println!("\\nGroup {}: hash={}", i + 1, group_hash);
            println!("  Group ID: {}", group_info.group_id);
            println!("  Occurrences: {}", group_info.occurrences);
            println!("  Rows per occurrence: {}", group_info.group_size);
            println!("  Total duplicate rows: {}", group_info.total_rows);
        }
    }
    
    // Export clean data
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let output_filename = format!("test_data_300k_clean_{}.arrow", timestamp);
    let output_path = Path::new(&output_filename);
    
    println!("\\n=== Exporting Clean Data ===");
    let clean_batch = detector.create_clean_arrow_file(&batch, &table_name, output_path)?;
    println!("Created clean file: {}", output_filename);
    println!("Clean file has {} rows (removed {} rows)", 
             clean_batch.num_rows(), 
             batch.num_rows() - clean_batch.num_rows());
    
    Ok(())
}
"""
    
    # Write the test code
    with open("src/bin/test_300k_duplicates.rs", "w") as f:
        f.write(test_code)
    
    # Run the test
    result = subprocess.run(
        ["cargo", "run", "--bin", "test_300k_duplicates", "--release"],
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    return result.returncode == 0

if __name__ == "__main__":
    success = test_duplicate_detection()
    if success:
        print("\n✅ Test completed successfully!")
    else:
        print("\n❌ Test failed!")