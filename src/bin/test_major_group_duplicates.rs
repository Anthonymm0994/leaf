use leaf::core::{Database, duplicate_detector::{DuplicateDetector, DuplicateDetectionConfig}};
use std::sync::Arc;
use std::path::Path;
use std::collections::HashSet;

fn main() -> anyhow::Result<()> {
    println!("\nTesting duplicate detection on major groups...");
    let db = Arc::new(Database::open_writable(".")?);
    
    // Import the CSV with major group IDs
    let csv_path = Path::new("test_data_300k_with_groups.csv");
    let table_name = "test_data_300k_with_groups";
    db.import_csv(csv_path, table_name)?;
    println!("Imported table: {}", table_name);
    
    // Get the record batch
    let batch = db.get_table_arrow_batch(table_name)?;
    println!("Loaded {} rows", batch.num_rows());
    
    // Configure duplicate detection for major groups
    let mut ignore_cols = HashSet::new();
    ignore_cols.insert("good_time".to_string());
    ignore_cols.insert("dumb_time".to_string());
    ignore_cols.insert("major_group_id".to_string());
    
    let config = DuplicateDetectionConfig {
        group_column: "major_group_id".to_string(),
        ignore_columns: ignore_cols,
        null_equals_null: true,
    };
    
    let detector = DuplicateDetector::new(config);
    
    println!("\nRunning duplicate detection...");
    let result = detector.detect_duplicates(&batch)?;
    
    println!("\n=== Detection Results ===");
    println!("Total duplicate groups found: {}", result.duplicate_groups.len());
    println!("Total duplicate rows: {}", result.total_duplicate_rows);
    
    // Calculate percentages
    let total_major_groups = 855; // From our analysis
    let duplicate_major_groups = result.duplicate_groups.len();
    let duplicate_percentage = (duplicate_major_groups as f64 / total_major_groups as f64) * 100.0;
    
    println!("\nMajor group statistics:");
    println!("  Total major groups: {}", total_major_groups);
    println!("  Duplicate major groups: {}", duplicate_major_groups);
    println!("  Percentage with duplicates: {:.1}%", duplicate_percentage);
    
    // Count occurrences from row_indices
    let mut duplication_counts = std::collections::HashMap::new();
    for group_info in &result.duplicate_groups {
        let occurrences = group_info.row_indices.len();
        *duplication_counts.entry(occurrences).or_insert(0) += 1;
    }
    
    println!("\nDuplication patterns:");
    for (count, freq) in duplication_counts.iter() {
        let percentage = (*freq as f64 / total_major_groups as f64) * 100.0;
        println!("  {}x: {} groups ({:.1}%)", count, freq, percentage);
    }
    
    // Show examples
    if result.duplicate_groups.len() > 0 {
        println!("\nExample duplicate major groups (first 3):");
        for (i, group_info) in result.duplicate_groups.iter().take(3).enumerate() {
            println!("\nGroup {}: major_group_id={}", i + 1, group_info.group_id);
            println!("  Occurrences: {}", group_info.row_indices.len());
            println!("  Rows per occurrence: {}", group_info.group_size);
            println!("  Total rows: {}", group_info.row_indices.len() * group_info.group_size);
            
            // Show first few occurrences
            for (j, rows) in group_info.row_indices.iter().take(2).enumerate() {
                if let Some(first_row) = rows.first() {
                    println!("  Occurrence {}: rows {}-{}", 
                             j + 1,
                             first_row, 
                             first_row + group_info.group_size - 1);
                }
            }
        }
    }
    
    // Export clean data
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let output_filename = format!("test_data_300k_no_duplicates_{}.arrow", timestamp);
    let output_path = Path::new(&output_filename);
    
    println!("\n=== Exporting Clean Data ===");
    let clean_rows = detector.create_clean_arrow_file(&batch, table_name, output_path)?;
    println!("Created clean file: {}", output_filename);
    println!("Kept {} rows (removed {} duplicate rows, {:.1}% of data)", 
             clean_rows,
             batch.num_rows() - clean_rows,
             ((batch.num_rows() - clean_rows) as f64 / batch.num_rows() as f64) * 100.0);
    
    Ok(())
}