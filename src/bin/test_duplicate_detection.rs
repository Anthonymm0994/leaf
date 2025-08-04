use leaf::core::{Database, duplicate_detector::{DuplicateDetector, DuplicateDetectionConfig}};
use std::collections::HashSet;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Testing Duplicate Detection ===");
    
    // Open database
    let mut db = Database::open_writable(".")?;
    
    // Import test CSV
    let csv_path = Path::new("test_duplicate_groups.csv");
    if !csv_path.exists() {
        println!("Error: test_duplicate_groups.csv not found. Run test_duplicate_detection_proper.py first.");
        return Ok(());
    }
    
    println!("Importing test_duplicate_groups.csv...");
    db.stream_insert_csv_with_header_row(
        "test_duplicate_groups",
        csv_path,
        ',',
        0
    )?;
    
    // Get the table data
    let batch = db.get_table_arrow_batch("test_duplicate_groups")?;
    println!("Loaded {} rows", batch.num_rows());
    
    // Configure duplicate detection
    let mut ignore_columns = HashSet::new();
    ignore_columns.insert("good_time".to_string());
    ignore_columns.insert("dumb_time".to_string());
    
    let config = DuplicateDetectionConfig {
        group_column: "group_id".to_string(),
        ignore_columns,
        null_equals_null: true,
    };
    
    // Run detection
    let detector = DuplicateDetector::new(config);
    let result = detector.detect_duplicates(&batch)?;
    
    // Print results
    println!("\n=== Detection Results ===");
    println!("Total duplicate groups found: {}", result.total_duplicates);
    println!("Total duplicate rows: {}", result.total_duplicate_rows);
    println!("\nDuplicate groups:");
    
    for (i, group) in result.duplicate_groups.iter().enumerate() {
        println!("\nGroup {}: group_id={}", i + 1, group.group_id);
        println!("  Occurrences: {}", group.row_indices.len());
        println!("  Rows per occurrence: {}", group.group_size);
        println!("  Total rows: {}", group.row_indices.len() * group.group_size);
        
        // Show row ranges
        for (j, rows) in group.row_indices.iter().enumerate() {
            if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
                println!("  Occurrence {}: rows {}-{}", j + 1, first, last);
            }
        }
    }
    
    // Test creating clean file
    println!("\n=== Testing Export ===");
    let output_path = Path::new("test_duplicate_blocks_clean.arrow");
    let (path, kept_rows) = detector.create_clean_arrow_file_with_path(
        &batch,
        &result,
        Path::new("."),
        "test_duplicate_groups"
    )?;
    
    println!("Created clean file: {:?}", path);
    println!("Kept {} rows (removed {} duplicates)", 
        kept_rows, 
        batch.num_rows() - kept_rows
    );
    
    // Verify the clean file
    db.load_table_arrow_ipc("test_clean", &path)?;
    let clean_batch = db.get_table_arrow_batch("test_clean")?;
    println!("\nClean file has {} rows", clean_batch.num_rows());
    
    println!("\n✓ Test completed successfully!");
    
    Ok(())
}