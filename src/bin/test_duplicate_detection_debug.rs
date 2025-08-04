use leaf::core::{Database, duplicate_detector::{DuplicateDetector, DuplicateDetectionConfig}};
use std::collections::{HashSet, HashMap};
use std::path::Path;
use datafusion::arrow::array::StringArray;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Testing Duplicate Detection (Debug) ===");
    
    // Open database
    let mut db = Database::open_writable(".")?;
    
    // Import test CSV
    let csv_path = Path::new("test_duplicate_groups.csv");
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
    
    // Manually analyze groups
    let group_col_idx = batch.schema()
        .fields()
        .iter()
        .position(|field| field.name() == "group_id")
        .unwrap();
    
    let group_col = batch.column(group_col_idx);
    let group_array = group_col.as_any().downcast_ref::<StringArray>().unwrap();
    
    // Group rows by group_id
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (row_idx, group_id) in group_array.iter().enumerate() {
        if let Some(group_id) = group_id {
            groups.entry(group_id.to_string()).or_insert_with(Vec::new).push(row_idx);
        }
    }
    
    println!("\nGroups found: {}", groups.len());
    
    // Analyze content of each group
    let non_time_cols: Vec<_> = batch.schema().fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.name().contains("time") && field.name() != "group_id")
        .map(|(idx, field)| (idx, field.name().clone()))
        .collect();
    
    println!("\nNon-time columns: {:?}", non_time_cols.iter().map(|(_, name)| name).collect::<Vec<_>>());
    
    // Create signatures for each group
    let mut group_signatures: HashMap<String, String> = HashMap::new();
    
    for (group_id, row_indices) in &groups {
        if let Some(&first_row) = row_indices.first() {
            let mut sig_parts = Vec::new();
            
            for (col_idx, col_name) in &non_time_cols {
                let value = leaf::core::database::Database::array_value_to_string(
                    batch.column(*col_idx),
                    first_row,
                    batch.schema().field(*col_idx).data_type()
                ).unwrap_or_else(|_| "ERROR".to_string());
                
                sig_parts.push(format!("{}={}", col_name, value));
            }
            
            let signature = sig_parts.join(", ");
            group_signatures.insert(group_id.clone(), signature);
        }
    }
    
    // Find groups with identical signatures
    let mut signature_to_groups: HashMap<String, Vec<String>> = HashMap::new();
    for (group_id, signature) in &group_signatures {
        signature_to_groups.entry(signature.clone()).or_insert_with(Vec::new).push(group_id.clone());
    }
    
    println!("\nGroups with identical content:");
    for (signature, group_ids) in &signature_to_groups {
        if group_ids.len() > 1 {
            println!("\nDuplicate found:");
            println!("  Groups: {:?}", group_ids);
            println!("  Signature: {}", signature);
        }
    }
    
    // Now run the actual detector
    println!("\n=== Running Actual Detector ===");
    
    let mut ignore_columns = HashSet::new();
    ignore_columns.insert("good_time".to_string());
    ignore_columns.insert("dumb_time".to_string());
    
    let config = DuplicateDetectionConfig {
        group_column: "group_id".to_string(),
        ignore_columns,
        null_equals_null: true,
    };
    
    let detector = DuplicateDetector::new(config);
    let result = detector.detect_duplicates(&batch)?;
    
    println!("Total duplicate groups found: {}", result.total_duplicates);
    println!("Total duplicate rows: {}", result.total_duplicate_rows);
    
    Ok(())
}