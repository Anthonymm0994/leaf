use anyhow::Result;
use leaf::core::{Database, EnhancedGroupingProcessor};
use leaf::ui::{EnhancedGroupingRequest, GroupingConfig, GroupingRule};

fn main() -> Result<()> {
    println!("Testing null preservation fix...");
    
    // Create database and load the original data
    let mut db = Database::open_writable(".")?;
    
    // Load the original test data
    let arrow_file = "test_data/test_data_300k.arrow";
    println!("Loading {}", arrow_file);
    db.load_table_arrow_ipc("test_data_300k", std::path::Path::new(arrow_file))?;
    
    // Get the batch to check original data
    let batch = db.load_table_arrow_batch("test_data_300k")?;
    println!("Loaded {} rows", batch.num_rows());
    
    // Check null counts in original data
    println!("\nOriginal null counts:");
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(i);
        let null_count = column.null_count();
        if null_count > 0 {
            println!("  {}: {} nulls", field.name(), null_count);
        }
    }
    
    // Create enhanced grouping request
    let request = EnhancedGroupingRequest {
        table_name: "test_data_300k".to_string(),
        configurations: vec![
            GroupingConfig {
                rule: GroupingRule::ValueChange { column: "good_time".to_string() },
                output_column: "good_time_group_id".to_string(),
                reset_on_change: false,
            },
            GroupingConfig {
                rule: GroupingRule::ValueChange { column: "dumb_time".to_string() },
                output_column: "dumb_time_group_id".to_string(),
                reset_on_change: false,
            },
        ],
        output_filename: Some("test_null_fix_output".to_string()),
    };
    
    // Process the request
    let processor = EnhancedGroupingProcessor::new();
    let output_dir = std::path::Path::new("test_data");
    
    match processor.process_request(&request, &db, output_dir) {
        Ok(output_file) => {
            println!("\nTransformation successful: {}", output_file);
            
            // Load the transformed file
            let output_path = output_dir.join(&output_file);
            db.load_table_arrow_ipc("transformed", &output_path)?;
            
            // Check the transformed data
            let transformed_batch = db.load_table_arrow_batch("transformed")?;
            println!("\nTransformed null counts:");
            for (i, field) in transformed_batch.schema().fields().iter().enumerate() {
                let column = transformed_batch.column(i);
                let null_count = column.null_count();
                if null_count > 0 {
                    println!("  {}: {} nulls", field.name(), null_count);
                }
            }
            
            // Check data types
            println!("\nTransformed data types:");
            for field in transformed_batch.schema().fields() {
                println!("  {}: {:?}", field.name(), field.data_type());
            }
        }
        Err(e) => {
            eprintln!("Transformation failed: {}", e);
        }
    }
    
    Ok(())
}