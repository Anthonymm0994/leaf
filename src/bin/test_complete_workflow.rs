use leaf::core::database::Database;
use leaf::core::transformations::DataTransformer;
use leaf::core::time_grouping::TimeGrouper;
use leaf::core::computed_columns_processor::{ComputedColumnsProcessor, ComputedColumnType};
use std::path::Path;
use datafusion::arrow::datatypes::DataType;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing complete workflow: Import -> Transform -> Time Bin");
    println!("{}", "=".repeat(80));
    
    // Create a new database
    let mut db = Database::open_writable(".")?;
    
    // Import the CSV file
    let csv_path = Path::new("data_gen_scripts/test_data_300k.csv");
    println!("\n1. Importing CSV: {:?}", csv_path);
    
    let table_name = "test_workflow";
    let delimiter = db.stream_insert_csv_with_header_row(table_name, csv_path, ',', 0)?;
    println!("   Detected delimiter: '{}'", delimiter);
    
    // Check the schema
    println!("\n2. Checking imported schema:");
    let schema_query = format!("SELECT * FROM {} LIMIT 1", table_name);
    let types = db.get_column_types(&schema_query)?;
    let names = db.get_column_names(&schema_query)?;
    
    // Find datetime and time columns
    let mut datetime_cols = Vec::new();
    let mut time_cols = Vec::new();
    
    for (name, dtype) in names.iter().zip(types.iter()) {
        if matches!(dtype, DataType::Timestamp(_, _)) {
            if name.contains("datetime") {
                datetime_cols.push(name.clone());
            } else if name.contains("time") {
                time_cols.push(name.clone());
            }
        }
    }
    
    println!("   Found {} datetime columns: {:?}", datetime_cols.len(), datetime_cols);
    println!("   Found {} time columns: {:?}", time_cols.len(), time_cols);
    
    // Test datetime values
    if !datetime_cols.is_empty() {
        println!("\n3. Testing datetime column values:");
        let col = &datetime_cols[0];
        let query = format!("SELECT {} FROM {} WHERE {} IS NOT NULL LIMIT 5", col, table_name, col);
        let results = db.execute_query(&query)?;
        
        println!("   {} sample values:", col);
        for (i, row) in results.iter().enumerate() {
            if !row.is_empty() {
                println!("     Row {}: '{}'", i, row[0]);
            }
        }
    }
    
    // Test time binning on a time column
    if !time_cols.is_empty() {
        println!("\n4. Testing time binning:");
        let time_col = &time_cols[0];
        
        // Create a new table with time bins
        let binned_table = format!("{}_binned", table_name);
        let batch = db.load_table_arrow_batch(table_name)?;
        
        // Add time bin column
        let time_grouper = TimeGrouper::new();
        let bin_col_name = format!("{}_bin", time_col);
        
        println!("   Creating time bins for column: {}", time_col);
        
        // Use fixed interval of 1 hour
        match time_grouper.add_time_bin_column(
            batch.clone(),
            time_col,
            &bin_col_name,
            "fixed",
            Some("01:00:00".to_string()),
            None,
            None
        ) {
            Ok(new_batch) => {
                db.create_table_from_record_batch(&binned_table, new_batch)?;
                println!("   Successfully created time bins!");
                
                // Check the results
                let query = format!("SELECT {}, {} FROM {} LIMIT 10", time_col, bin_col_name, binned_table);
                let results = db.execute_query(&query)?;
                
                println!("   Sample results:");
                for (i, row) in results.iter().enumerate().take(5) {
                    if row.len() >= 2 {
                        println!("     {}: {} -> bin {}", i, row[0], row[1]);
                    }
                }
            }
            Err(e) => {
                println!("   Error creating time bins: {}", e);
            }
        }
    }
    
    // Test computed columns
    println!("\n5. Testing computed columns:");
    
    // Find a numeric column
    let mut numeric_col = None;
    for (name, dtype) in names.iter().zip(types.iter()) {
        if matches!(dtype, DataType::Float64 | DataType::Int64) && name.contains("normal") {
            numeric_col = Some(name.clone());
            break;
        }
    }
    
    if let Some(col) = numeric_col {
        println!("   Adding z-score for column: {}", col);
        
        let processor = ComputedColumnsProcessor::new();
        let zscore_table = format!("{}_zscore", table_name);
        let batch = db.load_table_arrow_batch(table_name)?;
        
        match processor.add_computed_column(
            batch,
            &col,
            &format!("{}_zscore", col),
            ComputedColumnType::ZScore
        ) {
            Ok(new_batch) => {
                db.create_table_from_record_batch(&zscore_table, new_batch)?;
                println!("   Successfully added z-score column!");
                
                // Check results
                let query = format!("SELECT {}, {}_zscore FROM {} LIMIT 5", col, col, zscore_table);
                let results = db.execute_query(&query)?;
                
                println!("   Sample results:");
                for (i, row) in results.iter().enumerate() {
                    if row.len() >= 2 {
                        println!("     {}: {} -> z-score {}", i, row[0], row[1]);
                    }
                }
            }
            Err(e) => {
                println!("   Error adding z-score: {}", e);
            }
        }
    }
    
    println!("\n6. Test completed successfully!");
    
    Ok(())
}