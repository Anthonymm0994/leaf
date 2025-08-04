use leaf::core::database::Database;
use leaf::core::computed_columns_processor::{
    ComputedColumnsProcessor, ComputedColumnsRequest, ColumnConfiguration, ComputationType
};
use leaf::core::error::LeafError;
use std::sync::Arc;
use std::path::Path;
use datafusion::arrow::array::*;

fn main() -> Result<(), LeafError> {
    println!("=== Comprehensive Computed Columns Validation ===\n");
    
    // Load the test data
    let db = Arc::new(Database::open_read_only()?);
    let test_file = Path::new("data_gen_scripts/test_data_300k.csv");
    
    if !test_file.exists() {
        return Err(LeafError::Custom("Test file not found. Please generate test data first.".to_string()));
    }
    
    // Import the CSV
    let table_name = "test_data_300k";
    db.import_csv(test_file, table_name)?;
    
    println!("Loaded test data: {} rows", db.execute_query(&format!("SELECT COUNT(*) FROM {}", table_name))?[0].get(0).unwrap());
    
    // Get numeric columns
    let columns = db.get_table_columns(table_name)?;
    let numeric_columns: Vec<(String, bool)> = columns.iter()
        .filter_map(|(name, dtype)| {
            // Check if it's a numeric type
            let is_numeric = match dtype {
                datafusion::arrow::datatypes::DataType::Int8 |
                datafusion::arrow::datatypes::DataType::Int16 |
                datafusion::arrow::datatypes::DataType::Int32 |
                datafusion::arrow::datatypes::DataType::Int64 |
                datafusion::arrow::datatypes::DataType::UInt8 |
                datafusion::arrow::datatypes::DataType::UInt16 |
                datafusion::arrow::datatypes::DataType::UInt32 |
                datafusion::arrow::datatypes::DataType::UInt64 |
                datafusion::arrow::datatypes::DataType::Float32 |
                datafusion::arrow::datatypes::DataType::Float64 => true,
                _ => false
            };
            
            if is_numeric || name.contains("integer_") || name.contains("real_") || 
               name == "width" || name == "height" || name == "angle" ||
               name.contains("bimodal") || name.contains("exponential") || 
               name.contains("uniform") || name.contains("normal") || name.contains("linear") {
                // Check if nullable
                let null_check_query = format!(
                    "SELECT COUNT(*) FROM {} WHERE {} IS NULL OR CAST({} AS VARCHAR) = ''",
                    table_name, name, name
                );
                
                if let Ok(results) = db.execute_query(&null_check_query) {
                    if let Some(count_array) = results[0].column(0).as_any().downcast_ref::<Int64Array>() {
                        let null_count = count_array.value(0);
                        Some((name.clone(), null_count > 0))
                    } else {
                        Some((name.clone(), false))
                    }
                } else {
                    Some((name.clone(), false))
                }
            } else {
                None
            }
        })
        .collect();
    
    println!("\nNumeric columns found: {}", numeric_columns.len());
    for (name, is_nullable) in &numeric_columns {
        println!("  - {} (nullable: {})", name, is_nullable);
    }
    
    // Test different computation types
    let computation_types = vec![
        (ComputationType::Delta, "Delta (row-to-row difference)"),
        (ComputationType::CumulativeSum, "Cumulative Sum"),
        (ComputationType::Percentage, "Percentage of Total"),
        (ComputationType::MovingAverage { window_size: 5 }, "Moving Average (5)"),
        (ComputationType::ZScore, "Z-Score"),
    ];
    
    let mut results = Vec::new();
    let processor = ComputedColumnsProcessor::new();
    
    // Test a representative sample of columns
    let test_columns = vec![
        "width",               // Non-nullable float
        "integer_infer_blank", // Nullable integer  
        "real_infer_dash",     // Non-nullable real
        "bimodal",            // Non-nullable distribution
        "linear_over_time",   // Non-nullable distribution
    ];
    
    for col_name in test_columns {
        if let Some((_, is_nullable)) = numeric_columns.iter().find(|(name, _)| name == col_name) {
            println!("\n\nTesting column: {} (nullable: {})", col_name, is_nullable);
            println!("-" * 60);
            
            // Get sample values
            let sample_query = format!(
                "SELECT {} FROM {} WHERE {} IS NOT NULL LIMIT 5",
                col_name, table_name, col_name
            );
            
            if let Ok(samples) = db.execute_query(&sample_query) {
                if !samples.is_empty() && samples[0].num_rows() > 0 {
                    println!("Sample values:");
                    let col = samples[0].column(0);
                    
                    // Print based on type
                    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        for i in 0..col.len().min(5) {
                            if arr.is_valid(i) {
                                println!("  - {:.3}", arr.value(i));
                            }
                        }
                    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        for i in 0..col.len().min(5) {
                            if arr.is_valid(i) {
                                println!("  - {}", arr.value(i));
                            }
                        }
                    }
                }
            }
            
            // Test each computation type
            for (comp_type, comp_desc) in &computation_types {
                println!("\n  Computation: {}", comp_desc);
                
                let config = ColumnConfiguration {
                    source_column: col_name.to_string(),
                    computation_type: comp_type.clone(),
                    output_column_name: format!("{}_{}", col_name, match comp_type {
                        ComputationType::Delta => "delta",
                        ComputationType::CumulativeSum => "cumsum",
                        ComputationType::Percentage => "pct",
                        ComputationType::MovingAverage { .. } => "ma5",
                        ComputationType::ZScore => "zscore",
                        _ => "computed",
                    }),
                };
                
                let request = ComputedColumnsRequest {
                    table_name: table_name.to_string(),
                    configurations: vec![config.clone()],
                    output_filename: Some(format!("{}_{}_{}", 
                        table_name, 
                        col_name,
                        match comp_type {
                            ComputationType::Delta => "delta",
                            ComputationType::CumulativeSum => "cumsum",
                            ComputationType::Percentage => "pct",
                            ComputationType::MovingAverage { .. } => "ma5",
                            ComputationType::ZScore => "zscore",
                            _ => "computed",
                        }
                    )),
                };
                
                match processor.process_request(&db, &request, Path::new("test_outputs")) {
                    Ok(output_table) => {
                        // Validate the output
                        let validation_query = format!(
                            "SELECT COUNT(*), COUNT({}), MIN({}), MAX({}), AVG({}) FROM {}",
                            config.output_column_name, config.output_column_name,
                            config.output_column_name, config.output_column_name,
                            output_table
                        );
                        
                        match db.execute_query(&validation_query) {
                            Ok(stats) => {
                                if !stats.is_empty() && stats[0].num_rows() > 0 {
                                    let total_rows = stats[0].column(0).as_any()
                                        .downcast_ref::<Int64Array>().unwrap().value(0);
                                    let non_null_rows = stats[0].column(1).as_any()
                                        .downcast_ref::<Int64Array>().unwrap().value(0);
                                    
                                    println!("    ✓ Success: {} rows computed ({} non-null)",
                                        total_rows, non_null_rows);
                                    
                                    // Print stats if available
                                    if let Some(min_arr) = stats[0].column(2).as_any().downcast_ref::<Float64Array>() {
                                        if min_arr.is_valid(0) {
                                            let min_val = min_arr.value(0);
                                            let max_val = stats[0].column(3).as_any()
                                                .downcast_ref::<Float64Array>().unwrap().value(0);
                                            let avg_val = stats[0].column(4).as_any()
                                                .downcast_ref::<Float64Array>().unwrap().value(0);
                                            
                                            println!("    Stats: min={:.3}, max={:.3}, avg={:.3}",
                                                min_val, max_val, avg_val);
                                        }
                                    }
                                    
                                    results.push((
                                        col_name.to_string(),
                                        comp_desc.to_string(),
                                        "Success".to_string(),
                                        format!("{} rows computed", non_null_rows)
                                    ));
                                }
                            }
                            Err(e) => {
                                println!("    ⚠ Warning: Could not validate output - {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("    ✗ Failed: {}", e);
                        results.push((
                            col_name.to_string(),
                            comp_desc.to_string(),
                            "Failed".to_string(),
                            e.to_string()
                        ));
                    }
                }
            }
        }
    }
    
    // Summary
    println!("\n\n" + "=" * 80);
    println!("SUMMARY");
    println!("=" * 80);
    
    let total = results.len();
    let successful = results.iter().filter(|(_, _, status, _)| status == "Success").count();
    let failed = results.iter().filter(|(_, _, status, _)| status == "Failed").count();
    
    println!("Total tests: {}", total);
    println!("Successful: {} ({:.1}%)", successful, (successful as f64 / total as f64) * 100.0);
    println!("Failed: {} ({:.1}%)", failed, (failed as f64 / total as f64) * 100.0);
    
    if failed > 0 {
        println!("\nFailed tests:");
        for (col, comp, status, msg) in &results {
            if status == "Failed" {
                println!("  - {} + {}: {}", col, comp, msg);
            }
        }
    }
    
    println!("\n✓ Validation complete!");
    
    Ok(())
}