use leaf::core::database::Database;
use leaf::core::enhanced_grouping_processor::{
    EnhancedGroupingProcessor, EnhancedGroupingRequest, GroupingConfiguration, GroupingRule
};
use leaf::core::error::LeafError;
use std::sync::Arc;
use std::path::Path;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;

fn main() -> Result<(), LeafError> {
    println!("=== Comprehensive Group ID Validation ===\n");
    
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
    
    // Get all columns and categorize them
    let columns = db.get_table_columns(table_name)?;
    
    // Test columns from different types
    let test_columns = vec![
        // Text columns
        ("category_3", "text", "categorical text"),
        ("text_infer_blank", "text", "nullable text"),
        ("tags", "text", "nullable tags"),
        
        // Numeric columns
        ("width", "numeric", "non-nullable float"),
        ("integer_infer_blank", "numeric", "nullable integer"),
        
        // Boolean columns  
        ("isGood", "boolean", "non-nullable boolean"),
        ("boolean_infer_blank", "boolean", "nullable boolean"),
        
        // Time columns
        ("good_time", "time", "non-nullable time"),
        ("dumb_time", "time", "nullable time"),
        
        // Date columns
        ("date_infer_blank", "date", "nullable date"),
    ];
    
    let rules = vec![
        (GroupingRule::ValueChange, false, "Value Change"),
        (GroupingRule::ValueChange, true, "Value Change (Reset)"),
        (GroupingRule::IsEmpty, false, "Is Empty"),
        (GroupingRule::IsEmpty, true, "Is Empty (Reset)"),
        (GroupingRule::ValueEquals("a".to_string()), false, "Value Equals 'a'"),
    ];
    
    let mut results = Vec::new();
    let processor = EnhancedGroupingProcessor::new();
    
    for (col_name, col_type, col_desc) in &test_columns {
        if !columns.iter().any(|(name, _)| name == col_name) {
            println!("\nSkipping {} - column not found", col_name);
            continue;
        }
        
        println!("\n\nTesting column: {} ({}, {})", col_name, col_type, col_desc);
        println!("-" * 60);
        
        // Check null count
        let null_check_query = format!(
            "SELECT COUNT(*) FROM {} WHERE {} IS NULL OR CAST({} AS VARCHAR) = ''",
            table_name, col_name, col_name
        );
        let null_count: i64 = db.execute_query(&null_check_query)?[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        
        let is_nullable = null_count > 0;
        println!("Nullable: {} (null/empty count: {})", is_nullable, null_count);
        
        // Get sample values
        let sample_query = format!(
            "SELECT DISTINCT {} FROM {} WHERE {} IS NOT NULL AND CAST({} AS VARCHAR) != '' LIMIT 5",
            col_name, table_name, col_name, col_name
        );
        
        if let Ok(samples) = db.execute_query(&sample_query) {
            if !samples.is_empty() && samples[0].num_rows() > 0 {
                println!("Sample values:");
                let col = samples[0].column(0);
                
                // Print based on type
                for i in 0..col.len().min(5) {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        if arr.is_valid(i) {
                            println!("  - '{}'", arr.value(i));
                        }
                    } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        if arr.is_valid(i) {
                            println!("  - {}", arr.value(i));
                        }
                    } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        if arr.is_valid(i) {
                            println!("  - {:.3}", arr.value(i));
                        }
                    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        if arr.is_valid(i) {
                            println!("  - {}", arr.value(i));
                        }
                    }
                }
            }
        }
        
        // Test each rule
        for (rule, reset, rule_desc) in &rules {
            // Skip empty rules for non-nullable columns
            if matches!(rule, GroupingRule::IsEmpty) && !is_nullable {
                println!("\n  Skipping {} - column is non-nullable", rule_desc);
                continue;
            }
            
            println!("\n  Rule: {}", rule_desc);
            
            let config = GroupingConfiguration {
                column_name: col_name.to_string(),
                rule: rule.clone(),
                reset_on_change: *reset,
                block_id_name: format!("{}_block_id", col_name),
                group_id_name: format!("{}_group_id", col_name),
                row_id_name: format!("{}_row_id", col_name),
            };
            
            let request = EnhancedGroupingRequest {
                table_name: table_name.to_string(),
                configurations: vec![config.clone()],
                output_filename: Some(format!("{}_{}_groupid", table_name, col_name)),
            };
            
            match processor.process_request(&db, &request, Path::new("test_outputs")) {
                Ok(output_table) => {
                    // Validate the output
                    let validation_query = format!(
                        "SELECT COUNT(DISTINCT {}), COUNT(DISTINCT {}), MAX({}) FROM {}",
                        config.block_id_name, config.group_id_name, config.row_id_name,
                        output_table
                    );
                    
                    match db.execute_query(&validation_query) {
                        Ok(stats) => {
                            if !stats.is_empty() && stats[0].num_rows() > 0 {
                                let block_count = stats[0].column(0).as_any()
                                    .downcast_ref::<Int64Array>().unwrap().value(0);
                                let group_count = stats[0].column(1).as_any()
                                    .downcast_ref::<Int64Array>().unwrap().value(0);
                                let max_row_id = stats[0].column(2).as_any()
                                    .downcast_ref::<Int64Array>().unwrap().value(0);
                                
                                println!("    ✓ Success: {} blocks, {} groups, max row_id={}",
                                    block_count, group_count, max_row_id);
                                
                                // Show sample of the grouping
                                let sample_query = format!(
                                    "SELECT {}, {}, {}, {} FROM {} LIMIT 10",
                                    col_name, config.block_id_name, config.group_id_name, 
                                    config.row_id_name, output_table
                                );
                                
                                if let Ok(sample_results) = db.execute_query(&sample_query) {
                                    if !sample_results.is_empty() && sample_results[0].num_rows() > 0 {
                                        println!("    Sample rows:");
                                        let batch = &sample_results[0];
                                        
                                        for i in 0..batch.num_rows().min(5) {
                                            let val = if let Some(arr) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                                                if arr.is_valid(i) { arr.value(i) } else { "<null>" }
                                            } else {
                                                "<value>"
                                            };
                                            
                                            let block_id = batch.column(1).as_any()
                                                .downcast_ref::<Int64Array>().unwrap().value(i);
                                            let group_id = batch.column(2).as_any()
                                                .downcast_ref::<Int64Array>().unwrap().value(i);
                                            let row_id = batch.column(3).as_any()
                                                .downcast_ref::<Int64Array>().unwrap().value(i);
                                            
                                            println!("      {} -> block={}, group={}, row={}",
                                                val, block_id, group_id, row_id);
                                        }
                                    }
                                }
                                
                                results.push((
                                    col_name.to_string(),
                                    rule_desc.to_string(),
                                    "Success".to_string(),
                                    format!("{} blocks, {} groups", block_count, group_count)
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
                        rule_desc.to_string(),
                        "Failed".to_string(),
                        e.to_string()
                    ));
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
        for (col, rule, status, msg) in &results {
            if status == "Failed" {
                println!("  - {} + {}: {}", col, rule, msg);
            }
        }
    }
    
    println!("\n✓ Validation complete!");
    
    Ok(())
}