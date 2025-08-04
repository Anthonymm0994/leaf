use leaf::core::database::Database;
use leaf::core::time_grouping::{TimeBinConfig, TimeBinStrategy, TimeGroupingProcessor};
use leaf::core::error::LeafError;
use std::sync::Arc;
use std::path::Path;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::TimeUnit;

fn main() -> Result<(), LeafError> {
    println!("=== Comprehensive Time Binning Validation ===\n");
    
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
    
    // Get column info
    let columns = db.get_table_columns(table_name)?;
    println!("\nAvailable columns:");
    for (name, dtype) in &columns {
        println!("  - {}: {:?}", name, dtype);
    }
    
    // Test different time column types
    let time_columns = vec![
        ("good_time", "HH:MM:SS.sss format, non-nullable"),
        ("dumb_time", "HH:MM:SS.sss format, nullable"),
        ("timeseconds_infer_blank", "Time in seconds, nullable"),
        ("timemilliseconds_infer_dash", "Time in milliseconds, non-nullable"),
        ("datetime_infer_blank", "Full datetime, nullable"),
    ];
    
    let strategies = vec![
        (TimeBinStrategy::FixedInterval { 
            interval: 10, 
            interval_format: "seconds".to_string() 
        }, "10 second bins"),
        (TimeBinStrategy::FixedInterval { 
            interval: 60, 
            interval_format: "seconds".to_string() 
        }, "1 minute bins"),
        (TimeBinStrategy::ThresholdBased { 
            threshold: 30, 
            threshold_format: "seconds".to_string() 
        }, "30 second threshold"),
    ];
    
    let mut results = Vec::new();
    
    for (col_name, col_desc) in &time_columns {
        if !columns.iter().any(|(name, _)| name == col_name) {
            println!("\nSkipping {} - column not found", col_name);
            continue;
        }
        
        println!("\n\nTesting column: {} ({})", col_name, col_desc);
        println!("-" * 60);
        
        // Check null count
        let null_check_query = format!(
            "SELECT COUNT(*) FROM {} WHERE {} IS NULL OR {} = ''",
            table_name, col_name, col_name
        );
        let null_count: i64 = db.execute_query(&null_check_query)?[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        
        println!("Null/empty values: {}", null_count);
        
        // Get sample values
        let sample_query = format!(
            "SELECT {} FROM {} WHERE {} IS NOT NULL AND {} != '' LIMIT 5",
            col_name, table_name, col_name, col_name
        );
        let samples = db.execute_query(&sample_query)?;
        
        if !samples.is_empty() && samples[0].num_rows() > 0 {
            println!("Sample values:");
            let col = samples[0].column(0);
            for i in 0..col.len().min(5) {
                if let Some(val) = col.as_any().downcast_ref::<StringArray>() {
                    println!("  - {}", val.value(i));
                } else if let Some(val) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                    println!("  - {}", val.value_as_datetime(i).unwrap());
                }
            }
        }
        
        // Test each strategy
        for (strategy, strategy_desc) in &strategies {
            println!("\n  Strategy: {}", strategy_desc);
            
            let config = TimeBinConfig {
                selected_table: table_name.to_string(),
                selected_column: col_name.to_string(),
                strategy: strategy.clone(),
                output_column_name: format!("{}_bin", col_name),
                output_filename: Some(format!("{}_{}_bins", table_name, col_name)),
            };
            
            match TimeGroupingProcessor::new().apply_grouping(
                &db,
                &config,
                Path::new("test_outputs")
            ) {
                Ok(output_table) => {
                    // Validate the output
                    let bin_count_query = format!(
                        "SELECT {}, COUNT(*) as cnt FROM {} GROUP BY {} ORDER BY cnt DESC LIMIT 10",
                        config.output_column_name, output_table, config.output_column_name
                    );
                    
                    match db.execute_query(&bin_count_query) {
                        Ok(bin_results) => {
                            if !bin_results.is_empty() && bin_results[0].num_rows() > 0 {
                                let total_bins = db.execute_query(&format!(
                                    "SELECT COUNT(DISTINCT {}) FROM {}",
                                    config.output_column_name, output_table
                                ))?[0].column(0)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .value(0);
                                
                                println!("    ✓ Success: Created {} bins", total_bins);
                                
                                // Show top bins
                                println!("    Top bins:");
                                let bins = bin_results[0].column(0);
                                let counts = bin_results[0].column(1)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap();
                                
                                for i in 0..bins.len().min(3) {
                                    if let Some(bin_val) = bins.as_any().downcast_ref::<StringArray>() {
                                        println!("      - {}: {} rows", bin_val.value(i), counts.value(i));
                                    }
                                }
                                
                                results.push((
                                    col_name.to_string(),
                                    strategy_desc.to_string(),
                                    "Success".to_string(),
                                    format!("{} bins created", total_bins)
                                ));
                            }
                        }
                        Err(e) => {
                            println!("    ⚠ Warning: Could not analyze bins - {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("    ✗ Failed: {}", e);
                    results.push((
                        col_name.to_string(),
                        strategy_desc.to_string(),
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
        for (col, strategy, status, msg) in &results {
            if status == "Failed" {
                println!("  - {} + {}: {}", col, strategy, msg);
            }
        }
    }
    
    Ok(())
}