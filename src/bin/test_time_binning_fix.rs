use anyhow::Result;
use std::sync::Arc;
use leaf::core::{Database, TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};

fn main() -> Result<()> {
    println!("=== Testing Time Binning Fix ===\n");
    
    // Test with different time ranges
    test_3_hour_range()?;
    test_24_hour_range()?;
    test_with_actual_data()?;
    
    Ok(())
}

fn test_3_hour_range() -> Result<()> {
    println!("Test 1: 3-hour range data (10:00 to 13:00)");
    println!("-" .repeat(50));
    
    // Create test data
    let test_data = vec![
        vec!["10:00:00", "A"],
        vec!["10:30:00", "B"],
        vec!["11:15:00", "C"],
        vec!["11:45:00", "D"],
        vec!["12:00:00", "E"],
        vec!["12:30:00", "F"],
        vec!["13:00:00", "G"],
    ];
    
    let mut groups = Vec::new();
    
    // Test with 1-hour intervals
    TimeGroupingEngine::create_fixed_interval_groups(
        &test_data,
        0, // time column index
        3600, // 1 hour in seconds
        &mut groups
    )?;
    
    println!("Time values and their bins:");
    for (i, (row, bin)) in test_data.iter().zip(groups.iter()).enumerate() {
        println!("  {} -> Bin {}", row[0], bin);
    }
    
    println!("\nExpected: Bins 0, 0, 1, 1, 2, 2, 3");
    println!("Actual: Bins {:?}", groups);
    println!();
    
    Ok(())
}

fn test_24_hour_range() -> Result<()> {
    println!("Test 2: 24-hour range data");
    println!("-" .repeat(50));
    
    // Create test data spanning 24 hours
    let test_data = vec![
        vec!["00:00:00", "A"],
        vec!["01:00:00", "B"],
        vec!["06:00:00", "C"],
        vec!["12:00:00", "D"],
        vec!["18:00:00", "E"],
        vec!["23:59:59", "F"],
    ];
    
    let mut groups = Vec::new();
    
    // Test with 1-hour intervals
    TimeGroupingEngine::create_fixed_interval_groups(
        &test_data,
        0, // time column index
        3600, // 1 hour in seconds
        &mut groups
    )?;
    
    println!("Time values and their bins:");
    for (i, (row, bin)) in test_data.iter().zip(groups.iter()).enumerate() {
        println!("  {} -> Bin {}", row[0], bin);
    }
    
    println!("\nExpected: Bins 0, 1, 6, 12, 18, 23");
    println!("Actual: Bins {:?}", groups);
    println!();
    
    Ok(())
}

fn test_with_actual_data() -> Result<()> {
    println!("Test 3: Actual data from test_data_300k.csv");
    println!("-" .repeat(50));
    
    let mut db = Database::open_writable(".")?;
    
    // Check if we have the test data
    let csv_path = "data_gen_scripts/test_data_300k.csv";
    if !std::path::Path::new(csv_path).exists() {
        println!("test_data_300k.csv not found. Extracting from tar.gz...");
        
        // Extract the file
        std::process::Command::new("tar")
            .args(&["-xzf", "data_gen_scripts/test_data_300k.tar.gz", "-C", "data_gen_scripts/"])
            .output()?;
    }
    
    // Import the CSV
    println!("Importing CSV...");
    db.stream_insert_csv_with_header_row("test_data_300k", std::path::Path::new(csv_path), ',', 0)?;
    
    // Get sample of good_time values
    let query = "SELECT good_time FROM test_data_300k WHERE good_time IS NOT NULL ORDER BY good_time LIMIT 10";
    let result = db.execute_query(query)?;
    
    println!("Sample good_time values (sorted):");
    for row in &result {
        println!("  {}", row[0]);
    }
    
    // Test time binning on good_time column
    let config = TimeBinConfig {
        selected_table: "test_data_300k".to_string(),
        selected_column: "good_time".to_string(),
        strategy: TimeBinStrategy::FixedInterval {
            interval_seconds: 3600, // 1 hour
            interval_format: "1h".to_string(),
        },
        output_column_name: "good_time_hourly_bin".to_string(),
        output_filename: Some("test_binning_result".to_string()),
    };
    
    let output_dir = std::path::Path::new(".");
    match TimeGroupingEngine::apply_grouping(&Arc::new(db.clone()), &config, output_dir) {
        Ok(output_table) => {
            println!("\nTime binning successful! Created table: {}", output_table);
            
            // Check the bin distribution
            let bin_query = format!(
                "SELECT good_time_hourly_bin, COUNT(*) as count FROM \"{}\" 
                 GROUP BY good_time_hourly_bin 
                 ORDER BY CAST(good_time_hourly_bin AS INTEGER)",
                output_table
            );
            
            if let Ok(bins) = db.execute_query(&bin_query) {
                println!("\nBin distribution:");
                for row in bins {
                    println!("  Bin {}: {} rows", row[0], row[1]);
                }
            }
        }
        Err(e) => {
            println!("Time binning failed: {}", e);
        }
    }
    
    Ok(())
}