use anyhow::Result;
use std::sync::Arc;
use leaf::core::{Database, TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};

fn main() -> Result<()> {
    println!("=== Thorough Time Binning Test ===\n");
    
    // Test with the actual test data file
    test_with_csv_file()?;
    test_with_arrow_file()?;
    
    Ok(())
}

fn test_with_csv_file() -> Result<()> {
    println!("Testing with CSV file (test_data_300k.csv)...\n");
    
    let mut db = Database::open_writable(".")?;
    
    // Import the CSV file
    let csv_path = "data_gen_scripts/test_data_300k.csv";
    println!("Importing CSV file: {}", csv_path);
    
    match db.stream_insert_csv_with_header_row("test_data_300k", std::path::Path::new(csv_path), ',', 0) {
        Ok(_) => println!("CSV imported successfully"),
        Err(e) => {
            println!("Failed to import CSV: {}", e);
            return Err(e.into());
        }
    }
    
    // Get table info
    let query = "SELECT * FROM test_data_300k LIMIT 5";
    let columns = db.get_column_names(&query)?;
    let types = db.get_column_types(&query)?;
    
    println!("\nTable columns:");
    for (col, dtype) in columns.iter().zip(types.iter()) {
        println!("  {}: {:?}", col, dtype);
    }
    
    // Find time columns
    let time_columns: Vec<_> = columns.iter()
        .filter(|col| col.contains("time"))
        .cloned()
        .collect();
    
    println!("\nTime-related columns found: {:?}", time_columns);
    
    // Test each time column
    for time_col in &time_columns {
        println!("\n--- Testing column: {} ---", time_col);
        test_time_column(&Arc::new(db.clone()), "test_data_300k", time_col)?;
    }
    
    Ok(())
}

fn test_with_arrow_file() -> Result<()> {
    println!("\n\nTesting with Arrow file (test_data_300k_grouped.arrow)...\n");
    
    let mut db = Database::open_writable(".")?;
    
    // Try to load the arrow file if it exists
    let arrow_path = "test_data_300k_grouped.arrow";
    if std::path::Path::new(arrow_path).exists() {
        println!("Loading Arrow file: {}", arrow_path);
        match db.load_table_arrow_ipc("test_data_300k_grouped", std::path::Path::new(arrow_path)) {
            Ok(_) => println!("Arrow file loaded successfully"),
            Err(e) => {
                println!("Failed to load Arrow file: {}", e);
                return Ok(()); // Continue with other tests
            }
        }
        
        // Get table info
        let query = "SELECT * FROM test_data_300k_grouped LIMIT 5";
        let columns = db.get_column_names(&query)?;
        let types = db.get_column_types(&query)?;
        
        println!("\nTable columns:");
        for (col, dtype) in columns.iter().zip(types.iter()) {
            println!("  {}: {:?}", col, dtype);
        }
        
        // Find time columns
        let time_columns: Vec<_> = columns.iter()
            .filter(|col| col.contains("time"))
            .cloned()
            .collect();
        
        println!("\nTime-related columns found: {:?}", time_columns);
        
        // Test good_time and dumb_time specifically
        for time_col in &["good_time", "dumb_time"] {
            if columns.contains(&time_col.to_string()) {
                println!("\n--- Testing column: {} ---", time_col);
                test_time_column(&Arc::new(db.clone()), "test_data_300k_grouped", time_col)?;
            }
        }
    } else {
        println!("Arrow file not found: {}", arrow_path);
    }
    
    Ok(())
}

fn test_time_column(db: &Arc<Database>, table_name: &str, column_name: &str) -> Result<()> {
    // Get sample values
    let sample_query = format!(
        "SELECT DISTINCT \"{}\" FROM \"{}\" WHERE \"{}\" IS NOT NULL LIMIT 10",
        column_name, table_name, column_name
    );
    
    println!("Sample values:");
    match db.execute_query(&sample_query) {
        Ok(rows) => {
            for (i, row) in rows.iter().take(5).enumerate() {
                if !row.is_empty() {
                    println!("  [{}] '{}'", i, row[0]);
                }
            }
        }
        Err(e) => {
            println!("  Failed to get samples: {}", e);
        }
    }
    
    // Check for empty strings
    let empty_query = format!(
        "SELECT COUNT(*) FROM \"{}\" WHERE \"{}\" = ''",
        table_name, column_name
    );
    
    if let Ok(rows) = db.execute_query(&empty_query) {
        if let Some(row) = rows.first() {
            if let Ok(count) = row[0].parse::<i64>() {
                if count > 0 {
                    println!("  Warning: {} empty string values found", count);
                }
            }
        }
    }
    
    // Test 1-hour fixed interval binning
    println!("\nTesting 1-hour fixed interval binning:");
    let config = TimeBinConfig {
        selected_table: table_name.to_string(),
        selected_column: column_name.to_string(),
        strategy: TimeBinStrategy::FixedInterval {
            interval_seconds: 3600, // 1 hour
            interval_format: "1h".to_string(),
        },
        output_column_name: format!("{}_hourly_bin", column_name),
        output_filename: Some(format!("{}_hourly_binned", table_name)),
    };
    
    let output_dir = std::path::Path::new(".");
    match TimeGroupingEngine::apply_grouping(db, &config, output_dir) {
        Ok(output_table) => {
            println!("  Success! Created table: {}", output_table);
            
            // Verify the binning results
            let verify_query = format!(
                "SELECT \"{}\", COUNT(*) as count FROM \"{}\" GROUP BY \"{}\" ORDER BY \"{}\"",
                config.output_column_name, output_table, config.output_column_name, config.output_column_name
            );
            
            if let Ok(rows) = db.execute_query(&verify_query) {
                println!("  Bin distribution:");
                for row in rows.iter().take(10) {
                    if row.len() >= 2 {
                        println!("    Bin {}: {} rows", row[0], row[1]);
                    }
                }
                if rows.len() > 10 {
                    println!("    ... and {} more bins", rows.len() - 10);
                }
            }
        }
        Err(e) => {
            println!("  Failed: {}", e);
            
            // Try to get more details about the error
            let test_parse_query = format!(
                "SELECT \"{}\" FROM \"{}\" WHERE \"{}\" IS NOT NULL AND \"{}\" != '' LIMIT 1",
                column_name, table_name, column_name, column_name
            );
            
            if let Ok(rows) = db.execute_query(&test_parse_query) {
                if let Some(row) = rows.first() {
                    if !row.is_empty() {
                        println!("  Testing parse on value: '{}'", row[0]);
                        
                        // Test parsing directly
                        use chrono::NaiveTime;
                        let test_val = &row[0];
                        
                        // Try different formats
                        if let Ok(time) = NaiveTime::parse_from_str(test_val, "%H:%M:%S%.f") {
                            println!("  ✓ Parsed with %H:%M:%S%.f format: {:?}", time);
                        } else if let Ok(time) = NaiveTime::parse_from_str(test_val, "%H:%M:%S%.3f") {
                            println!("  ✓ Parsed with %H:%M:%S%.3f format: {:?}", time);
                        } else if let Ok(time) = NaiveTime::parse_from_str(test_val, "%H:%M:%S") {
                            println!("  ✓ Parsed with %H:%M:%S format: {:?}", time);
                        } else {
                            println!("  ✗ Failed to parse with any time format");
                        }
                    }
                }
            }
        }
    }
    
    Ok(())
}