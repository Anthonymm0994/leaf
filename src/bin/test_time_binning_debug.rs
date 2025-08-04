use leaf::core::{Database, time_grouping::TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};
use std::sync::Arc;
use std::path::Path;
use std::fs;

fn main() {
    println!("=== Time Binning Debug Test ===\n");
    
    // Create output directory
    let output_dir = Path::new("test_output");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir).expect("Failed to create output directory");
    }
    
    // Create a test CSV file with HH:MM:SS.sss format
    let csv_content = r#"good_time,value,category
00:00:00.000,100,A
00:00:00.100,110,A
00:00:00.200,120,A
00:00:30.000,130,B
00:00:30.100,140,B
00:01:00.000,150,C
00:01:00.100,160,C
00:01:30.000,170,D
00:02:00.000,180,E
00:30:00.000,190,F
01:00:00.000,200,G
01:30:00.000,210,H
02:00:00.000,220,I
03:00:00.000,230,J
"#;
    
    let csv_path = output_dir.join("test_time_data.csv");
    fs::write(&csv_path, csv_content).expect("Failed to write test CSV");
    println!("✓ Created test CSV file: {:?}", csv_path);
    
    // Create database and load the CSV
    let db_path = output_dir.join("test_time_binning.db");
    if db_path.exists() {
        fs::remove_file(&db_path).ok();
    }
    let db = Arc::new(Database::open_writable(&db_path).expect("Failed to create database"));
    
    // Create table and insert data manually
    let create_sql = r#"
        CREATE TABLE test_time_data (
            good_time TEXT,
            value INTEGER,
            category TEXT
        )
    "#;
    
    if let Err(e) = db.execute_sql(create_sql) {
        println!("✗ Failed to create table: {}", e);
        return;
    }
    
    // Insert the data
    let insert_sql = r#"
        INSERT INTO test_time_data VALUES
        ('00:00:00.000', 100, 'A'),
        ('00:00:00.100', 110, 'A'),
        ('00:00:00.200', 120, 'A'),
        ('00:00:30.000', 130, 'B'),
        ('00:00:30.100', 140, 'B'),
        ('00:01:00.000', 150, 'C'),
        ('00:01:00.100', 160, 'C'),
        ('00:01:30.000', 170, 'D'),
        ('00:02:00.000', 180, 'E'),
        ('00:30:00.000', 190, 'F'),
        ('01:00:00.000', 200, 'G'),
        ('01:30:00.000', 210, 'H'),
        ('02:00:00.000', 220, 'I'),
        ('03:00:00.000', 230, 'J')
    "#;
    
    if let Err(e) = db.execute_sql(insert_sql) {
        println!("✗ Failed to insert data: {}", e);
        return;
    }
    
    println!("✓ Created table with 14 rows");
    
    // Test query to see what we get back
    println!("\n--- Testing query results ---");
    match db.execute_query("SELECT good_time FROM test_time_data LIMIT 5") {
        Ok(rows) => {
            println!("Query returned {} rows", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("  Row {}: {:?}", i, row);
            }
        }
        Err(e) => println!("✗ Query failed: {}", e),
    }
    
    // Test different binning strategies
    let strategies = vec![
        ("Fixed 30s", TimeBinStrategy::FixedInterval { 
            interval_seconds: 30, 
            interval_format: "30".to_string() 
        }),
        ("Fixed 1m", TimeBinStrategy::FixedInterval { 
            interval_seconds: 60, 
            interval_format: "60".to_string() 
        }),
        ("Fixed 30m", TimeBinStrategy::FixedInterval { 
            interval_seconds: 1800, 
            interval_format: "30:00".to_string() 
        }),
        ("Fixed 1h", TimeBinStrategy::FixedInterval { 
            interval_seconds: 3600, 
            interval_format: "1:00:00".to_string() 
        }),
        ("Threshold 30m", TimeBinStrategy::ThresholdBased { 
            threshold_seconds: 1800, 
            threshold_format: "30:00".to_string() 
        }),
    ];
    
    for (name, strategy) in strategies {
        println!("\n--- Testing {} ---", name);
        
        let config = TimeBinConfig {
            selected_table: "test_time_data".to_string(),
            selected_column: "good_time".to_string(),
            strategy,
            output_column_name: "time_bin".to_string(),
            output_filename: Some(format!("test_{}", name.replace(" ", "_").to_lowercase())),
        };
        
        match TimeGroupingEngine::apply_grouping(&db, &config, output_dir) {
            Ok(output_table) => {
                println!("✓ Created table: {}", output_table);
                
                // Query the results
                let query = format!(
                    "SELECT good_time, time_bin FROM \"{}\" ORDER BY good_time LIMIT 10",
                    output_table
                );
                
                match db.execute_query(&query) {
                    Ok(rows) => {
                        println!("  Sample results:");
                        for row in rows {
                            if let (Some(time), Some(bin)) = (row.get(0), row.get(1)) {
                                println!("    {} -> {}", time, bin);
                            }
                        }
                    }
                    Err(e) => println!("  ✗ Failed to query results: {}", e),
                }
                
                // Count bins
                let count_query = format!(
                    "SELECT time_bin, COUNT(*) as count FROM \"{}\" GROUP BY time_bin ORDER BY time_bin",
                    output_table
                );
                
                match db.execute_query(&count_query) {
                    Ok(rows) => {
                        println!("  Bin counts:");
                        for row in rows {
                            if let (Some(bin), Some(count)) = (row.get(0), row.get(1)) {
                                println!("    {}: {} rows", bin, count);
                            }
                        }
                    }
                    Err(e) => println!("  ✗ Failed to count bins: {}", e),
                }
            }
            Err(e) => println!("✗ Failed: {}", e),
        }
    }
}