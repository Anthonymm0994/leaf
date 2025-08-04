use leaf::core::{Database, time_grouping::TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};
use std::sync::Arc;
use std::path::Path;

fn main() {
    println!("=== Comprehensive Time Binning Test ===\n");
    
    // Test with simple time format
    let test_cases = vec![
        ("time_only.csv", "time", vec![
            ("Fixed 30s", TimeBinStrategy::FixedInterval { interval_seconds: 30, interval_format: "30".to_string() }),
            ("Fixed 1m", TimeBinStrategy::FixedInterval { interval_seconds: 60, interval_format: "60".to_string() }),
            ("Manual", TimeBinStrategy::ManualIntervals { 
                intervals: vec!["12:00:00".to_string(), "12:05:00".to_string(), "12:10:00".to_string()],
                interval_string: "12:00:00, 12:05:00, 12:10:00".to_string()
            }),
            ("Threshold 90s", TimeBinStrategy::ThresholdBased { threshold_seconds: 90, threshold_format: "90".to_string() }),
        ]),
    ];
    
    // Create output directory
    let output_dir = Path::new("test_output");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir).expect("Failed to create output directory");
    }
    
    for (file, time_column, strategies) in test_cases {
        println!("\n--- Testing file: {} ---", file);
        
        // Create database and load test data
        let db_path = output_dir.join(format!("test_{}.db", file.replace(".csv", "")));
        let db = Arc::new(Database::open_writable(&db_path).expect("Failed to create database"));
        
        // Load the CSV file
        let csv_path = Path::new("test_data").join(file);
        if !csv_path.exists() {
            println!("  ⚠️  File not found: {:?}", csv_path);
            continue;
        }
        
        // Read CSV and create table
        let table_name = file.replace(".csv", "");
        
        // For simplicity, we'll create the table manually with known schema
        match file {
            "time_only.csv" => {
                let create_sql = "CREATE TABLE time_only (time TEXT, value INTEGER, event TEXT)";
                if let Err(e) = db.execute_sql(create_sql) {
                    println!("  ✗ Failed to create table: {}", e);
                    continue;
                }
                // Insert data manually for testing
                let insert_sql = r#"
                    INSERT INTO time_only VALUES 
                    ('12:00:00', 100, 'event1'),
                    ('12:01:00', 150, 'event2'),
                    ('12:02:00', 200, 'event3'),
                    ('12:03:00', 250, 'event4'),
                    ('12:04:00', 300, 'event5'),
                    ('12:05:00', 350, 'event6'),
                    ('12:06:00', 400, 'event7'),
                    ('12:07:00', 450, 'event8'),
                    ('12:08:00', 500, 'event9'),
                    ('12:09:00', 550, 'event10')
                "#;
                if let Err(e) = db.execute_sql(insert_sql) {
                    println!("  ✗ Failed to insert data: {}", e);
                    continue;
                }
            }
            _ => {
                println!("  ⚠️  Skipping unsupported file: {}", file);
                continue;
            }
        }
        println!("  ✓ Created table: {}", table_name);
        
        // Get table info
        match db.execute_query(&format!("SELECT COUNT(*) as count FROM \"{}\"", table_name)) {
            Ok(rows) => {
                if let Some(count) = rows.get(0).and_then(|r| r.get(0)) {
                    println!("  ✓ Table has {} rows", count);
                }
            }
            Err(e) => println!("  ✗ Failed to count rows: {}", e),
        }
        
        // Test each strategy
        for (strategy_name, strategy) in strategies {
            println!("\n  Testing strategy: {}", strategy_name);
            
            let config = TimeBinConfig {
                selected_table: table_name.clone(),
                selected_column: time_column.to_string(),
                strategy: strategy.clone(),
                output_column_name: format!("{}_bin", time_column),
                output_filename: Some(format!("{}_{}_binned", table_name, strategy_name.replace(" ", "_").to_lowercase())),
            };
            
            match TimeGroupingEngine::apply_grouping(&db, &config, output_dir) {
                Ok(output_table) => {
                    println!("    ✓ Created binned table: {}", output_table);
                    
                    // Analyze the results
                    let query = format!(
                        "SELECT \"{}\", COUNT(*) as count FROM \"{}\" GROUP BY \"{}\" ORDER BY \"{}\"",
                        config.output_column_name, output_table, config.output_column_name, config.output_column_name
                    );
                    
                    match db.execute_query(&query) {
                        Ok(rows) => {
                            println!("    ✓ Created {} bins:", rows.len());
                            for (i, row) in rows.iter().take(5).enumerate() {
                                if let (Some(bin), Some(count)) = (row.get(0), row.get(1)) {
                                    println!("      Bin {}: {} rows", bin, count);
                                }
                            }
                            if rows.len() > 5 {
                                println!("      ... and {} more bins", rows.len() - 5);
                            }
                        }
                        Err(e) => println!("    ✗ Failed to analyze bins: {}", e),
                    }
                }
                Err(e) => println!("    ✗ Failed to apply binning: {}", e),
            }
        }
    }
    
    // Test with synthetic time data
    println!("\n\n--- Testing with synthetic time data ---");
    test_synthetic_time_data();
}

fn test_synthetic_time_data() {
    let db_path = Path::new("test_output").join("synthetic_test.db");
    let db = Arc::new(Database::open_writable(&db_path).expect("Failed to create database"));
    
    // Create a table with time data in HH:MM:SS.sss format
    let create_sql = r#"
        CREATE TABLE synthetic_time (
            id INTEGER,
            time_value TEXT,
            value DOUBLE
        )
    "#;
    
    if let Err(e) = db.execute_sql(create_sql) {
        println!("Failed to create synthetic table: {}", e);
        return;
    }
    
    // Insert test data spanning several hours
    let times = vec![
        "00:00:00.000", "00:00:30.500", "00:01:00.000", "00:01:30.123",
        "00:02:00.456", "00:02:30.789", "00:03:00.000", "00:03:30.111",
        "01:00:00.000", "01:00:30.222", "01:01:00.333", "01:01:30.444",
        "02:00:00.000", "02:00:30.555", "02:01:00.666", "02:01:30.777",
        "03:00:00.000", "03:00:30.888", "03:01:00.999", "03:01:30.000",
    ];
    
    for (i, time) in times.iter().enumerate() {
        let insert_sql = format!(
            "INSERT INTO synthetic_time VALUES ({}, '{}', {})",
            i, time, (i as f64) * 10.0
        );
        if let Err(e) = db.execute_sql(&insert_sql) {
            println!("Failed to insert row: {}", e);
        }
    }
    
    println!("✓ Created synthetic table with {} rows", times.len());
    
    // Test different binning strategies
    let strategies = vec![
        ("Fixed 30min", TimeBinStrategy::FixedInterval { 
            interval_seconds: 1800, 
            interval_format: "30:00".to_string() 
        }),
        ("Fixed 1hour", TimeBinStrategy::FixedInterval { 
            interval_seconds: 3600, 
            interval_format: "1:00:00".to_string() 
        }),
        ("Threshold 30min", TimeBinStrategy::ThresholdBased { 
            threshold_seconds: 1800, 
            threshold_format: "30:00".to_string() 
        }),
    ];
    
    let output_dir = Path::new("test_output");
    
    for (name, strategy) in strategies {
        println!("\nTesting {}", name);
        
        let config = TimeBinConfig {
            selected_table: "synthetic_time".to_string(),
            selected_column: "time_value".to_string(),
            strategy,
            output_column_name: "time_bin".to_string(),
            output_filename: Some(format!("synthetic_{}", name.replace(" ", "_").to_lowercase())),
        };
        
        match TimeGroupingEngine::apply_grouping(&db, &config, output_dir) {
            Ok(output_table) => {
                println!("  ✓ Created table: {}", output_table);
                
                // Show sample results
                let query = format!(
                    "SELECT time_value, time_bin FROM \"{}\" ORDER BY id LIMIT 10",
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
                    Err(e) => println!("  Failed to query results: {}", e),
                }
            }
            Err(e) => println!("  ✗ Failed: {}", e),
        }
    }
}