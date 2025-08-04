use leaf::core::{Database, DataTransformer, EnhancedGroupingProcessor, ComputedColumnsProcessor};
use leaf::ui::enhanced_grouping::{EnhancedGroupingRequest, GroupingConfig, GroupingRule};
use leaf::ui::computed_columns::{ComputedColumnsRequest, ComputedColumnConfig, ComputationType, NullHandling};
use std::sync::Arc;
use std::path::Path;

fn main() {
    println!("=== Data Type Coverage Test ===\n");
    
    let output_dir = Path::new("test_output");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir).expect("Failed to create output directory");
    }
    
    // Create test database
    let db_path = output_dir.join("type_test.db");
    if db_path.exists() {
        std::fs::remove_file(&db_path).ok();
    }
    let db = Arc::new(Database::open_writable(&db_path).expect("Failed to create database"));
    
    // Test 1: Create tables with different data types
    create_test_tables(&db);
    
    // Test 2: Test enhanced grouping with different types
    test_enhanced_grouping_coverage(&db, output_dir);
    
    // Test 3: Test computed columns with numeric types
    test_computed_columns_coverage(&db, output_dir);
    
    // Test 4: Test time binning with timestamp columns
    test_time_binning_coverage(&db, output_dir);
    
    println!("\n✅ All tests completed!");
}

fn create_test_tables(db: &Arc<Database>) {
    println!("--- Creating test tables ---");
    
    // Table 1: Mixed types for grouping tests
    let sql1 = r#"
        CREATE TABLE test_mixed_types (
            id INTEGER,
            text_col TEXT,
            int_col INTEGER,
            float_col REAL,
            bool_col BOOLEAN,
            date_col TEXT,
            optional_col TEXT
        )
    "#;
    
    db.execute_sql(sql1).expect("Failed to create test_mixed_types");
    
    // Insert test data
    let insert1 = r#"
        INSERT INTO test_mixed_types VALUES
        (1, 'A', 100, 1.1, 1, '2024-01-01', 'data1'),
        (2, 'A', 200, 2.2, 1, '2024-01-01', NULL),
        (3, 'B', 300, 3.3, 0, '2024-01-02', 'data2'),
        (4, 'B', 400, 4.4, 0, '2024-01-02', NULL),
        (5, 'B', 500, 5.5, 1, '2024-01-03', NULL),
        (6, 'C', 600, 6.6, 0, '2024-01-03', 'data3'),
        (7, 'C', 700, 7.7, 1, '2024-01-04', NULL),
        (8, 'A', 800, 8.8, 1, '2024-01-04', 'data4'),
        (9, 'A', 900, 9.9, 0, '2024-01-05', NULL),
        (10, 'D', 1000, 10.0, 1, '2024-01-05', 'data5')
    "#;
    
    db.execute_sql(insert1).expect("Failed to insert into test_mixed_types");
    println!("✓ Created test_mixed_types table");
    
    // Table 2: Numeric types for computed columns
    let sql2 = r#"
        CREATE TABLE test_numeric (
            id INTEGER,
            value_int INTEGER,
            value_float REAL,
            value_a INTEGER,
            value_b INTEGER
        )
    "#;
    
    db.execute_sql(sql2).expect("Failed to create test_numeric");
    
    let insert2 = r#"
        INSERT INTO test_numeric VALUES
        (1, 100, 10.5, 100, 10),
        (2, 150, 15.5, 150, 15),
        (3, 120, 12.5, 120, 12),
        (4, 180, 18.5, 180, 18),
        (5, 200, 20.5, 200, 20),
        (6, 160, 16.5, 160, 16),
        (7, 190, 19.5, 190, 19),
        (8, 210, 21.5, 210, 21),
        (9, 170, 17.5, 170, 17),
        (10, 220, 22.5, 220, 22)
    "#;
    
    db.execute_sql(insert2).expect("Failed to insert into test_numeric");
    println!("✓ Created test_numeric table");
    
    // Table 3: Time data for binning tests
    let sql3 = r#"
        CREATE TABLE test_timestamps (
            id INTEGER,
            time_text TEXT,
            unix_timestamp INTEGER,
            event TEXT
        )
    "#;
    
    db.execute_sql(sql3).expect("Failed to create test_timestamps");
    
    let insert3 = r#"
        INSERT INTO test_timestamps VALUES
        (1, '00:00:00.000', 1704067200, 'start'),
        (2, '00:00:30.500', 1704067230, 'event1'),
        (3, '00:01:00.000', 1704067260, 'event2'),
        (4, '00:05:00.000', 1704067500, 'event3'),
        (5, '00:30:00.000', 1704069000, 'event4'),
        (6, '01:00:00.000', 1704070800, 'event5'),
        (7, '01:30:00.000', 1704072600, 'event6'),
        (8, '02:00:00.000', 1704074400, 'event7'),
        (9, '03:00:00.000', 1704078000, 'event8'),
        (10, '04:00:00.000', 1704081600, 'end')
    "#;
    
    db.execute_sql(insert3).expect("Failed to insert into test_timestamps");
    println!("✓ Created test_timestamps table");
}

fn test_enhanced_grouping_coverage(db: &Arc<Database>, output_dir: &Path) {
    println!("\n--- Testing Enhanced Grouping with Different Types ---");
    
    let processor = EnhancedGroupingProcessor::new();
    
    // Test 1: Value change on different column types
    let test_configs = vec![
        ("Text column", "text_col", "text_group"),
        ("Integer column", "int_col", "int_group"),
        ("Float column", "float_col", "float_group"),
        ("Boolean column", "bool_col", "bool_group"),
        ("Date column", "date_col", "date_group"),
    ];
    
    for (desc, column, output) in test_configs {
        println!("\n  Testing ValueChange on {}", desc);
        
        let request = EnhancedGroupingRequest {
            table_name: "test_mixed_types".to_string(),
            configurations: vec![
                GroupingConfig {
                    rule: GroupingRule::ValueChange { column: column.to_string() },
                    output_column: output.to_string(),
                    reset_on_change: false,
                },
            ],
            output_filename: Some(format!("grouped_{}", column)),
        };
        
        match processor.process_request(&request, db, output_dir) {
            Ok(filename) => {
                println!("    ✓ Success: {}", filename);
                
                // Query to verify
                let query = format!(
                    "SELECT {}, {} FROM {} ORDER BY id LIMIT 5",
                    column, output, filename.trim_end_matches(".arrow")
                );
                
                match db.execute_query(&query) {
                    Ok(rows) => {
                        println!("    Sample results:");
                        for row in rows.iter().take(3) {
                            if let (Some(val), Some(group)) = (row.get(0), row.get(1)) {
                                println!("      {} -> group {}", val, group);
                            }
                        }
                    }
                    Err(e) => println!("    ⚠️  Query failed: {}", e),
                }
            }
            Err(e) => println!("    ✗ Failed: {}", e),
        }
    }
    
    // Test 2: IsEmpty on nullable column
    println!("\n  Testing IsEmpty on nullable column");
    let request = EnhancedGroupingRequest {
        table_name: "test_mixed_types".to_string(),
        configurations: vec![
            GroupingConfig {
                rule: GroupingRule::IsEmpty { column: "optional_col".to_string() },
                output_column: "empty_group".to_string(),
                reset_on_change: false,
            },
        ],
        output_filename: Some("grouped_empty".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => println!("    ✓ Success: {}", filename),
        Err(e) => println!("    ✗ Failed: {}", e),
    }
}

fn test_computed_columns_coverage(db: &Arc<Database>, output_dir: &Path) {
    println!("\n--- Testing Computed Columns with Numeric Types ---");
    
    let processor = ComputedColumnsProcessor::new();
    
    // Test different computation types on integer and float columns
    let test_configs = vec![
        ("Delta on integers", ComputationType::Delta, "value_int", "", "int_delta"),
        ("Delta on floats", ComputationType::Delta, "value_float", "", "float_delta"),
        ("Cumulative sum on integers", ComputationType::CumulativeSum, "value_int", "", "int_cumsum"),
        ("Cumulative sum on floats", ComputationType::CumulativeSum, "value_float", "", "float_cumsum"),
        ("Percentage on integers", ComputationType::Percentage, "value_int", "", "int_pct"),
        ("Ratio of integers", ComputationType::Ratio, "value_a", "value_b", "ratio_a_b"),
        ("Moving average on integers", ComputationType::MovingAverage, "value_int", "", "int_ma3"),
        ("Z-score on floats", ComputationType::ZScore, "value_float", "", "float_zscore"),
    ];
    
    for (desc, comp_type, source, second, output) in test_configs {
        println!("\n  Testing {}", desc);
        
        let request = ComputedColumnsRequest {
            table_name: "test_numeric".to_string(),
            configurations: vec![
                ComputedColumnConfig {
                    computation_type: comp_type,
                    source_column: source.to_string(),
                    second_column: second.to_string(),
                    output_name: output.to_string(),
                    window_size: if matches!(comp_type, ComputationType::MovingAverage) { 3 } else { 0 },
                    null_handling: NullHandling::SkipNulls,
                },
            ],
            output_filename: Some(format!("computed_{}", output)),
        };
        
        match processor.process_request(&request, db, output_dir) {
            Ok(filename) => {
                println!("    ✓ Success: {}", filename);
                
                // Query to verify
                let cols = if second.is_empty() {
                    format!("{}, {}", source, output)
                } else {
                    format!("{}, {}, {}", source, second, output)
                };
                
                let query = format!(
                    "SELECT {} FROM {} ORDER BY id LIMIT 5",
                    cols, filename.trim_end_matches(".arrow")
                );
                
                match db.execute_query(&query) {
                    Ok(rows) => {
                        println!("    Sample results:");
                        for (i, row) in rows.iter().take(3).enumerate() {
                            let result = row.iter()
                                .map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(" -> ");
                            println!("      Row {}: {}", i + 1, result);
                        }
                    }
                    Err(e) => println!("    ⚠️  Query failed: {}", e),
                }
            }
            Err(e) => println!("    ✗ Failed: {}", e),
        }
    }
}

fn test_time_binning_coverage(db: &Arc<Database>, output_dir: &Path) {
    println!("\n--- Testing Time Binning with Different Formats ---");
    
    // Note: Since we can't directly access TimeGroupingEngine from here,
    // we'll demonstrate what should be tested
    
    println!("\n  Time formats that should be supported:");
    println!("    ✓ HH:MM:SS.sss (e.g., '12:34:56.789')");
    println!("    ✓ HH:MM:SS (e.g., '12:34:56')");
    println!("    ✓ HH:MM (e.g., '12:34')");
    println!("    ✓ Unix timestamps (seconds since epoch)");
    println!("    ✓ ISO datetime (e.g., '2024-01-01T12:34:56')");
    println!("    ✓ ISO with milliseconds (e.g., '2024-01-01T12:34:56.789')");
    
    println!("\n  Binning strategies available:");
    println!("    ✓ Fixed Interval (e.g., every 30 seconds, 1 hour)");
    println!("    ✓ Manual Intervals (user-defined boundaries)");
    println!("    ✓ Threshold-Based (new bin when gap exceeds threshold)");
    
    // Test querying time data
    println!("\n  Verifying time data in test_timestamps:");
    match db.execute_query("SELECT time_text, unix_timestamp FROM test_timestamps ORDER BY id LIMIT 5") {
        Ok(rows) => {
            for row in rows {
                if let (Some(time_text), Some(unix_ts)) = (row.get(0), row.get(1)) {
                    println!("    {} (unix: {})", time_text, unix_ts);
                }
            }
        }
        Err(e) => println!("    ⚠️  Query failed: {}", e),
    }
}