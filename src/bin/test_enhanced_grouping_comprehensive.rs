use leaf::core::{Database, EnhancedGroupingProcessor};
use leaf::ui::enhanced_grouping::{EnhancedGroupingRequest, GroupingConfig, GroupingRule};
use std::sync::Arc;
use std::path::Path;

fn main() {
    println!("=== Comprehensive Enhanced Grouping Test ===\n");
    
    // Create test database
    let db = Arc::new(Database::open_memory().expect("Failed to create database"));
    
    // Create test tables
    create_test_tables(&db);
    
    // Create processor
    let processor = EnhancedGroupingProcessor::new();
    let output_dir = Path::new("test_output");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir).expect("Failed to create output directory");
    }
    
    // Test Value Change grouping
    println!("\n--- Testing Value Change Grouping ---");
    test_value_change(&processor, &db, output_dir);
    
    // Test Value Equals grouping
    println!("\n--- Testing Value Equals Grouping ---");
    test_value_equals(&processor, &db, output_dir);
    
    // Test Is Empty grouping
    println!("\n--- Testing Is Empty Grouping ---");
    test_is_empty(&processor, &db, output_dir);
    
    // Test with timestamp columns
    println!("\n--- Testing with Timestamp Columns ---");
    test_timestamp_grouping(&processor, &db, output_dir);
    
    // Test reset on change behavior
    println!("\n--- Testing Reset on Change ---");
    test_reset_on_change(&processor, &db, output_dir);
}

fn create_test_tables(db: &Arc<Database>) {
    // Table 1: Simple data with repeating values
    let create_sql = r#"
        CREATE TABLE test_groups (
            id INTEGER,
            category TEXT,
            status TEXT,
            value DOUBLE,
            optional_field TEXT
        )
    "#;
    
    db.execute_sql(create_sql).expect("Failed to create table");
    
    // Insert test data
    let data = vec![
        (1, "A", "active", 100.0, Some("data1")),
        (2, "A", "active", 150.0, Some("data2")),
        (3, "A", "inactive", 120.0, None),
        (4, "B", "active", 180.0, Some("data3")),
        (5, "B", "active", 200.0, None),
        (6, "B", "inactive", 160.0, Some("data4")),
        (7, "C", "active", 190.0, None),
        (8, "C", "active", 210.0, Some("data5")),
        (9, "A", "inactive", 170.0, None),
        (10, "A", "active", 220.0, Some("data6")),
    ];
    
    for (id, cat, status, val, opt) in data {
        let opt_val = opt.map(|s| format!("'{}'", s)).unwrap_or("NULL".to_string());
        let insert_sql = format!(
            "INSERT INTO test_groups VALUES ({}, '{}', '{}', {}, {})",
            id, cat, status, val, opt_val
        );
        db.execute_sql(&insert_sql).expect("Failed to insert data");
    }
    
    // Table 2: Time series data
    let create_sql2 = r#"
        CREATE TABLE test_timeseries (
            id INTEGER,
            timestamp TIMESTAMP,
            sensor TEXT,
            reading DOUBLE
        )
    "#;
    
    db.execute_sql(create_sql2).expect("Failed to create table");
    
    // Insert time series data
    let base_time = chrono::Utc::now();
    for i in 0..20 {
        let timestamp = base_time + chrono::Duration::minutes(i * 5);
        let sensor = if i % 3 == 0 { "sensor_A" } else if i % 3 == 1 { "sensor_B" } else { "sensor_C" };
        let reading = 100.0 + (i as f64) * 5.0 + (i % 3) as f64 * 10.0;
        
        let insert_sql = format!(
            "INSERT INTO test_timeseries VALUES ({}, '{}', '{}', {})",
            i + 1, timestamp.format("%Y-%m-%d %H:%M:%S"), sensor, reading
        );
        db.execute_sql(&insert_sql).expect("Failed to insert data");
    }
    
    println!("✓ Created test tables with sample data");
}

fn test_value_change(processor: &EnhancedGroupingProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = EnhancedGroupingRequest {
        table_name: "test_groups".to_string(),
        configurations: vec![
            GroupingConfig {
                rule: GroupingRule::ValueChange { column: "category".to_string() },
                output_column: "category_group_id".to_string(),
                reset_on_change: false,
            },
            GroupingConfig {
                rule: GroupingRule::ValueChange { column: "status".to_string() },
                output_column: "status_group_id".to_string(),
                reset_on_change: false,
            },
        ],
        output_filename: Some("test_value_change".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_results(db, "test_value_change", &["id", "category", "category_group_id", "status", "status_group_id"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_value_equals(processor: &EnhancedGroupingProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = EnhancedGroupingRequest {
        table_name: "test_groups".to_string(),
        configurations: vec![
            GroupingConfig {
                rule: GroupingRule::ValueEquals { 
                    column: "category".to_string(),
                    value: "B".to_string(),
                },
                output_column: "is_category_b".to_string(),
                reset_on_change: false,
            },
            GroupingConfig {
                rule: GroupingRule::ValueEquals { 
                    column: "status".to_string(),
                    value: "active".to_string(),
                },
                output_column: "is_active".to_string(),
                reset_on_change: false,
            },
        ],
        output_filename: Some("test_value_equals".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_results(db, "test_value_equals", &["id", "category", "is_category_b", "status", "is_active"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_is_empty(processor: &EnhancedGroupingProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = EnhancedGroupingRequest {
        table_name: "test_groups".to_string(),
        configurations: vec![
            GroupingConfig {
                rule: GroupingRule::IsEmpty { column: "optional_field".to_string() },
                output_column: "empty_block_id".to_string(),
                reset_on_change: false,
            },
        ],
        output_filename: Some("test_is_empty".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_results(db, "test_is_empty", &["id", "optional_field", "empty_block_id"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_timestamp_grouping(processor: &EnhancedGroupingProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = EnhancedGroupingRequest {
        table_name: "test_timeseries".to_string(),
        configurations: vec![
            GroupingConfig {
                rule: GroupingRule::ValueChange { column: "sensor".to_string() },
                output_column: "sensor_group_id".to_string(),
                reset_on_change: false,
            },
            GroupingConfig {
                rule: GroupingRule::ValueChange { column: "timestamp".to_string() },
                output_column: "time_change_id".to_string(),
                reset_on_change: false,
            },
        ],
        output_filename: Some("test_timestamp".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_results(db, "test_timestamp", &["id", "timestamp", "sensor", "sensor_group_id", "time_change_id"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_reset_on_change(processor: &EnhancedGroupingProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = EnhancedGroupingRequest {
        table_name: "test_groups".to_string(),
        configurations: vec![
            GroupingConfig {
                rule: GroupingRule::ValueChange { column: "category".to_string() },
                output_column: "category_seq".to_string(),
                reset_on_change: true,  // This will reset counter on each change
            },
            GroupingConfig {
                rule: GroupingRule::IsEmpty { column: "optional_field".to_string() },
                output_column: "empty_seq".to_string(),
                reset_on_change: true,  // This will reset counter for each empty block
            },
        ],
        output_filename: Some("test_reset".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_results(db, "test_reset", &["id", "category", "category_seq", "optional_field", "empty_seq"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn show_results(db: &Arc<Database>, table_name: &str, columns: &[&str]) {
    let cols = columns.join(", ");
    let query = format!("SELECT {} FROM \"{}\" ORDER BY id", cols, table_name);
    
    match db.execute_query(&query) {
        Ok(rows) => {
            println!("  Results:");
            // Print header
            println!("  {}", columns.join("\t"));
            println!("  {}", "-".repeat(columns.len() * 10));
            // Print all rows
            for row in rows {
                let values: Vec<String> = (0..columns.len())
                    .map(|i| row.get(i).unwrap_or(&"NULL".to_string()).to_string())
                    .collect();
                println!("  {}", values.join("\t"));
            }
        }
        Err(e) => println!("  Failed to query results: {}", e),
    }
}