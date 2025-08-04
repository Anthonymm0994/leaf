use leaf::core::{Database, ComputedColumnsProcessor};
use leaf::ui::computed_columns::{ComputedColumnsRequest, ComputedColumnConfig, ComputationType, NullHandling};
use std::sync::Arc;
use std::path::Path;

fn main() {
    println!("=== Comprehensive Computed Columns Test ===\n");
    
    // Create test database
    let db = Arc::new(Database::open_memory().expect("Failed to create database"));
    
    // Create test table with various data types
    create_test_table(&db);
    
    // Create processor
    let processor = ComputedColumnsProcessor::new();
    let output_dir = Path::new("test_output");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir).expect("Failed to create output directory");
    }
    
    // Test Delta transformation
    println!("\n--- Testing Delta Transformation ---");
    test_delta(&processor, &db, output_dir);
    
    // Test Cumulative Sum
    println!("\n--- Testing Cumulative Sum ---");
    test_cumulative_sum(&processor, &db, output_dir);
    
    // Test Percentage
    println!("\n--- Testing Percentage of Total ---");
    test_percentage(&processor, &db, output_dir);
    
    // Test Ratio
    println!("\n--- Testing Ratio ---");
    test_ratio(&processor, &db, output_dir);
    
    // Test Moving Average
    println!("\n--- Testing Moving Average ---");
    test_moving_average(&processor, &db, output_dir);
    
    // Test Z-Score
    println!("\n--- Testing Z-Score ---");
    test_zscore(&processor, &db, output_dir);
    
    // Test with null values
    println!("\n--- Testing with NULL values ---");
    test_with_nulls(&processor, &db, output_dir);
}

fn create_test_table(db: &Arc<Database>) {
    let create_sql = r#"
        CREATE TABLE test_data (
            id INTEGER,
            timestamp TEXT,
            value_a DOUBLE,
            value_b DOUBLE,
            category TEXT
        )
    "#;
    
    db.execute_sql(create_sql).expect("Failed to create table");
    
    // Insert test data
    let data = vec![
        (1, "00:00:00", 100.0, 10.0, "A"),
        (2, "00:01:00", 150.0, 15.0, "A"),
        (3, "00:02:00", 120.0, 12.0, "B"),
        (4, "00:03:00", 180.0, 18.0, "B"),
        (5, "00:04:00", 200.0, 20.0, "A"),
        (6, "00:05:00", 160.0, 16.0, "B"),
        (7, "00:06:00", 190.0, 19.0, "A"),
        (8, "00:07:00", 210.0, 21.0, "B"),
        (9, "00:08:00", 170.0, 17.0, "A"),
        (10, "00:09:00", 220.0, 22.0, "B"),
    ];
    
    for (id, time, val_a, val_b, cat) in data {
        let insert_sql = format!(
            "INSERT INTO test_data VALUES ({}, '{}', {}, {}, '{}')",
            id, time, val_a, val_b, cat
        );
        db.execute_sql(&insert_sql).expect("Failed to insert data");
    }
    
    println!("✓ Created test table with 10 rows");
}

fn test_delta(processor: &ComputedColumnsProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = ComputedColumnsRequest {
        table_name: "test_data".to_string(),
        configurations: vec![
            ComputedColumnConfig {
                computation_type: ComputationType::Delta,
                source_column: "value_a".to_string(),
                second_column: String::new(),
                output_name: "value_a_delta".to_string(),
                window_size: 0,
                null_handling: NullHandling::SkipNulls,
            },
            ComputedColumnConfig {
                computation_type: ComputationType::Delta,
                source_column: "value_b".to_string(),
                second_column: String::new(),
                output_name: "value_b_delta".to_string(),
                window_size: 0,
                null_handling: NullHandling::SkipNulls,
            },
        ],
        output_filename: Some("test_delta".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_sample_results(db, "test_delta", &["id", "value_a", "value_a_delta", "value_b", "value_b_delta"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_cumulative_sum(processor: &ComputedColumnsProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = ComputedColumnsRequest {
        table_name: "test_data".to_string(),
        configurations: vec![
            ComputedColumnConfig {
                computation_type: ComputationType::CumulativeSum,
                source_column: "value_a".to_string(),
                second_column: String::new(),
                output_name: "value_a_cumsum".to_string(),
                window_size: 0,
                null_handling: NullHandling::SkipNulls,
            },
        ],
        output_filename: Some("test_cumsum".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_sample_results(db, "test_cumsum", &["id", "value_a", "value_a_cumsum"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_percentage(processor: &ComputedColumnsProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = ComputedColumnsRequest {
        table_name: "test_data".to_string(),
        configurations: vec![
            ComputedColumnConfig {
                computation_type: ComputationType::Percentage,
                source_column: "value_a".to_string(),
                second_column: String::new(),
                output_name: "value_a_pct".to_string(),
                window_size: 0,
                null_handling: NullHandling::SkipNulls,
            },
        ],
        output_filename: Some("test_percentage".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_sample_results(db, "test_percentage", &["id", "value_a", "value_a_pct"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_ratio(processor: &ComputedColumnsProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = ComputedColumnsRequest {
        table_name: "test_data".to_string(),
        configurations: vec![
            ComputedColumnConfig {
                computation_type: ComputationType::Ratio,
                source_column: "value_a".to_string(),
                second_column: "value_b".to_string(),
                output_name: "a_to_b_ratio".to_string(),
                window_size: 0,
                null_handling: NullHandling::SkipNulls,
            },
        ],
        output_filename: Some("test_ratio".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_sample_results(db, "test_ratio", &["id", "value_a", "value_b", "a_to_b_ratio"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_moving_average(processor: &ComputedColumnsProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = ComputedColumnsRequest {
        table_name: "test_data".to_string(),
        configurations: vec![
            ComputedColumnConfig {
                computation_type: ComputationType::MovingAverage,
                source_column: "value_a".to_string(),
                second_column: String::new(),
                output_name: "value_a_ma3".to_string(),
                window_size: 3,
                null_handling: NullHandling::SkipNulls,
            },
        ],
        output_filename: Some("test_moving_avg".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_sample_results(db, "test_moving_avg", &["id", "value_a", "value_a_ma3"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_zscore(processor: &ComputedColumnsProcessor, db: &Arc<Database>, output_dir: &Path) {
    let request = ComputedColumnsRequest {
        table_name: "test_data".to_string(),
        configurations: vec![
            ComputedColumnConfig {
                computation_type: ComputationType::ZScore,
                source_column: "value_a".to_string(),
                second_column: String::new(),
                output_name: "value_a_zscore".to_string(),
                window_size: 0,
                null_handling: NullHandling::SkipNulls,
            },
        ],
        output_filename: Some("test_zscore".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_sample_results(db, "test_zscore", &["id", "value_a", "value_a_zscore"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn test_with_nulls(processor: &ComputedColumnsProcessor, db: &Arc<Database>, output_dir: &Path) {
    // Create table with nulls
    let create_sql = r#"
        CREATE TABLE test_nulls (
            id INTEGER,
            value DOUBLE
        )
    "#;
    
    db.execute_sql(create_sql).expect("Failed to create table");
    
    // Insert data with nulls
    db.execute_sql("INSERT INTO test_nulls VALUES (1, 100.0)").unwrap();
    db.execute_sql("INSERT INTO test_nulls VALUES (2, NULL)").unwrap();
    db.execute_sql("INSERT INTO test_nulls VALUES (3, 150.0)").unwrap();
    db.execute_sql("INSERT INTO test_nulls VALUES (4, NULL)").unwrap();
    db.execute_sql("INSERT INTO test_nulls VALUES (5, 200.0)").unwrap();
    
    let request = ComputedColumnsRequest {
        table_name: "test_nulls".to_string(),
        configurations: vec![
            ComputedColumnConfig {
                computation_type: ComputationType::Delta,
                source_column: "value".to_string(),
                second_column: String::new(),
                output_name: "value_delta".to_string(),
                window_size: 0,
                null_handling: NullHandling::SkipNulls,
            },
            ComputedColumnConfig {
                computation_type: ComputationType::CumulativeSum,
                source_column: "value".to_string(),
                second_column: String::new(),
                output_name: "value_cumsum".to_string(),
                window_size: 0,
                null_handling: NullHandling::FillWithZero,
            },
        ],
        output_filename: Some("test_nulls".to_string()),
    };
    
    match processor.process_request(&request, db, output_dir) {
        Ok(filename) => {
            println!("✓ Created file: {}", filename);
            show_sample_results(db, "test_nulls", &["id", "value", "value_delta", "value_cumsum"]);
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}

fn show_sample_results(db: &Arc<Database>, table_name: &str, columns: &[&str]) {
    let cols = columns.join(", ");
    let query = format!("SELECT {} FROM \"{}\" LIMIT 5", cols, table_name);
    
    match db.execute_query(&query) {
        Ok(rows) => {
            println!("  Sample results:");
            // Print header
            println!("  {}", columns.join("\t"));
            // Print rows
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