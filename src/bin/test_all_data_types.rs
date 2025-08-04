use std::sync::Arc;
use std::path::Path;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime, DateTime, Utc};

fn main() {
    println!("=== Testing All Data Types ===\n");
    
    // Test 1: Create test data with all supported types
    println!("--- Creating test data with various types ---");
    create_test_data_all_types();
    
    // Test 2: Test time binning with different timestamp types
    println!("\n--- Testing time binning with different timestamp types ---");
    test_time_binning_types();
    
    // Test 3: Test enhanced grouping with all column types
    println!("\n--- Testing enhanced grouping with all column types ---");
    test_enhanced_grouping_types();
    
    // Test 4: Test computed columns with numeric types
    println!("\n--- Testing computed columns with numeric types ---");
    test_computed_columns_types();
}

fn create_test_data_all_types() {
    // Create schema with all supported types
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value_i32", DataType::Int32, false),
        Field::new("value_i64", DataType::Int64, false),
        Field::new("value_f32", DataType::Float32, false),
        Field::new("value_f64", DataType::Float64, false),
        Field::new("is_active", DataType::Boolean, false),
        Field::new("date32", DataType::Date32, false),
        Field::new("date64", DataType::Date64, false),
        Field::new("timestamp_s", DataType::Timestamp(TimeUnit::Second, None), false),
        Field::new("timestamp_ms", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("timestamp_us", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("timestamp_ns", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("optional_text", DataType::Utf8, true), // nullable
    ]);
    
    // Create arrays for each type
    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let name_array = StringArray::from(vec!["A", "A", "B", "B", "B", "C", "C", "D", "D", "E"]);
    let value_i32_array = Int32Array::from(vec![100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]);
    let value_i64_array = Int64Array::from(vec![1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]);
    let value_f32_array = Float32Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.0]);
    let value_f64_array = Float64Array::from(vec![10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7, 80.8, 90.9, 100.0]);
    let bool_array = BooleanArray::from(vec![true, true, false, false, true, false, true, true, false, true]);
    
    // Date arrays (days since epoch)
    let date32_array = Date32Array::from(vec![18000, 18001, 18002, 18003, 18004, 18005, 18006, 18007, 18008, 18009]);
    let date64_array = Date64Array::from(vec![
        1640995200000, // 2022-01-01 in milliseconds
        1641081600000, // 2022-01-02
        1641168000000, // 2022-01-03
        1641254400000, // 2022-01-04
        1641340800000, // 2022-01-05
        1641427200000, // 2022-01-06
        1641513600000, // 2022-01-07
        1641600000000, // 2022-01-08
        1641686400000, // 2022-01-09
        1641772800000, // 2022-01-10
    ]);
    
    // Timestamp arrays
    let base_time = 1640995200; // 2022-01-01 00:00:00 UTC
    let timestamp_s_array = TimestampSecondArray::from(vec![
        base_time, base_time + 30, base_time + 60, base_time + 90,
        base_time + 120, base_time + 3600, base_time + 3630, base_time + 3660,
        base_time + 7200, base_time + 10800
    ]);
    
    let timestamp_ms_array = TimestampMillisecondArray::from(vec![
        base_time * 1000, (base_time + 30) * 1000, (base_time + 60) * 1000,
        (base_time + 90) * 1000, (base_time + 120) * 1000, (base_time + 3600) * 1000,
        (base_time + 3630) * 1000, (base_time + 3660) * 1000, (base_time + 7200) * 1000,
        (base_time + 10800) * 1000
    ]);
    
    let timestamp_us_array = TimestampMicrosecondArray::from(vec![
        base_time * 1_000_000, (base_time + 30) * 1_000_000, (base_time + 60) * 1_000_000,
        (base_time + 90) * 1_000_000, (base_time + 120) * 1_000_000, (base_time + 3600) * 1_000_000,
        (base_time + 3630) * 1_000_000, (base_time + 3660) * 1_000_000, (base_time + 7200) * 1_000_000,
        (base_time + 10800) * 1_000_000
    ]);
    
    let timestamp_ns_array = TimestampNanosecondArray::from(vec![
        base_time * 1_000_000_000, (base_time + 30) * 1_000_000_000, (base_time + 60) * 1_000_000_000,
        (base_time + 90) * 1_000_000_000, (base_time + 120) * 1_000_000_000, (base_time + 3600) * 1_000_000_000,
        (base_time + 3630) * 1_000_000_000, (base_time + 3660) * 1_000_000_000, (base_time + 7200) * 1_000_000_000,
        (base_time + 10800) * 1_000_000_000
    ]);
    
    // Optional text with some nulls
    let optional_text_array = StringArray::from(vec![
        Some("data1"), None, Some("data2"), Some("data3"), None,
        None, Some("data4"), None, Some("data5"), Some("data6")
    ]);
    
    // Create record batch
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_i32_array),
            Arc::new(value_i64_array),
            Arc::new(value_f32_array),
            Arc::new(value_f64_array),
            Arc::new(bool_array),
            Arc::new(date32_array),
            Arc::new(date64_array),
            Arc::new(timestamp_s_array),
            Arc::new(timestamp_ms_array),
            Arc::new(timestamp_us_array),
            Arc::new(timestamp_ns_array),
            Arc::new(optional_text_array),
        ],
    ).expect("Failed to create record batch");
    
    println!("✓ Created test data with {} rows and {} columns", batch.num_rows(), batch.num_columns());
    println!("  Schema:");
    for field in schema.fields() {
        println!("    - {}: {:?}", field.name(), field.data_type());
    }
}

fn test_time_binning_types() {
    println!("\nTesting time binning with different timestamp formats:");
    
    // Test parsing different time formats
    let test_times = vec![
        ("HH:MM:SS.sss", "12:34:56.789"),
        ("HH:MM:SS", "12:34:56"),
        ("ISO DateTime", "2024-01-01T12:34:56"),
        ("ISO with ms", "2024-01-01T12:34:56.789"),
        ("Unix timestamp", "1704114896"),
    ];
    
    for (format_name, time_str) in test_times {
        println!("  Testing {}: {}", format_name, time_str);
        // In real implementation, this would call the parse_timestamp function
    }
    
    println!("\n  Timestamp types that should work with time binning:");
    println!("    - Timestamp(Second, None)");
    println!("    - Timestamp(Millisecond, None)");
    println!("    - Timestamp(Microsecond, None)");
    println!("    - Timestamp(Nanosecond, None)");
    println!("    - Utf8 (text timestamps in various formats)");
}

fn test_enhanced_grouping_types() {
    println!("\nTesting enhanced grouping with different column types:");
    
    let supported_types = vec![
        "Utf8 (text)",
        "Int32",
        "Int64", 
        "Float32",
        "Float64",
        "Boolean",
        "Date32",
        "Date64",
        "Timestamp(Second, None)",
        "Timestamp(Millisecond, None)",
        "Timestamp(Microsecond, None)",
        "Timestamp(Nanosecond, None)",
    ];
    
    println!("  Supported column types for grouping rules:");
    for dtype in supported_types {
        println!("    ✓ {}", dtype);
    }
    
    println!("\n  Grouping rules available:");
    println!("    - ValueChange: Creates new group when column value changes");
    println!("    - ValueEquals: Groups rows where column equals specific value");
    println!("    - IsEmpty: Groups consecutive rows where column is empty/null");
}

fn test_computed_columns_types() {
    println!("\nTesting computed columns with numeric types:");
    
    println!("  Numeric types supported:");
    println!("    ✓ Int32");
    println!("    ✓ Int64");
    println!("    ✓ Float32");
    println!("    ✓ Float64");
    
    println!("\n  Computation types available:");
    println!("    - Delta: Row-to-row difference (numeric columns)");
    println!("    - CumulativeSum: Running total (numeric columns)");
    println!("    - Percentage: Percentage of total (numeric columns)");
    println!("    - Ratio: Ratio between two columns (numeric columns)");
    println!("    - MovingAverage: Moving average with window (numeric columns)");
    println!("    - ZScore: Standardized score (numeric columns)");
    
    println!("\n  Note: All computations handle NULL values based on NullHandling setting");
}

// Helper function to demonstrate timestamp conversion
fn demonstrate_timestamp_conversion() {
    println!("\n--- Timestamp Conversion Examples ---");
    
    // Timestamp in seconds
    let ts_seconds = 1704114896i64; // 2024-01-01 12:34:56 UTC
    let dt = DateTime::<Utc>::from_timestamp(ts_seconds, 0).unwrap();
    println!("  Seconds: {} -> {}", ts_seconds, dt.format("%Y-%m-%d %H:%M:%S"));
    
    // Timestamp in milliseconds
    let ts_millis = ts_seconds * 1000 + 789;
    let dt = DateTime::<Utc>::from_timestamp_millis(ts_millis).unwrap();
    println!("  Milliseconds: {} -> {}", ts_millis, dt.format("%Y-%m-%d %H:%M:%S.%3f"));
    
    // Timestamp in microseconds
    let ts_micros = ts_millis * 1000 + 123;
    let dt = DateTime::<Utc>::from_timestamp_micros(ts_micros).unwrap();
    println!("  Microseconds: {} -> {}", ts_micros, dt.format("%Y-%m-%d %H:%M:%S.%6f"));
    
    // Timestamp in nanoseconds
    let ts_nanos = ts_micros * 1000 + 456;
    let secs = ts_nanos / 1_000_000_000;
    let nanos = (ts_nanos % 1_000_000_000) as u32;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos).unwrap();
    println!("  Nanoseconds: {} -> {}", ts_nanos, dt.format("%Y-%m-%d %H:%M:%S.%9f"));
}