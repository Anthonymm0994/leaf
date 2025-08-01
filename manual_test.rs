// Manual test for time-based grouping functionality
// This file can be run with: cargo run --bin manual_test

use std::sync::Arc;
use leaf::core::{Database, TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing Time-Based Grouping Functionality");
    println!("=============================================");
    
    // Test 1: Create database and import data
    println!("\n1. Creating test database...");
    let db = Database::open_writable("test_time_bin.db")?;
    
    println!("2. Importing test data...");
    db.stream_insert_csv("test_data/unix_timestamps.csv", "test_table", ',', true)?;
    
    // Test 2: Fixed interval grouping
    println!("\n3. Testing Fixed Interval Grouping (60 seconds)...");
    let config = TimeBinConfig {
        selected_table: "test_table".to_string(),
        selected_column: "timestamp".to_string(),
        strategy: TimeBinStrategy::FixedInterval {
            interval_seconds: 60,
            interval_format: "60".to_string(),
        },
        output_column_name: "time_bin".to_string(),
    };
    
    let db_arc = Arc::new(db);
    match TimeGroupingEngine::apply_grouping(&db_arc, &config) {
        Ok(output_table) => {
            println!("✅ Fixed interval grouping successful!");
            println!("   Output table: {}", output_table);
            
            // Verify results
            let query = format!("SELECT * FROM \"{}\" LIMIT 5", output_table);
            match db_arc.execute_query(&query) {
                Ok(rows) => {
                    println!("   First 5 rows:");
                    for (i, row) in rows.iter().enumerate() {
                        println!("   Row {}: {:?}", i, row);
                    }
                }
                Err(e) => println!("   ❌ Error querying results: {}", e),
            }
        }
        Err(e) => {
            println!("❌ Fixed interval grouping failed: {}", e);
            return Err(e.into());
        }
    }
    
    // Test 3: Threshold-based grouping
    println!("\n4. Testing Threshold-Based Grouping (30 seconds)...");
    let config2 = TimeBinConfig {
        selected_table: "test_table".to_string(),
        selected_column: "timestamp".to_string(),
        strategy: TimeBinStrategy::ThresholdBased {
            threshold_seconds: 30,
            threshold_format: "30".to_string(),
        },
        output_column_name: "time_bin_threshold".to_string(),
    };
    
    match TimeGroupingEngine::apply_grouping(&db_arc, &config2) {
        Ok(output_table) => {
            println!("✅ Threshold-based grouping successful!");
            println!("   Output table: {}", output_table);
        }
        Err(e) => {
            println!("❌ Threshold-based grouping failed: {}", e);
        }
    }
    
    println!("\n🎉 All tests completed!");
    Ok(())
} 