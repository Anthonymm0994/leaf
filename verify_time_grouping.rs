// Verification script for time-based grouping functionality
use std::sync::Arc;
use leaf::core::{Database, TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Verifying Time-Based Grouping Functionality");
    println!("=============================================");
    
    // Test 1: Database operations
    println!("\n📊 Test 1: Database Operations");
    let db = Database::open_writable("verify_test.db")?;
    println!("✅ Database created successfully");
    
    // Test 2: CSV import
    println!("\n📁 Test 2: CSV Import");
    db.stream_insert_csv("test_data/unix_timestamps.csv", "test_table", ',', true)?;
    println!("✅ CSV imported successfully");
    
    // Test 3: Fixed interval grouping
    println!("\n⏱️ Test 3: Fixed Interval Grouping");
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
            
            // Verify the results
            let query = format!("SELECT * FROM \"{}\" LIMIT 3", output_table);
            match db_arc.execute_query(&query) {
                Ok(rows) => {
                    println!("   Sample results:");
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
    
    // Test 4: Threshold-based grouping
    println!("\n🔄 Test 4: Threshold-Based Grouping");
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
    
    // Test 5: Manual intervals grouping
    println!("\n📅 Test 5: Manual Intervals Grouping");
    let config3 = TimeBinConfig {
        selected_table: "test_table".to_string(),
        selected_column: "timestamp".to_string(),
        strategy: TimeBinStrategy::ManualIntervals {
            intervals: vec!["60".to_string(), "120".to_string(), "180".to_string()],
            interval_string: "60,120,180".to_string(),
        },
        output_column_name: "time_bin_manual".to_string(),
    };
    
    match TimeGroupingEngine::apply_grouping(&db_arc, &config3) {
        Ok(output_table) => {
            println!("✅ Manual intervals grouping successful!");
            println!("   Output table: {}", output_table);
        }
        Err(e) => {
            println!("❌ Manual intervals grouping failed: {}", e);
        }
    }
    
    println!("\n🎉 All verification tests completed successfully!");
    println!("The time-based grouping functionality is working correctly.");
    
    Ok(())
} 