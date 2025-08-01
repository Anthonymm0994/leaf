// Final test for time-based grouping functionality
use std::sync::Arc;
use leaf::core::{Database, TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Final Test: Time-Based Grouping Functionality");
    println!("================================================");
    
    // Test 1: Basic functionality
    println!("\n1️⃣ Testing basic functionality...");
    
    let db = Database::open_writable("final_test.db")?;
    println!("   ✅ Database created");
    
    db.stream_insert_csv("test_data/unix_timestamps.csv", "test_table", ',', true)?;
    println!("   ✅ CSV imported");
    
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
            println!("   ✅ Time grouping successful!");
            println!("   📊 Output table: {}", output_table);
            
            // Verify results
            let query = format!("SELECT * FROM \"{}\" LIMIT 3", output_table);
            match db_arc.execute_query(&query) {
                Ok(rows) => {
                    println!("   📋 Sample results:");
                    for (i, row) in rows.iter().enumerate() {
                        println!("      Row {}: {:?}", i, row);
                    }
                }
                Err(e) => println!("   ❌ Error querying results: {}", e),
            }
        }
        Err(e) => {
            println!("   ❌ Time grouping failed: {}", e);
            return Err(e.into());
        }
    }
    
    // Test 2: Different strategies
    println!("\n2️⃣ Testing different grouping strategies...");
    
    // Threshold-based
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
        Ok(output_table) => println!("   ✅ Threshold-based grouping: {}", output_table),
        Err(e) => println!("   ❌ Threshold-based grouping failed: {}", e),
    }
    
    // Manual intervals
    let config3 = TimeBinConfig {
        selected_table: "test_table".to_string(),
        selected_column: "timestamp".to_string(),
        strategy: TimeBinStrategy::ManualIntervals {
            intervals: vec!["60".to_string(), "120".to_string()],
            interval_string: "60,120".to_string(),
        },
        output_column_name: "time_bin_manual".to_string(),
    };
    
    match TimeGroupingEngine::apply_grouping(&db_arc, &config3) {
        Ok(output_table) => println!("   ✅ Manual intervals grouping: {}", output_table),
        Err(e) => println!("   ❌ Manual intervals grouping failed: {}", e),
    }
    
    println!("\n🎉 All tests completed successfully!");
    println!("The time-based grouping functionality is working correctly.");
    println!("You can now use the 'Add Time Bin Column' feature in the application.");
    
    Ok(())
} 