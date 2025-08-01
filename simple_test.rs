use std::sync::Arc;
use leaf::core::database::Database;
use leaf::core::time_grouping::TimeGroupingEngine;
use leaf::ui::time_based_grouping::{TimeBasedGroupingConfig, GroupingStrategy};

fn main() {
    println!("🧪 Simple Time-Based Grouping Test");
    println!("===================================");
    
    // Create a test database
    let mut db = Database::open_writable(std::path::Path::new("test_data")).unwrap();
    
    // Import the test data
    let result = db.stream_insert_csv("unix_test", std::path::Path::new("test_data/unix_timestamps.csv"), ',', true);
    if let Err(e) = result {
        println!("❌ Failed to import CSV: {}", e);
        return;
    }
    
    println!("✅ Successfully imported CSV data");
    
    // Create configuration for fixed interval grouping
    let config = TimeBasedGroupingConfig {
        selected_table: "unix_test".to_string(),
        selected_column: "timestamp".to_string(),
        strategy: GroupingStrategy::FixedInterval {
            interval_seconds: 60, // 1 minute intervals
            interval_format: "60".to_string(),
        },
        output_column_name: "time_group".to_string(),
    };
    
    // Apply the grouping
    let db_arc = Arc::new(db);
    match TimeGroupingEngine::apply_grouping(&db_arc, &config) {
        Ok(output_table) => {
            println!("✅ Successfully created grouped table: {}", output_table);
            
            // Verify the results
            let query = format!("SELECT * FROM \"{}\"", output_table);
            match db_arc.execute_query(&query) {
                Ok(rows) => {
                    println!("📋 Result table has {} rows", rows.len());
                    for (i, row) in rows.iter().take(5).enumerate() {
                        println!("  Row {}: {:?}", i, row);
                    }
                }
                Err(e) => println!("❌ Failed to query result: {}", e),
            }
        }
        Err(e) => println!("❌ Failed to apply grouping: {}", e),
    }
    
    println!("✅ Test completed!");
} 