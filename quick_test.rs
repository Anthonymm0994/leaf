// Quick test for time-based grouping functionality
use std::sync::Arc;
use leaf::core::{Database, TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};

fn main() {
    println!("Quick test for time-based grouping...");
    
    // Test database creation
    match Database::open_writable("quick_test.db") {
        Ok(db) => {
            println!("✅ Database created");
            
            // Test CSV import
            match db.stream_insert_csv("test_data/unix_timestamps.csv", "test_table", ',', true) {
                Ok(_) => {
                    println!("✅ CSV imported");
                    
                    // Test time grouping
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
                            println!("✅ Time grouping successful!");
                            println!("Output table: {}", output_table);
                        }
                        Err(e) => {
                            println!("❌ Time grouping failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ CSV import failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("❌ Database creation failed: {}", e);
        }
    }
} 