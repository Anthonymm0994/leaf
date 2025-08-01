use std::sync::Arc;
use leaf::core::{Database, TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Time-Based Grouping functionality...");
    
    // Create a test database
    let db = Database::open_writable("test_time_bin.db")?;
    
    // Import test data
    println!("Importing test data...");
    db.stream_insert_csv("test_data/unix_timestamps.csv", "test_table", ',', true)?;
    
    // Test configuration
    let config = TimeBinConfig {
        selected_table: "test_table".to_string(),
        selected_column: "timestamp".to_string(),
        strategy: TimeBinStrategy::FixedInterval {
            interval_seconds: 60,
            interval_format: "60".to_string(),
        },
        output_column_name: "time_bin".to_string(),
    };
    
    println!("Applying time-based grouping...");
    let db_arc = Arc::new(db);
    let result = TimeGroupingEngine::apply_grouping(&db_arc, &config);
    
    match result {
        Ok(output_table) => {
            println!("✅ Success! Output table: {}", output_table);
            
            // Verify the result by querying the new table
            let query = format!("SELECT * FROM \"{}\" LIMIT 5", output_table);
            let rows = db_arc.execute_query(&query)?;
            
            println!("First 5 rows of grouped data:");
            for (i, row) in rows.iter().enumerate() {
                println!("Row {}: {:?}", i, row);
            }
            
            Ok(())
        }
        Err(e) => {
            println!("❌ Error: {}", e);
            Err(e.into())
        }
    }
} 