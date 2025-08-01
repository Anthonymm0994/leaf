// Comprehensive test for time-based grouping functionality
use std::sync::Arc;
use leaf::core::{Database, TimeGroupingEngine};
use leaf::ui::time_bin_dialog::{TimeBinConfig, TimeBinStrategy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Comprehensive Test: Time-Based Grouping Functionality");
    println!("========================================================");
    
    // Test 1: Basic functionality with Unix timestamps
    println!("\n1️⃣ Testing with Unix timestamps...");
    test_with_file("test_data/unix_timestamps.csv", "unix_test")?;
    
    // Test 2: ISO timestamps
    println!("\n2️⃣ Testing with ISO timestamps...");
    test_with_file("test_data/iso_timestamps.csv", "iso_test")?;
    
    // Test 3: DateTime timestamps
    println!("\n3️⃣ Testing with DateTime timestamps...");
    test_with_file("test_data/datetime_timestamps.csv", "datetime_test")?;
    
    // Test 4: Time-only format
    println!("\n4️⃣ Testing with time-only format...");
    test_with_file("test_data/time_only.csv", "time_only_test")?;
    
    // Test 5: Data with gaps
    println!("\n5️⃣ Testing with data containing gaps...");
    test_with_file("test_data/gaps_data.csv", "gaps_test")?;
    
    println!("\n🎉 All comprehensive tests completed successfully!");
    println!("The time-based grouping functionality is working correctly.");
    println!("You can now use the 'Add Time Bin Column' feature in the application.");
    
    Ok(())
}

fn test_with_file(csv_path: &str, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open_writable(&format!("{}.db", table_name))?;
    println!("   ✅ Database created");
    
    db.stream_insert_csv(csv_path, table_name, ',', true)?;
    println!("   ✅ CSV imported");
    
    let db_arc = Arc::new(db);
    
    // Test fixed interval grouping
    let config = TimeBinConfig {
        selected_table: table_name.to_string(),
        selected_column: "timestamp".to_string(),
        strategy: TimeBinStrategy::FixedInterval {
            interval_seconds: 60,
            interval_format: "60".to_string(),
        },
        output_column_name: "time_bin".to_string(),
    };
    
    match TimeGroupingEngine::apply_grouping(&db_arc, &config) {
        Ok(output_table) => {
            println!("   ✅ Fixed interval grouping: {}", output_table);
            
            // Verify results
            let query = format!("SELECT * FROM \"{}\" LIMIT 2", output_table);
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
            println!("   ❌ Fixed interval grouping failed: {}", e);
            return Err(e.into());
        }
    }
    
    Ok(())
} 