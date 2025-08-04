use leaf::core::database::Database;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing time binning query behavior");
    println!("{}", "=".repeat(60));
    
    // Open the database
    let mut db = Database::open_writable(".")?;
    
    // Load the Arrow file
    db.load_table_arrow_ipc("test_data_300k_grouped", std::path::Path::new("test_data/test_data_300k_grouped.arrow"))?;
    
    // Create Arc for database
    let db_arc = Arc::new(db);
    
    // Query the good_time column (same as time bin dialog does)
    let query = "SELECT \"good_time\" FROM \"test_data_300k_grouped\" LIMIT 10";
    println!("\nExecuting query: {}", query);
    
    match db_arc.execute_query(query) {
        Ok(rows) => {
            println!("Query returned {} rows", rows.len());
            for (i, row) in rows.iter().enumerate() {
                if !row.is_empty() {
                    println!("  Row {}: '{}'", i, row[0]);
                }
            }
        }
        Err(e) => {
            println!("Query failed: {}", e);
        }
    }
    
    // Also test dumb_time column
    let query2 = "SELECT \"dumb_time\" FROM \"test_data_300k_grouped\" LIMIT 10";
    println!("\n\nExecuting query: {}", query2);
    
    match db_arc.execute_query(query2) {
        Ok(rows) => {
            println!("Query returned {} rows", rows.len());
            for (i, row) in rows.iter().enumerate() {
                if !row.is_empty() && !row[0].is_empty() {
                    println!("  Row {}: '{}'", i, row[0]);
                }
            }
        }
        Err(e) => {
            println!("Query failed: {}", e);
        }
    }
    
    Ok(())
}