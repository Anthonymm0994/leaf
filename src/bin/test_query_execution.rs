use anyhow::Result;
use std::sync::Arc;
use leaf::core::{Database, QueryExecutor};

fn main() -> Result<()> {
    println!("=== Testing Query Execution ===\n");
    
    // Open database
    let db = Arc::new(Database::open_readonly(".")?);
    
    // List tables
    let tables = db.list_tables()?;
    println!("Available tables:");
    for table in &tables {
        println!("  - {}", table);
    }
    
    if tables.is_empty() {
        println!("No tables found. Please import some data first.");
        return Ok(());
    }
    
    // Test query on first table
    let table_name = &tables[0];
    let query = format!("SELECT * FROM \"{}\"", table_name);
    
    println!("\nTesting query: {}", query);
    println!("Page: 0, Page size: 10");
    
    match QueryExecutor::execute_with_pagination(&db, &query, 0, 10) {
        Ok(result) => {
            println!("Success!");
            println!("Total rows: {:?}", result.total_rows);
            println!("Returned rows: {}", result.rows.len());
            println!("Columns: {:?}", result.columns);
            
            // Show first few rows
            for (i, row) in result.rows.iter().take(3).enumerate() {
                println!("Row {}: {:?}", i, row);
            }
        }
        Err(e) => {
            println!("Error executing query: {}", e);
        }
    }
    
    Ok(())
}