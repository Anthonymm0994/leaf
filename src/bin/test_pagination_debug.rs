use leaf::core::{Database, QueryExecutor};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing pagination functionality");
    println!("{}", "=".repeat(60));
    
    // Open database
    let db = Arc::new(Database::open_writable(".")?);
    
    // Test query
    let query = "SELECT * FROM \"test_data_300k\"";
    let page = 0;
    let page_size = 25;
    
    println!("Query: {}", query);
    println!("Page: {}, Page size: {}", page, page_size);
    
    // Execute with pagination
    match QueryExecutor::execute_with_pagination(&db, query, page, page_size) {
        Ok(result) => {
            println!("\nQuery executed successfully!");
            println!("Columns: {:?}", result.columns);
            println!("Rows returned: {}", result.rows.len());
            println!("Total rows: {:?}", result.total_rows);
            
            if let Some(total) = result.total_rows {
                let total_pages = ((total as f32) / (page_size as f32)).ceil() as usize;
                println!("Total pages: {}", total_pages);
                
                // Check button states
                let prev_enabled = page > 0;
                let next_enabled = page + 1 < total_pages;
                println!("\nButton states:");
                println!("  Previous enabled: {}", prev_enabled);
                println!("  Next enabled: {}", next_enabled);
            } else {
                println!("WARNING: total_rows is None!");
            }
        }
        Err(e) => {
            println!("Error executing query: {}", e);
        }
    }
    
    Ok(())
}