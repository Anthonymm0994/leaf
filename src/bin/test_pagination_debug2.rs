use leaf::core::{Database, QueryExecutor};
use std::sync::Arc;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing pagination functionality");
    println!("{}", "=".repeat(60));
    
    // Open database
    let mut db_mut = Database::open_writable(".")?;
    
    // Load the arrow file first
    let arrow_path = Path::new("test_data/test_data_300k.arrow");
    if arrow_path.exists() {
        println!("Loading test_data_300k.arrow...");
        db_mut.load_table_arrow_ipc("test_data_300k", arrow_path)?;
        println!("Table loaded successfully!");
    } else {
        println!("Error: test_data_300k.arrow not found!");
        return Ok(());
    }
    
    // Convert to Arc for query execution
    let db = Arc::new(db_mut);
    
    // Test query
    let query = "SELECT * FROM \"test_data_300k\"";
    let page = 0;
    let page_size = 25;
    
    println!("\nQuery: {}", query);
    println!("Page: {}, Page size: {}", page, page_size);
    
    // Execute with pagination
    match QueryExecutor::execute_with_pagination(&db, query, page, page_size) {
        Ok(result) => {
            println!("\nQuery executed successfully!");
            println!("Columns: {} columns", result.columns.len());
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
            
            // Show first few values
            if !result.rows.is_empty() {
                println!("\nFirst row sample:");
                for (i, val) in result.rows[0].iter().enumerate().take(5) {
                    println!("  {}: {}", result.columns.get(i).unwrap_or(&"?".to_string()), val);
                }
            }
        }
        Err(e) => {
            println!("Error executing query: {}", e);
        }
    }
    
    Ok(())
}