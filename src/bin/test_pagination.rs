use anyhow::Result;
use std::sync::Arc;
use leaf::core::{Database, QueryExecutor};

fn main() -> Result<()> {
    println!("=== Pagination Test ===\n");
    
    // Setup database
    let csv_path = "data_gen_scripts/test_data_300k.csv";
    if !std::path::Path::new(csv_path).exists() {
        println!("Extracting test_data_300k.csv...");
        std::process::Command::new("tar")
            .args(&["-xzf", "data_gen_scripts/test_data_300k.tar.gz", "-C", "data_gen_scripts/"])
            .output()?;
    }
    
    let mut db = Database::open_writable(".")?;
    println!("Importing test data...");
    db.stream_insert_csv_with_header_row("test_data_300k", std::path::Path::new(csv_path), ',', 0)?;
    let db_arc = Arc::new(db);
    
    // Test pagination with different page sizes
    let test_cases = vec![
        ("Simple SELECT", "SELECT * FROM test_data_300k", 10),
        ("With WHERE clause", "SELECT * FROM test_data_300k WHERE width > 100", 25),
        ("With ORDER BY", "SELECT * FROM test_data_300k ORDER BY good_time", 5),
        ("Complex query", "SELECT good_time, width, height FROM test_data_300k WHERE \"isGood\" = true ORDER BY width DESC", 20),
    ];
    
    for (name, query, page_size) in test_cases {
        println!("Test: {}", name);
        println!("Query: {}", query);
        println!("Page size: {}", page_size);
        
        // Test first 3 pages
        for page in 0..3 {
            match QueryExecutor::execute_with_pagination(&db_arc, query, page, page_size) {
                Ok(result) => {
                    let total_rows = result.total_rows.unwrap_or(0);
                    let total_pages = (total_rows as f32 / page_size as f32).ceil() as usize;
                    
                    println!("  Page {}/{}: {} rows returned (total: {} rows)", 
                        page + 1, total_pages, result.rows.len(), total_rows);
                    
                    // Show first row of each page
                    if let Some(first_row) = result.rows.first() {
                        let preview: Vec<String> = first_row.iter()
                            .take(3)
                            .map(|v| if v.len() > 15 { format!("{}...", &v[..12]) } else { v.clone() })
                            .collect();
                        println!("    First row: {:?}", preview);
                    }
                    
                    // Verify pagination math
                    let expected_rows = if page + 1 < total_pages {
                        page_size
                    } else if page < total_pages {
                        total_rows - (page * page_size)
                    } else {
                        0
                    };
                    
                    if result.rows.len() != expected_rows {
                        println!("    WARNING: Expected {} rows but got {}", expected_rows, result.rows.len());
                    }
                }
                Err(e) => {
                    println!("  Error on page {}: {}", page, e);
                    break;
                }
            }
        }
        println!();
    }
    
    // Test edge cases
    println!("=== Edge Case Tests ===\n");
    
    // Large page size
    println!("Test: Large page size (1000)");
    match QueryExecutor::execute_with_pagination(&db_arc, "SELECT * FROM test_data_300k", 0, 1000) {
        Ok(result) => {
            println!("  Success: {} rows returned (total: {:?})", 
                result.rows.len(), result.total_rows);
        }
        Err(e) => println!("  Error: {}", e),
    }
    
    // Page beyond data
    println!("\nTest: Page beyond available data");
    match QueryExecutor::execute_with_pagination(&db_arc, "SELECT * FROM test_data_300k", 1000, 100) {
        Ok(result) => {
            println!("  Success: {} rows returned (expected 0)", result.rows.len());
        }
        Err(e) => println!("  Error: {}", e),
    }
    
    // Query with no results
    println!("\nTest: Query with no results");
    match QueryExecutor::execute_with_pagination(&db_arc, "SELECT * FROM test_data_300k WHERE width > 99999", 0, 10) {
        Ok(result) => {
            println!("  Success: {} rows returned (total: {:?})", 
                result.rows.len(), result.total_rows);
            if result.total_rows != Some(0) {
                println!("  WARNING: Expected total_rows to be 0 for query with no results");
            }
        }
        Err(e) => println!("  Error: {}", e),
    }
    
    Ok(())
}