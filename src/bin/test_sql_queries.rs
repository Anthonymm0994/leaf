use anyhow::Result;
use std::sync::Arc;
use leaf::core::{Database, QueryExecutor};

fn main() -> Result<()> {
    println!("=== SQL Query Test Suite ===\n");
    
    // Extract test data if needed
    let csv_path = "data_gen_scripts/test_data_300k.csv";
    if !std::path::Path::new(csv_path).exists() {
        println!("Extracting test_data_300k.csv...");
        std::process::Command::new("tar")
            .args(&["-xzf", "data_gen_scripts/test_data_300k.tar.gz", "-C", "data_gen_scripts/"])
            .output()?;
    }
    
    // Create database and import test data
    let mut db = Database::open_writable(".")?;
    println!("Importing test data...");
    db.stream_insert_csv_with_header_row("test_data", std::path::Path::new(csv_path), ',', 0)?;
    
    let db_arc = Arc::new(db);
    
    // Get column info
    println!("\nTable Structure:");
    let info_query = "SELECT * FROM test_data LIMIT 1";
    match QueryExecutor::execute(&db_arc, info_query) {
        Ok(result) => {
            println!("Columns: {:?}", result.columns);
            println!("Types: {:?}", result.column_types);
        }
        Err(e) => println!("Error getting table info: {}", e),
    }
    
    println!("\n{}\n", "=".repeat(80));
    
    // Test various queries
    let test_queries = vec![
        // Basic SELECT
        ("Basic SELECT", "SELECT * FROM test_data LIMIT 5"),
        
        // WHERE with string equality (correct SQL syntax)
        ("String equality (single quotes)", "SELECT * FROM test_data WHERE category_3 = 'A' LIMIT 5"),
        ("String equality (double quotes for identifier)", "SELECT * FROM test_data WHERE \"category_3\" = 'A' LIMIT 5"),
        
        // WHERE with numeric comparison
        ("Numeric comparison", "SELECT * FROM test_data WHERE width > 50 LIMIT 5"),
        ("Numeric range", "SELECT * FROM test_data WHERE height BETWEEN 20 AND 30 LIMIT 5"),
        
        // Boolean conditions
        ("Boolean true", "SELECT * FROM test_data WHERE isGood = true LIMIT 5"),
        ("Boolean false", "SELECT * FROM test_data WHERE isOld = false LIMIT 5"),
        
        // Time comparisons
        ("Time comparison", "SELECT * FROM test_data WHERE good_time > '12:00:00' LIMIT 5"),
        ("Time range", "SELECT * FROM test_data WHERE good_time BETWEEN '10:00:00' AND '11:00:00' LIMIT 5"),
        
        // NULL handling
        ("NULL check", "SELECT * FROM test_data WHERE text_infer_blank IS NULL LIMIT 5"),
        ("NOT NULL check", "SELECT * FROM test_data WHERE text_infer_blank IS NOT NULL LIMIT 5"),
        
        // Pattern matching
        ("LIKE pattern", "SELECT * FROM test_data WHERE tags LIKE '%tag1%' LIMIT 5"),
        
        // IN operator
        ("IN operator", "SELECT * FROM test_data WHERE category_4 IN ('X', 'Y', 'Z') LIMIT 5"),
        
        // Complex conditions
        ("AND condition", "SELECT * FROM test_data WHERE width > 30 AND height < 40 LIMIT 5"),
        ("OR condition", "SELECT * FROM test_data WHERE category_3 = 'A' OR category_3 = 'B' LIMIT 5"),
        
        // Aggregations
        ("COUNT", "SELECT COUNT(*) FROM test_data"),
        ("COUNT with WHERE", "SELECT COUNT(*) FROM test_data WHERE isGood = true"),
        ("GROUP BY", "SELECT category_3, COUNT(*) as count FROM test_data GROUP BY category_3 ORDER BY count DESC LIMIT 10"),
        ("Multiple aggregates", "SELECT MIN(width), MAX(width), AVG(width), COUNT(*) FROM test_data"),
        
        // ORDER BY
        ("ORDER BY ASC", "SELECT good_time, width FROM test_data ORDER BY good_time ASC LIMIT 10"),
        ("ORDER BY DESC", "SELECT good_time, height FROM test_data ORDER BY height DESC LIMIT 10"),
        
        // DISTINCT
        ("DISTINCT values", "SELECT DISTINCT category_3 FROM test_data ORDER BY category_3"),
        
        // Pagination test
        ("Pagination test", "SELECT * FROM test_data LIMIT 10 OFFSET 20"),
    ];
    
    for (name, query) in test_queries {
        println!("Test: {}", name);
        println!("Query: {}", query);
        
        match QueryExecutor::execute_with_pagination(&db_arc, query, 0, 10) {
            Ok(result) => {
                println!("✓ Success!");
                if let Some(total) = result.total_rows {
                    println!("  Total rows: {}", total);
                }
                println!("  Returned rows: {}", result.rows.len());
                
                // Show first few results for non-aggregate queries
                if !query.contains("COUNT(") && !query.contains("MIN(") && !query.contains("MAX(") {
                    for (i, row) in result.rows.iter().take(3).enumerate() {
                        let row_str: Vec<String> = row.iter()
                            .take(5) // Show first 5 columns max
                            .map(|v| {
                                if v.len() > 20 {
                                    format!("{}...", &v[..17])
                                } else {
                                    v.clone()
                                }
                            })
                            .collect();
                        println!("  Row {}: {:?}", i, row_str);
                    }
                } else {
                    // For aggregate queries, show the result
                    if let Some(row) = result.rows.first() {
                        println!("  Result: {:?}", row);
                    }
                }
            }
            Err(e) => {
                println!("✗ Error: {}", e);
            }
        }
        println!();
    }
    
    // Test common SQL errors
    println!("\n{}", "=".repeat(80));
    println!("\nTesting Common SQL Errors:");
    println!("(These should fail with helpful error messages)\n");
    
    let error_queries = vec![
        ("Double quotes for string literal (wrong)", "SELECT * FROM test_data WHERE category_3 = \"A\""),
        ("Missing quotes around string", "SELECT * FROM test_data WHERE category_3 = A"),
        ("Wrong operator for equality", "SELECT * FROM test_data WHERE category_3 == 'A'"),
        ("Invalid column name", "SELECT * FROM test_data WHERE nonexistent_column = 'A'"),
        ("Invalid table name", "SELECT * FROM nonexistent_table"),
    ];
    
    for (name, query) in error_queries {
        println!("Error test: {}", name);
        println!("Query: {}", query);
        
        match QueryExecutor::execute(&db_arc, query) {
            Ok(_) => println!("✗ Unexpected success!"),
            Err(e) => println!("✓ Expected error: {}", e),
        }
        println!();
    }
    
    Ok(())
}