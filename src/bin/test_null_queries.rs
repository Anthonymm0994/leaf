use leaf::core::{Database, QueryExecutor};
use std::sync::Arc;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing NULL queries with test_data_300k_correct.csv");
    println!("{}", "=".repeat(60));
    
    // Create a new database
    let mut db = Database::open_writable("test_null_queries.db")?;
    
    // Load the CSV file
    println!("\n1. Loading test_data_300k_correct.csv...");
    let file_path = Path::new("test_data/large_files/test_data_300k_correct.csv");
    
    // Use stream_insert_csv_with_header_row which handles everything including type inference
    db.stream_insert_csv_with_header_row(
        "test_data",
        file_path,
        ',',
        0  // header row is at index 0
    )?;
    
    // Convert to Arc for query execution
    let db = Arc::new(db);
    
    println!("✓ Data loaded successfully");
    
    // Get total row count
    let total_count_query = "SELECT COUNT(*) FROM test_data";
    let total_result = QueryExecutor::execute(&db, total_count_query)?;
    println!("\nTotal rows in table: {}", total_result.rows[0][0]);
    
    // Create markdown output
    let mut output = String::new();
    output.push_str("# NULL Query Test Results\n\n");
    output.push_str("## Test Data Information\n\n");
    output.push_str(&format!("- **File**: test_data_300k_correct.csv\n"));
    output.push_str(&format!("- **Total Rows**: {}\n", total_result.rows[0][0]));
    output.push_str("- **Null Values Recognized**: Empty strings, 'NULL', 'null', 'N/A', '-' (default configuration)\n\n");
    
    // Test columns that should have nulls
    let test_columns = vec![
        "integer_infer_blank",
        "integer_infer_dash",
        "real_infer_blank",
        "real_infer_dash",
        "text_infer_blank",
        "text_infer_dash",
        "boolean_infer_blank",
        "boolean_infer_dash",
        "date_infer_blank",
        "date_infer_dash",
        "datetime_infer_blank",
        "datetime_infer_dash",
        "timeseconds_infer_blank",
        "timeseconds_infer_dash",
        "timemilliseconds_infer_blank",
        "timemilliseconds_infer_dash",
        "blob_infer_blank",
        "blob_infer_dash",
        "dumb_time",
    ];
    
    output.push_str("## NULL Query Tests by Column\n\n");
    
    for column in &test_columns {
        println!("\nTesting column: {}", column);
        output.push_str(&format!("### Column: `{}`\n\n", column));
        
        // Count NULL values
        let null_count_query = format!("SELECT COUNT(*) FROM test_data WHERE \"{}\" IS NULL", column);
        let null_result = QueryExecutor::execute(&db, &null_count_query)?;
        let null_count = &null_result.rows[0][0];
        
        // Count NOT NULL values
        let not_null_count_query = format!("SELECT COUNT(*) FROM test_data WHERE \"{}\" IS NOT NULL", column);
        let not_null_result = QueryExecutor::execute(&db, &not_null_count_query)?;
        let not_null_count = &not_null_result.rows[0][0];
        
        output.push_str(&format!("- **NULL count**: {}\n", null_count));
        output.push_str(&format!("- **NOT NULL count**: {}\n", not_null_count));
        
        // Show sample of NULL rows
        let sample_null_query = format!(
            "SELECT \"{}\" FROM test_data WHERE \"{}\" IS NULL LIMIT 5",
            column, column
        );
        let sample_null_result = QueryExecutor::execute(&db, &sample_null_query)?;
        
        output.push_str("\n**Sample NULL values (should show as empty):**\n```\n");
        for (i, row) in sample_null_result.rows.iter().enumerate() {
            output.push_str(&format!("Row {}: '{}'\n", i + 1, row[0]));
        }
        output.push_str("```\n");
        
        // Show sample of NOT NULL rows
        let sample_not_null_query = format!(
            "SELECT \"{}\" FROM test_data WHERE \"{}\" IS NOT NULL LIMIT 5",
            column, column
        );
        let sample_not_null_result = QueryExecutor::execute(&db, &sample_not_null_query)?;
        
        output.push_str("\n**Sample NOT NULL values:**\n```\n");
        for (i, row) in sample_not_null_result.rows.iter().enumerate() {
            output.push_str(&format!("Row {}: '{}'\n", i + 1, row[0]));
        }
        output.push_str("```\n\n");
    }
    
    // Test complex NULL queries
    output.push_str("## Complex NULL Queries\n\n");
    
    // Query 1: Multiple NULL conditions
    println!("\nTesting complex query 1: Multiple NULL conditions");
    output.push_str("### Query 1: Multiple NULL conditions\n\n");
    output.push_str("```sql\nSELECT COUNT(*) FROM test_data \nWHERE integer_infer_blank IS NULL \n  AND real_infer_blank IS NULL\n```\n\n");
    
    let complex_query1 = "SELECT COUNT(*) FROM test_data WHERE integer_infer_blank IS NULL AND real_infer_blank IS NULL";
    let complex_result1 = QueryExecutor::execute(&db, complex_query1)?;
    output.push_str(&format!("**Result**: {} rows\n\n", complex_result1.rows[0][0]));
    
    // Query 2: NULL in first row of major groups (dumb_time)
    println!("\nTesting complex query 2: NULL dumb_time (first rows of major groups)");
    output.push_str("### Query 2: First rows of major groups (dumb_time IS NULL)\n\n");
    output.push_str("```sql\nSELECT good_time, dumb_time, width, height \nFROM test_data \nWHERE dumb_time IS NULL \nLIMIT 10\n```\n\n");
    
    let complex_query2 = "SELECT good_time, dumb_time, width, height FROM test_data WHERE dumb_time IS NULL LIMIT 10";
    let complex_result2 = QueryExecutor::execute(&db, &complex_query2)?;
    
    output.push_str("| good_time | dumb_time | width | height |\n");
    output.push_str("|-----------|-----------|-------|--------|\n");
    for row in &complex_result2.rows {
        output.push_str(&format!("| {} | {} | {} | {} |\n", row[0], row[1], row[2], row[3]));
    }
    output.push_str("\n");
    
    // Query 3: Comparing NULL handling with empty strings
    println!("\nTesting complex query 3: NULL vs empty string comparison");
    output.push_str("### Query 3: NULL vs Empty String Comparison\n\n");
    
    // First, check if we have any actual empty strings (non-NULL)
    let empty_string_query = "SELECT COUNT(*) FROM test_data WHERE text_infer_blank = ''";
    let empty_result = QueryExecutor::execute(&db, &empty_string_query)?;
    output.push_str(&format!("- Rows where text_infer_blank = '' (empty string): {}\n", empty_result.rows[0][0]));
    
    let null_query = "SELECT COUNT(*) FROM test_data WHERE text_infer_blank IS NULL";
    let null_result = QueryExecutor::execute(&db, &null_query)?;
    output.push_str(&format!("- Rows where text_infer_blank IS NULL: {}\n\n", null_result.rows[0][0]));
    
    // Query 4: COALESCE function test
    println!("\nTesting complex query 4: COALESCE function");
    output.push_str("### Query 4: COALESCE Function Test\n\n");
    output.push_str("```sql\nSELECT \n  integer_infer_blank,\n  COALESCE(integer_infer_blank, -999) as with_default\nFROM test_data \nWHERE integer_infer_blank IS NULL \nLIMIT 5\n```\n\n");
    
    let coalesce_query = "SELECT integer_infer_blank, COALESCE(integer_infer_blank, -999) as with_default FROM test_data WHERE integer_infer_blank IS NULL LIMIT 5";
    let coalesce_result = QueryExecutor::execute(&db, &coalesce_query)?;
    
    output.push_str("| integer_infer_blank | with_default |\n");
    output.push_str("|---------------------|-------------|\n");
    for row in &coalesce_result.rows {
        output.push_str(&format!("| {} | {} |\n", row[0], row[1]));
    }
    output.push_str("\n");
    
    // Write results to markdown file
    let mut file = File::create("null_query_test_results.md")?;
    file.write_all(output.as_bytes())?;
    
    println!("\n✓ Test completed successfully!");
    println!("Results written to: null_query_test_results.md");
    
    Ok(())
}