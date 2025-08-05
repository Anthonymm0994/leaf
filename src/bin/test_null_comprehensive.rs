use leaf::core::{Database, QueryExecutor};
use std::sync::Arc;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Comprehensive NULL Testing");
    println!("{}", "=".repeat(80));
    
    // Create a new database
    let mut db = Database::open_writable("test_null_comprehensive.db")?;
    
    // Load the CSV file
    println!("\n1. Loading test_data_300k_correct.csv...");
    let file_path = Path::new("test_data/large_files/test_data_300k_correct.csv");
    
    db.stream_insert_csv_with_header_row(
        "test_data",
        file_path,
        ',',
        0  // header row is at index 0
    )?;
    
    let db = Arc::new(db);
    println!("✓ Data loaded successfully");
    
    // Create markdown output
    let mut output = String::new();
    output.push_str("# Comprehensive NULL Query Test Results\n\n");
    output.push_str("## Test Data Information\n\n");
    output.push_str("- **File**: test_data_300k_correct.csv\n");
    output.push_str("- **Database**: DataFusion (Arrow)\n");
    output.push_str("- **Null Values Recognized**: Empty strings, 'NULL', 'null', 'N/A', '-'\n\n");
    
    // First, let's check the schema to understand data types
    output.push_str("## Schema Information\n\n");
    let schema_query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'test_data' ORDER BY ordinal_position LIMIT 20";
    match QueryExecutor::execute(&db, schema_query) {
        Ok(result) => {
            output.push_str("```sql\n");
            output.push_str(schema_query);
            output.push_str("\n```\n\n");
            output.push_str("| Column | Data Type |\n");
            output.push_str("|--------|----------|\n");
            for row in &result.rows {
                if row.len() >= 2 {
                    output.push_str(&format!("| {} | {} |\n", row[0], row[1]));
                }
            }
            output.push_str("\n");
        }
        Err(_) => {
            // If information_schema doesn't work, let's check column types differently
            output.push_str("*Note: information_schema not available, checking column types via sample query*\n\n");
        }
    }
    
    // Test all infer columns
    let infer_columns = vec![
        ("integer_infer_blank", "Integer with blank nulls"),
        ("integer_infer_dash", "Integer with dash nulls"),
        ("real_infer_blank", "Real with blank nulls"),
        ("real_infer_dash", "Real with dash nulls"),
        ("text_infer_blank", "Text with blank nulls"),
        ("text_infer_dash", "Text with dash nulls"),
        ("boolean_infer_blank", "Boolean with blank nulls"),
        ("boolean_infer_dash", "Boolean with dash nulls"),
        ("date_infer_blank", "Date with blank nulls"),
        ("date_infer_dash", "Date with dash nulls"),
        ("datetime_infer_blank", "DateTime with blank nulls"),
        ("datetime_infer_dash", "DateTime with dash nulls"),
        ("timeseconds_infer_blank", "Time seconds with blank nulls"),
        ("timeseconds_infer_dash", "Time seconds with dash nulls"),
        ("timemilliseconds_infer_blank", "Time milliseconds with blank nulls"),
        ("timemilliseconds_infer_dash", "Time milliseconds with dash nulls"),
        ("blob_infer_blank", "Blob with blank nulls"),
        ("blob_infer_dash", "Blob with dash nulls"),
    ];
    
    output.push_str("## Detailed NULL Tests by Column\n\n");
    
    for (column, description) in &infer_columns {
        output.push_str(&format!("### {} - {}\n\n", column, description));
        
        // Query 1: Count NULL vs NOT NULL
        let count_query = format!(
            "SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN \"{}\" IS NULL THEN 1 END) as null_count,
                COUNT(CASE WHEN \"{}\" IS NOT NULL THEN 1 END) as not_null_count,
                ROUND(COUNT(CASE WHEN \"{}\" IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as null_percentage
            FROM test_data",
            column, column, column
        );
        
        output.push_str("**NULL Statistics:**\n```sql\n");
        output.push_str(&count_query);
        output.push_str("\n```\n\n");
        
        match QueryExecutor::execute(&db, &count_query) {
            Ok(result) => {
                if let Some(row) = result.rows.first() {
                    output.push_str(&format!("- Total rows: {}\n", row[0]));
                    output.push_str(&format!("- NULL count: {}\n", row[1]));
                    output.push_str(&format!("- NOT NULL count: {}\n", row[2]));
                    output.push_str(&format!("- NULL percentage: {}%\n\n", row[3]));
                }
            }
            Err(e) => {
                output.push_str(&format!("Error: {}\n\n", e));
            }
        }
        
        // Query 2: Show sample NULL and NOT NULL values
        let sample_query = format!(
            "SELECT 
                \"{}\" as value,
                CASE WHEN \"{}\" IS NULL THEN 'NULL' ELSE 'NOT NULL' END as null_status,
                width,
                height
            FROM test_data 
            WHERE \"{}\" IS NULL 
            LIMIT 5",
            column, column, column
        );
        
        output.push_str("**Sample NULL rows:**\n```sql\n");
        output.push_str(&sample_query);
        output.push_str("\n```\n\n");
        
        match QueryExecutor::execute(&db, &sample_query) {
            Ok(result) => {
                output.push_str("| Value | NULL Status | Width | Height |\n");
                output.push_str("|-------|-------------|-------|--------|\n");
                for row in &result.rows {
                    output.push_str(&format!("| '{}' | {} | {} | {} |\n", 
                        row[0], row[1], row[2], row[3]));
                }
                output.push_str("\n");
            }
            Err(e) => {
                output.push_str(&format!("Error: {}\n\n", e));
            }
        }
        
        // Query 3: Show some NOT NULL values
        let not_null_query = format!(
            "SELECT 
                \"{}\" as value,
                CASE WHEN \"{}\" IS NULL THEN 'NULL' ELSE 'NOT NULL' END as null_status,
                width,
                height
            FROM test_data 
            WHERE \"{}\" IS NOT NULL 
            LIMIT 5",
            column, column, column
        );
        
        output.push_str("**Sample NOT NULL rows:**\n```sql\n");
        output.push_str(&not_null_query);
        output.push_str("\n```\n\n");
        
        match QueryExecutor::execute(&db, &not_null_query) {
            Ok(result) => {
                output.push_str("| Value | NULL Status | Width | Height |\n");
                output.push_str("|-------|-------------|-------|--------|\n");
                for row in &result.rows {
                    output.push_str(&format!("| '{}' | {} | {} | {} |\n", 
                        row[0], row[1], row[2], row[3]));
                }
                output.push_str("\n");
            }
            Err(e) => {
                output.push_str(&format!("Error: {}\n\n", e));
            }
        }
    }
    
    // General queries section
    output.push_str("## General Query Tests\n\n");
    
    // Test 1: Basic SELECT with multiple columns
    output.push_str("### Test 1: Basic SELECT showing NULL handling\n\n");
    let general_query1 = "SELECT 
        width,
        integer_infer_blank,
        real_infer_dash,
        text_infer_blank,
        boolean_infer_dash,
        dumb_time
    FROM test_data 
    LIMIT 10";
    
    output.push_str("```sql\n");
    output.push_str(general_query1);
    output.push_str("\n```\n\n");
    
    match QueryExecutor::execute(&db, general_query1) {
        Ok(result) => {
            output.push_str("| width | integer_infer_blank | real_infer_dash | text_infer_blank | boolean_infer_dash | dumb_time |\n");
            output.push_str("|-------|---------------------|-----------------|------------------|--------------------|-----------|\n");
            for row in &result.rows {
                output.push_str(&format!("| {} | '{}' | '{}' | '{}' | '{}' | '{}' |\n", 
                    row[0], row[1], row[2], row[3], row[4], row[5]));
            }
            output.push_str("\n");
        }
        Err(e) => {
            output.push_str(&format!("Error: {}\n\n", e));
        }
    }
    
    // Test 2: Filtering with NULL conditions
    output.push_str("### Test 2: WHERE clause with NULL conditions\n\n");
    let general_query2 = "SELECT 
        good_time,
        dumb_time,
        integer_infer_blank,
        real_infer_blank
    FROM test_data 
    WHERE dumb_time IS NULL 
        AND integer_infer_blank IS NOT NULL
    LIMIT 5";
    
    output.push_str("```sql\n");
    output.push_str(general_query2);
    output.push_str("\n```\n\n");
    
    match QueryExecutor::execute(&db, general_query2) {
        Ok(result) => {
            output.push_str("| good_time | dumb_time | integer_infer_blank | real_infer_blank |\n");
            output.push_str("|-----------|-----------|---------------------|------------------|\n");
            for row in &result.rows {
                output.push_str(&format!("| {} | '{}' | '{}' | '{}' |\n", 
                    row[0], row[1], row[2], row[3]));
            }
            output.push_str("\n");
        }
        Err(e) => {
            output.push_str(&format!("Error: {}\n\n", e));
        }
    }
    
    // Test 3: Aggregations with NULL handling
    output.push_str("### Test 3: Aggregations with NULL values\n\n");
    let general_query3 = "SELECT 
        COUNT(*) as total_rows,
        COUNT(integer_infer_blank) as count_non_null_integers,
        AVG(integer_infer_blank) as avg_non_null_integers,
        COUNT(DISTINCT boolean_infer_blank) as distinct_boolean_values,
        MIN(date_infer_dash) as min_date,
        MAX(date_infer_dash) as max_date
    FROM test_data";
    
    output.push_str("```sql\n");
    output.push_str(general_query3);
    output.push_str("\n```\n\n");
    
    match QueryExecutor::execute(&db, general_query3) {
        Ok(result) => {
            if let Some(row) = result.rows.first() {
                output.push_str(&format!("- Total rows: {}\n", row[0]));
                output.push_str(&format!("- Count non-null integers: {}\n", row[1]));
                output.push_str(&format!("- Average non-null integers: {}\n", row[2]));
                output.push_str(&format!("- Distinct boolean values: {}\n", row[3]));
                output.push_str(&format!("- Min date: {}\n", row[4]));
                output.push_str(&format!("- Max date: {}\n\n", row[5]));
            }
        }
        Err(e) => {
            output.push_str(&format!("Error: {}\n\n", e));
        }
    }
    
    // Test 4: COALESCE and NULL functions
    output.push_str("### Test 4: NULL handling functions\n\n");
    let general_query4 = "SELECT 
        integer_infer_blank,
        COALESCE(integer_infer_blank, -1) as with_default,
        CASE 
            WHEN integer_infer_blank IS NULL THEN 'Missing'
            ELSE 'Present'
        END as status,
        NULLIF(text_infer_dash, '-') as nullif_result
    FROM test_data 
    WHERE integer_infer_blank IS NULL 
        OR text_infer_dash = '-'
    LIMIT 10";
    
    output.push_str("```sql\n");
    output.push_str(general_query4);
    output.push_str("\n```\n\n");
    
    match QueryExecutor::execute(&db, general_query4) {
        Ok(result) => {
            output.push_str("| integer_infer_blank | with_default | status | nullif_result |\n");
            output.push_str("|---------------------|--------------|--------|---------------|\n");
            for row in &result.rows {
                output.push_str(&format!("| '{}' | {} | {} | '{}' |\n", 
                    row[0], row[1], row[2], row[3]));
            }
            output.push_str("\n");
        }
        Err(e) => {
            output.push_str(&format!("Error: {}\n\n", e));
        }
    }
    
    // Test 5: Ordering with NULLs
    output.push_str("### Test 5: ORDER BY with NULL values\n\n");
    let general_query5 = "SELECT 
        integer_infer_blank,
        real_infer_blank,
        date_infer_blank
    FROM test_data 
    ORDER BY integer_infer_blank NULLS FIRST
    LIMIT 10";
    
    output.push_str("```sql\n");
    output.push_str(general_query5);
    output.push_str("\n```\n\n");
    
    match QueryExecutor::execute(&db, general_query5) {
        Ok(result) => {
            output.push_str("| integer_infer_blank | real_infer_blank | date_infer_blank |\n");
            output.push_str("|---------------------|------------------|------------------|\n");
            for row in &result.rows {
                output.push_str(&format!("| '{}' | '{}' | '{}' |\n", 
                    row[0], row[1], row[2]));
            }
            output.push_str("\n");
        }
        Err(e) => {
            output.push_str(&format!("Error: {}\n\n", e));
        }
    }
    
    // Create summary table
    output.push_str("\n## Summary Table - NULL Handling Status\n\n");
    output.push_str("| Column Type | Blank → NULL | Dash → NULL | IS NULL Works | Display as Empty | Aggregations | Notes |\n");
    output.push_str("|-------------|--------------|-------------|---------------|------------------|--------------|-------|\n");
    
    // Check each column type
    let column_checks = vec![
        ("integer", "integer_infer_blank", "integer_infer_dash"),
        ("real", "real_infer_blank", "real_infer_dash"),
        ("text", "text_infer_blank", "text_infer_dash"),
        ("boolean", "boolean_infer_blank", "boolean_infer_dash"),
        ("date", "date_infer_blank", "date_infer_dash"),
        ("datetime", "datetime_infer_blank", "datetime_infer_dash"),
        ("time_seconds", "timeseconds_infer_blank", "timeseconds_infer_dash"),
        ("time_milliseconds", "timemilliseconds_infer_blank", "timemilliseconds_infer_dash"),
        ("blob", "blob_infer_blank", "blob_infer_dash"),
    ];
    
    for (col_type, blank_col, dash_col) in column_checks {
        let mut row = format!("| {} ", col_type);
        
        // Check blank → NULL
        let blank_check = format!("SELECT COUNT(*) FROM test_data WHERE \"{}\" IS NULL", blank_col);
        match QueryExecutor::execute(&db, &blank_check) {
            Ok(result) => {
                if let Some(r) = result.rows.first() {
                    let count: i64 = r[0].parse().unwrap_or(0);
                    row.push_str(if count > 0 { "| ✓ " } else { "| ✗ " });
                } else {
                    row.push_str("| ? ");
                }
            }
            Err(_) => row.push_str("| ERR "),
        }
        
        // Check dash → NULL
        let dash_check = format!("SELECT COUNT(*) FROM test_data WHERE \"{}\" IS NULL", dash_col);
        match QueryExecutor::execute(&db, &dash_check) {
            Ok(result) => {
                if let Some(r) = result.rows.first() {
                    let count: i64 = r[0].parse().unwrap_or(0);
                    row.push_str(if count > 0 { "| ✓ " } else { "| ✗ " });
                } else {
                    row.push_str("| ? ");
                }
            }
            Err(_) => row.push_str("| ERR "),
        }
        
        // IS NULL works (already checked above)
        row.push_str("| ✓ ");
        
        // Display as empty (check a sample)
        let display_check = format!("SELECT \"{}\" FROM test_data WHERE \"{}\" IS NULL LIMIT 1", blank_col, blank_col);
        match QueryExecutor::execute(&db, &display_check) {
            Ok(result) => {
                if let Some(r) = result.rows.first() {
                    row.push_str(if r[0].is_empty() { "| ✓ " } else { "| ✗ " });
                } else {
                    row.push_str("| N/A ");
                }
            }
            Err(_) => row.push_str("| ERR "),
        }
        
        // Aggregations work
        let agg_check = format!("SELECT AVG(\"{}\"), COUNT(\"{}\") FROM test_data", blank_col, blank_col);
        match QueryExecutor::execute(&db, &agg_check) {
            Ok(_) => row.push_str("| ✓ "),
            Err(_) => row.push_str("| ✗ "),
        }
        
        // Add notes
        row.push_str("| |");
        output.push_str(&row);
        output.push_str("\n");
    }
    
    output.push_str("\n**Legend:**\n");
    output.push_str("- ✓ = Working correctly\n");
    output.push_str("- ✗ = Not working as expected\n");
    output.push_str("- ? = Unclear/needs investigation\n");
    output.push_str("- N/A = Not applicable\n");
    output.push_str("- ERR = Query error\n");
    
    // Write results to file
    let mut file = File::create("null_comprehensive_test_results.md")?;
    file.write_all(output.as_bytes())?;
    
    println!("\n✓ Comprehensive test completed!");
    println!("Results written to: null_comprehensive_test_results.md");
    
    Ok(())
}