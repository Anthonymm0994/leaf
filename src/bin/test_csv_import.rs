use leaf::core::database::Database;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing CSV import with datetime columns");
    println!("{}", "=".repeat(60));
    
    // Create a new database
    let mut db = Database::open_writable(".")?;
    
    // Import the CSV file
    let csv_path = Path::new("data_gen_scripts/test_data_300k.csv");
    println!("\nImporting: {:?}", csv_path);
    
    // Import with header detection
    let table_name = "test_import";
    let delimiter = db.stream_insert_csv_with_header_row(table_name, csv_path, ',', 0)?;
    println!("Detected delimiter: '{}'", delimiter);
    
    println!("Import completed!");
    
    // Check datetime_infer_blank column
    println!("\nChecking datetime_infer_blank column:");
    let query = format!("SELECT datetime_infer_blank FROM {} LIMIT 20", table_name);
    let results = db.execute_query(&query)?;
    
    let mut null_count = 0;
    let mut valid_count = 0;
    
    for (i, row) in results.iter().enumerate() {
        if !row.is_empty() {
            let value = &row[0];
            if value.is_empty() {
                null_count += 1;
                println!("  Row {}: NULL", i);
            } else {
                valid_count += 1;
                println!("  Row {}: '{}'", i, value);
            }
        }
    }
    
    println!("\nSummary:");
    println!("  Valid values: {}", valid_count);
    println!("  Null values: {}", null_count);
    println!("  Total: {}", valid_count + null_count);
    
    // Check column types
    println!("\nChecking column types:");
    let schema_query = format!("SELECT * FROM {} LIMIT 1", table_name);
    let types = db.get_column_types(&schema_query)?;
    let names = db.get_column_names(&schema_query)?;
    
    // Find datetime columns
    use datafusion::arrow::datatypes::DataType;
    for (name, dtype) in names.iter().zip(types.iter()) {
        if name.contains("datetime") {
            println!("  {}: {:?}", name, dtype);
        }
    }
    
    // Save the table to Arrow format
    let arrow_path = Path::new("test_import_result.arrow");
    println!("\nSaving to Arrow: {:?}", arrow_path);
    
    // Get the batch and save it
    let batch = db.load_table_arrow_batch(table_name)?;
    use datafusion::arrow::ipc::writer::FileWriter;
    use std::fs::File;
    
    let file = File::create(arrow_path)?;
    let mut writer = FileWriter::try_new(file, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    
    println!("\nTest completed successfully!");
    
    Ok(())
}