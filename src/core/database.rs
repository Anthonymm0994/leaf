use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{StringArray, Int64Array, Float64Array, BooleanArray, Date32Array, TimestampNanosecondArray, TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use tokio::runtime::Runtime;
use crate::core::error::{Result, LeafError};
use crate::infer::{TypeInferrer, ColumnType};

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub row_count: i64,
}

#[derive(Debug, Clone)]
pub struct ViewInfo {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct DataBatch {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub struct Database {
    // DataFusion context for in-memory analytics
    ctx: SessionContext,
    runtime: Runtime,
    // Cache for loaded data batches
    batch_cache: HashMap<String, DataBatch>,
    // Track registered tables
    registered_tables: HashMap<String, Arc<RecordBatch>>,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        // Create a new runtime and context
        let runtime = Runtime::new()
            .expect("Failed to create tokio runtime for Database clone");
        let ctx = SessionContext::new();
        
        Self {
            ctx,
            runtime,
            batch_cache: self.batch_cache.clone(),
            registered_tables: self.registered_tables.clone(),
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // DataFusion context closed
    }
}

impl Database {
    fn parse_csv_line(line: &str, delimiter: char) -> Vec<String> {
        let mut row = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();
        
        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        // End of quoted field
                        in_quotes = false;
                    } else {
                        // Start of quoted field
                        in_quotes = true;
                    }
                },
                c if c == delimiter && !in_quotes => {
                    // End of field
                    row.push(current_field.trim().to_string());
                    current_field.clear();
                },
                _ => {
                    current_field.push(ch);
                }
            }
        }
        
        // Add the last field
        row.push(current_field.trim().to_string());
        row
    }

    /// Normalize time data by padding all entries to the highest precision found
    /// Parse a datetime string to a timestamp in the specified unit
    fn parse_datetime_string_to_timestamp(datetime_str: &str, unit: &TimeUnit) -> Option<i64> {
        use chrono::NaiveDateTime;
        
        // Common datetime formats
        let formats = [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
        ];
        
        for format in &formats {
            if let Ok(datetime) = NaiveDateTime::parse_from_str(datetime_str, format) {
                // Convert to timestamp in the requested unit
                let timestamp = match unit {
                    TimeUnit::Second => datetime.and_utc().timestamp(),
                    TimeUnit::Millisecond => datetime.and_utc().timestamp_millis(),
                    TimeUnit::Microsecond => datetime.and_utc().timestamp_micros(),
                    TimeUnit::Nanosecond => datetime.and_utc().timestamp_nanos_opt().unwrap_or(0),
                };
                return Some(timestamp);
            }
        }
        
        None
    }
    
    /// Parse a time string in HH:MM:SS format to a timestamp in the specified unit
    fn parse_time_string_to_timestamp(time_str: &str, unit: &TimeUnit) -> Option<i64> {
        let parts: Vec<&str> = time_str.split(':').collect();
        if parts.len() != 3 {
            return None;
        }
        
        // Parse hours, minutes, seconds
        let hour = parts[0].parse::<u8>().ok()?;
        let minute = parts[1].parse::<u8>().ok()?;
        let seconds_part = parts[2];
        
        // Parse seconds and optional fractional part
        let (second, fraction) = if let Some(dot_pos) = seconds_part.find('.') {
            let second_str = &seconds_part[..dot_pos];
            let fraction_str = &seconds_part[dot_pos + 1..];
            let second = second_str.parse::<u8>().ok()?;
            let fraction = fraction_str.parse::<u64>().unwrap_or(0);
            (second, fraction)
        } else {
            let second = seconds_part.parse::<u8>().ok()?;
            (second, 0)
        };
        
        // Validate time components
        if hour >= 24 || minute >= 60 || second >= 60 {
            return None;
        }
        
        // Calculate total seconds since midnight
        let total_seconds = hour as i64 * 3600 + minute as i64 * 60 + second as i64;
        
        // Convert to the requested unit
        let timestamp = match unit {
            TimeUnit::Second => total_seconds,
            TimeUnit::Millisecond => total_seconds * 1_000 + (fraction / 1_000_000) as i64,
            TimeUnit::Microsecond => total_seconds * 1_000_000 + (fraction / 1_000) as i64,
            TimeUnit::Nanosecond => total_seconds * 1_000_000_000 + fraction as i64,
        };
        
        Some(timestamp)
    }

    fn normalize_time_column(values: &[String]) -> Vec<String> {
        let mut max_fraction_digits = 0;
        
        // First pass: find the maximum fraction digits
        for value in values {
            if value.is_empty() || value.to_lowercase() == "null" {
                continue;
            }
            
            let parts: Vec<&str> = value.split(':').collect();
            if parts.len() == 3 {
                let seconds_part = parts[2];
                if let Some(dot_pos) = seconds_part.find('.') {
                    let fraction_str = &seconds_part[dot_pos + 1..];
                    max_fraction_digits = max_fraction_digits.max(fraction_str.len());
                }
            }
        }
        
        // Second pass: pad all values to the maximum precision
        let mut normalized_values = Vec::new();
        for value in values {
            if value.is_empty() || value.to_lowercase() == "null" {
                normalized_values.push(value.clone());
                continue;
            }
            
            let parts: Vec<&str> = value.split(':').collect();
            if parts.len() == 3 {
                let seconds_part = parts[2];
                if let Some(dot_pos) = seconds_part.find('.') {
                    // Has fractional part - pad to max precision
                    let seconds_str = &seconds_part[..dot_pos];
                    let fraction_str = &seconds_part[dot_pos + 1..];
                    let padded_fraction = format!("{:0<width$}", fraction_str, width = max_fraction_digits);
                    normalized_values.push(format!("{}:{}:{}.{}", parts[0], parts[1], seconds_str, padded_fraction));
                } else {
                    // No fractional part - add zeros only if needed
                    if max_fraction_digits > 0 {
                        let padded_fraction = "0".repeat(max_fraction_digits);
                        normalized_values.push(format!("{}:{}:{}.{}", parts[0], parts[1], seconds_part, padded_fraction));
                    } else {
                        normalized_values.push(value.clone());
                    }
                }
            } else {
                // Not a valid time format, keep as is
                normalized_values.push(value.clone());
            }
        }
        
        normalized_values
    }

    /// Infer the most likely delimiter from a header line
    fn infer_delimiter_from_header(header_line: &str) -> char {
        let delimiters = [',', '\t', ';', '|'];
        let mut best_delimiter = ',';
        let mut max_fields = 0;
        
        for &delimiter in &delimiters {
            let fields = Self::parse_csv_line(header_line, delimiter);
            if fields.len() > max_fields && fields.len() > 1 {
                max_fields = fields.len();
                best_delimiter = delimiter;
            }
        }
        
        best_delimiter
    }

    /// Check if a line appears to be valid CSV data based on delimiter
    fn is_valid_csv_line(line: &str, delimiter: char, expected_columns: usize) -> bool {
        // Skip empty lines
        if line.trim().is_empty() {
            return false;
        }
        
        // Skip comment lines
        if line.trim().starts_with('#') {
            return false;
        }
        
        // Parse the line and check if it has the expected number of columns
        let fields = Self::parse_csv_line(line, delimiter);
        
        // If we have a specific expected column count, validate against it
        if expected_columns > 0 {
            return fields.len() == expected_columns;
        }
        
        // Otherwise, just check if it has multiple fields (at least 2)
        fields.len() >= 2
    }

    fn parse_date_string(date_str: &str) -> Option<i32> {
        // Parse date in YYYY-MM-DD format
        let parts: Vec<&str> = date_str.split('-').collect();
        if parts.len() != 3 {
            return None;
        }
        
        let year: i32 = parts[0].parse().ok()?;
        let month: u32 = parts[1].parse().ok()?;
        let day: u32 = parts[2].parse().ok()?;
        
        // Validate date
        if month < 1 || month > 12 || day < 1 || day > 31 {
            return None;
        }
        
        // Convert to days since epoch (1970-01-01)
        // This is a simplified conversion - in production you'd want a proper date library
        let days_since_epoch = (year - 1970) * 365 + (year - 1969) / 4; // Approximate leap year calculation
        
        // Add days for months (simplified)
        let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let mut days_in_year: i32 = 0;
        for i in 1..month {
            days_in_year += month_days[i as usize];
        }
        days_in_year += (day - 1) as i32;
        
        Some(days_since_epoch + days_in_year)
    }

    pub fn open_readonly<P: AsRef<Path>>(_path: P) -> Result<Self> {
        // DataFusion is in-memory, so we don't need file paths for now
        let runtime = Runtime::new()
            .map_err(|e| LeafError::Custom(format!("Failed to create tokio runtime: {}", e)))?;
        
        let ctx = SessionContext::new();
        
        Ok(Self {
            ctx,
            runtime,
            batch_cache: HashMap::new(),
            registered_tables: HashMap::new(),
        })
    }

    pub fn open_writable<P: AsRef<Path>>(_path: P) -> Result<Self> {
        // DataFusion is in-memory, so we don't need file paths for now
        let runtime = Runtime::new()
            .map_err(|e| LeafError::Custom(format!("Failed to create tokio runtime: {}", e)))?;
        
        let ctx = SessionContext::new();
        
        Ok(Self {
            ctx,
            runtime,
            batch_cache: HashMap::new(),
            registered_tables: HashMap::new(),
        })
    }

    pub fn is_readonly(&self) -> bool {
        false // DataFusion is always writable in-memory
    }

    // Load a table into memory as a DataBatch (cached)
    pub fn load_table_batch(&mut self, table_name: &str) -> Result<DataBatch> {
        // Check cache first
        if let Some(batch) = self.batch_cache.get(table_name) {
            return Ok(batch.clone());
        }
        
        // Load from DataFusion into DataBatch
        let query = format!("SELECT * FROM '{}'", table_name);
        let batch = self.execute_query_batch(&query)?;
        
        // Cache the batch
        self.batch_cache.insert(table_name.to_string(), batch.clone());
        
        Ok(batch)
    }

    // Execute a DataFusion query and return as DataBatch
    pub fn execute_query_batch(&self, query: &str) -> Result<DataBatch> {
        let ctx = self.ctx.clone();
        
        let result = self.runtime.block_on(async {
            ctx.sql(query).await
        }).map_err(|e| LeafError::Custom(format!("Failed to execute query: {}", e)))?;
        
        let record_batches = self.runtime.block_on(async {
            result.collect().await
        }).map_err(|e| LeafError::Custom(format!("Failed to collect results: {}", e)))?;
        
        if record_batches.is_empty() {
            return Ok(DataBatch {
                columns: vec![],
                rows: vec![],
            });
        }
        
        // Convert first batch to DataBatch
        let batch = &record_batches[0];
        let columns: Vec<String> = batch.schema().fields().iter()
            .map(|field| field.name().clone())
            .collect();
        
        let rows = self.record_batch_to_rows(batch)?;
        
        Ok(DataBatch {
            columns,
            rows,
        })
    }

    // Execute query and return as row data (for UI display)
    pub fn execute_query(&self, query: &str) -> Result<Vec<Vec<String>>> {
        let batch = self.execute_query_batch(query)?;
        Ok(batch.rows)
    }

    // Execute count query safely
    pub fn execute_count_query(&self, query: &str) -> Result<i64> {
        let batch = self.execute_query_batch(query)?;
        if batch.rows.is_empty() || batch.columns.is_empty() {
            return Ok(0);
        }
        
        // Try to parse the first value as i64
        if let Some(first_row) = batch.rows.first() {
            if let Some(first_value) = first_row.first() {
                return first_value.parse::<i64>()
                    .map_err(|e| LeafError::Custom(format!("Failed to parse count result: {}", e)));
            }
        }
        
        Ok(0)
    }

    // Create table in DataFusion (register an empty table)
    pub fn create_table(&mut self, table_name: &str, columns: &[(&str, &str)]) -> Result<()> {
        // Convert SQL types to Arrow types
        let fields: Vec<Field> = columns.iter()
            .map(|(name, sql_type)| {
                let arrow_type = self.sql_type_to_arrow_type(sql_type);
                Field::new(*name, arrow_type, true)
            })
            .collect();
        
        let schema = Schema::new(fields);
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        
        // Register the empty table
        self.ctx.register_batch(table_name, empty_batch)
            .map_err(|e| LeafError::Custom(format!("Failed to register table: {}", e)))?;
        
        Ok(())
    }

    // Insert data into DataFusion table
    pub fn insert_data(&mut self, table_name: &str, values: &[Vec<String>]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        
        // Clear cache for this table since data changed
        self.batch_cache.remove(table_name);
        
        // Convert string values to Arrow arrays
        let (columns, schema) = if let Some(existing_batch) = self.registered_tables.get(table_name) {
            // Use existing schema to preserve column names and types
            let existing_schema = existing_batch.schema();
            let columns: Vec<String> = existing_schema.fields().iter()
                .map(|field| field.name().clone())
                .collect();
            (columns, existing_schema.clone())
        } else {
            // Create default column names if table doesn't exist
            let columns: Vec<String> = (0..values[0].len()).map(|i| format!("col_{}", i)).collect();
            let schema = Arc::new(Schema::new(
                columns.iter().map(|name| Field::new(name, DataType::Utf8, true)).collect::<Vec<_>>()
            ));
            (columns, schema)
        };
        
        let arrays = self.string_rows_to_arrow_arrays(&columns, values)?;
        
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| LeafError::Custom(format!("Failed to create record batch: {}", e)))?;
        
        // Safely register or replace the table
        self.register_or_replace_table(table_name, batch.clone())?;
        
        // Store in our cache
        self.registered_tables.insert(table_name.to_string(), Arc::new(batch));
        
        Ok(())
    }

    // Helper method to safely register or replace a table
    fn register_or_replace_table(&mut self, table_name: &str, batch: RecordBatch) -> Result<()> {
        // Check if table already exists in our tracking
        if self.registered_tables.contains_key(table_name) {
            // Remove from our internal tracking first
            self.registered_tables.remove(table_name);
        }
        
        // Try to register the table
        match self.ctx.register_batch(table_name, batch.clone()) {
            Ok(_) => Ok(()),
            Err(_) => {
                // If registration fails, the table already exists in DataFusion
                // We need to create a new context to replace it
                // This is a workaround since DataFusion doesn't support table replacement
                
                // Create a new context
                let new_ctx = SessionContext::new();
                
                // Register all existing tables except the one we're replacing
                for (existing_name, existing_batch) in &self.registered_tables {
                    if existing_name != table_name {
                        new_ctx.register_batch(existing_name, existing_batch.as_ref().clone())
                            .map_err(|e| LeafError::Custom(format!("Failed to re-register table {}: {}", existing_name, e)))?;
                    }
                }
                
                // Register the new table
                new_ctx.register_batch(table_name, batch)
                    .map_err(|e| LeafError::Custom(format!("Failed to register data: {}", e)))?;
                
                // Replace the context
                self.ctx = new_ctx;
                
                Ok(())
            }
        }
    }

    // Get table information from DataFusion
    pub fn get_tables(&self) -> Result<Vec<TableInfo>> {
        let mut tables = Vec::new();
        
        for (table_name, batch) in &self.registered_tables {
            let columns: Vec<ColumnInfo> = batch.schema().fields().iter()
                .map(|field| ColumnInfo {
                    name: field.name().clone(),
                    data_type: self.arrow_type_to_sql_type(field.data_type()).to_string(),
                    is_nullable: field.is_nullable(),
                    is_primary_key: false, // DataFusion doesn't track primary keys
                })
                .collect();
            
            tables.push(TableInfo {
                name: table_name.clone(),
                columns,
                row_count: batch.num_rows() as i64,
            });
        }
        
        Ok(tables)
    }

    // Helper methods for DataFusion integration
    fn record_batch_to_rows(&self, batch: &RecordBatch) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = self.array_value_to_string(array, row_idx)?;
                row.push(value);
            }
            rows.push(row);
        }
        
        Ok(rows)
    }

    fn array_value_to_string(&self, array: &datafusion::arrow::array::ArrayRef, index: usize) -> Result<String> {
        use datafusion::arrow::array::*;
        use chrono::{DateTime, Utc};
        
        match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(string_array.value(index).to_string())
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                if int_array.is_null(index) {
                    Ok("NULL".to_string())
                } else {
                    Ok(int_array.value(index).to_string())
                }
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                if float_array.is_null(index) {
                    Ok("NULL".to_string())
                } else {
                    Ok(float_array.value(index).to_string())
                }
            }
            DataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(bool_array.value(index).to_string())
            }
            DataType::Timestamp(unit, _) => {
                // Handle timestamp arrays - convert back to time-only format
                match unit {
                    TimeUnit::Second => {
                        let timestamp_array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                        if timestamp_array.is_null(index) {
                            Ok("NULL".to_string())
                        } else {
                            let timestamp = timestamp_array.value(index);
                            // Check if this is a datetime (> 1 day) or time-of-day value
                            if timestamp > 86400 {
                                // This is a datetime - format as full datetime
                                let datetime = DateTime::from_timestamp(timestamp, 0)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Ok(datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                            } else {
                                // This is time-of-day - convert seconds since midnight back to HH:MM:SS format
                                let hours = (timestamp / 3600) % 24;
                                let minutes = (timestamp / 60) % 60;
                                let seconds = timestamp % 60;
                                Ok(format!("{:02}:{:02}:{:02}", hours, minutes, seconds))
                            }
                        }
                    }
                    TimeUnit::Millisecond => {
                        let timestamp_array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                        if timestamp_array.is_null(index) {
                            Ok("NULL".to_string())
                        } else {
                            let timestamp = timestamp_array.value(index);
                            // Check if this is a datetime (> 1 day in milliseconds) or time-of-day value
                            if timestamp > 86_400_000 {
                                // This is a datetime - format as full datetime
                                let datetime = DateTime::from_timestamp_millis(timestamp)
                                    .unwrap_or_else(|| DateTime::from_timestamp_millis(0).unwrap());
                                Ok(datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                            } else {
                                // This is time-of-day - convert milliseconds since midnight back to HH:MM:SS.mmm format
                                let total_seconds = timestamp / 1_000;
                                let milliseconds = timestamp % 1_000;
                                let hours = (total_seconds / 3600) % 24;
                                let minutes = (total_seconds / 60) % 60;
                                let seconds = total_seconds % 60;
                                if milliseconds == 0 {
                                    Ok(format!("{:02}:{:02}:{:02}", hours, minutes, seconds))
                                } else {
                                    Ok(format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, milliseconds))
                                }
                            }
                        }
                    }
                    TimeUnit::Microsecond => {
                        let timestamp_array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                        if timestamp_array.is_null(index) {
                            Ok("NULL".to_string())
                        } else {
                            let timestamp = timestamp_array.value(index);
                            // Convert microseconds since midnight back to HH:MM:SS.mmmmmm format
                            let total_seconds = timestamp / 1_000_000;
                            let microseconds = timestamp % 1_000_000;
                            let hours = (total_seconds / 3600) % 24;
                            let minutes = (total_seconds / 60) % 60;
                            let seconds = total_seconds % 60;
                            Ok(format!("{:02}:{:02}:{:02}.{:06}", hours, minutes, seconds, microseconds))
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let timestamp_array = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                        if timestamp_array.is_null(index) {
                            Ok("NULL".to_string())
                        } else {
                            let timestamp = timestamp_array.value(index);
                            // Convert nanoseconds since midnight back to HH:MM:SS.nnnnnnnnn format
                            let total_seconds = timestamp / 1_000_000_000;
                            let nanoseconds = timestamp % 1_000_000_000;
                            let hours = (total_seconds / 3600) % 24;
                            let minutes = (total_seconds / 60) % 60;
                            let seconds = total_seconds % 60;
                            Ok(format!("{:02}:{:02}:{:02}.{:09}", hours, minutes, seconds, nanoseconds))
                        }
                    }
                }
            }
            DataType::Date32 => {
                let date_array = array.as_any().downcast_ref::<Date32Array>().unwrap();
                if date_array.is_null(index) {
                    Ok("NULL".to_string())
                } else {
                    let days = date_array.value(index);
                    // Convert days since epoch (1970-01-01) to readable date
                    // This is a simplified conversion - in production you'd want a proper date library
                    let year = 1970 + (days / 365);
                    let remaining_days = days % 365;
                    
                    // Simplified month/day calculation
                    let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
                    let mut month = 1;
                    let mut day = remaining_days;
                    
                    for &days_in_month in &month_days[1..] {
                        if day < days_in_month {
                            break;
                        }
                        day -= days_in_month;
                        month += 1;
                    }
                    
                    Ok(format!("{:04}-{:02}-{:02}", year, month, day + 1))
                }
            }
            _ => {
                // For other types, convert to string representation
                Ok(format!("{:?}", array))
            }
        }
    }

    fn string_rows_to_arrow_arrays(&self, columns: &[String], values: &[Vec<String>]) -> Result<Vec<datafusion::arrow::array::ArrayRef>> {
        let mut arrays = Vec::new();
        
        for col_idx in 0..columns.len() {
            // Try to infer the data type from the first few rows
            let mut is_numeric = true;
            let mut is_integer = true;
            
            // Check first 10 rows to infer type
            for row in values.iter().take(10) {
                if col_idx < row.len() {
                    let value = &row[col_idx];
                    if !value.is_empty() {
                        // Check if it's numeric
                        if !value.chars().all(|c| c.is_numeric() || c == '.' || c == '-') {
                            is_numeric = false;
                            break;
                        }
                        // Check if it's integer
                        if value.contains('.') {
                            is_integer = false;
                        }
                    }
                }
            }
            
            if is_numeric && is_integer {
                // Create Int64 array
                let mut int_values = Vec::new();
                for row in values {
                    if col_idx < row.len() {
                        let value = &row[col_idx];
                        if value.is_empty() {
                            int_values.push(None);
                        } else {
                            match value.parse::<i64>() {
                                Ok(int_val) => int_values.push(Some(int_val)),
                                Err(_) => int_values.push(None),
                            }
                        }
                    } else {
                        int_values.push(None);
                    }
                }
                let array = Int64Array::from(int_values);
                arrays.push(Arc::new(array) as datafusion::arrow::array::ArrayRef);
            } else if is_numeric {
                // Create Float64 array
                let mut float_values = Vec::new();
                for row in values {
                    if col_idx < row.len() {
                        let value = &row[col_idx];
                        if value.is_empty() {
                            float_values.push(None);
                        } else {
                            match value.parse::<f64>() {
                                Ok(float_val) => float_values.push(Some(float_val)),
                                Err(_) => float_values.push(None),
                            }
                        }
                    } else {
                        float_values.push(None);
                    }
                }
                let array = Float64Array::from(float_values);
                arrays.push(Arc::new(array) as datafusion::arrow::array::ArrayRef);
            } else {
                // Create String array
                let mut string_values = Vec::new();
                for row in values {
                    if col_idx < row.len() {
                        string_values.push(row[col_idx].clone());
                    } else {
                        string_values.push(String::new());
                    }
                }
                let array = StringArray::from(string_values);
                arrays.push(Arc::new(array) as datafusion::arrow::array::ArrayRef);
            }
        }
        
        Ok(arrays)
    }

    fn string_rows_to_arrow_arrays_with_schema(&self, columns: &[String], values: &[Vec<String>], schema: &Schema) -> Result<Vec<datafusion::arrow::array::ArrayRef>> {
        let mut arrays = Vec::new();
        
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let data_type = field.data_type();
            
            match data_type {
                DataType::Int64 => {
                    // Create Int64 array
                    let mut int_values = Vec::new();
                    for row in values {
                        if col_idx < row.len() {
                            let value = &row[col_idx];
                            if value.is_empty() || value.to_lowercase() == "null" || value == "-" || value.to_lowercase() == "n/a" {
                                int_values.push(None);
                            } else {
                                match value.parse::<i64>() {
                                    Ok(int_val) => int_values.push(Some(int_val)),
                                    Err(_) => int_values.push(None),
                                }
                            }
                        } else {
                            int_values.push(None);
                        }
                    }
                    let array = Int64Array::from(int_values);
                    arrays.push(Arc::new(array) as datafusion::arrow::array::ArrayRef);
                },
                DataType::Float64 => {
                    // Create Float64 array
                    let mut float_values = Vec::new();
                    for row in values {
                        if col_idx < row.len() {
                            let value = &row[col_idx];
                            if value.is_empty() || value.to_lowercase() == "null" || value == "-" || value.to_lowercase() == "n/a" {
                                float_values.push(None);
                            } else {
                                match value.parse::<f64>() {
                                    Ok(float_val) => float_values.push(Some(float_val)),
                                    Err(_) => float_values.push(None),
                                }
                            }
                        } else {
                            float_values.push(None);
                        }
                    }
                    let array = Float64Array::from(float_values);
                    arrays.push(Arc::new(array) as datafusion::arrow::array::ArrayRef);
                },
                DataType::Boolean => {
                    // Create Boolean array
                    let mut bool_values = Vec::new();
                    for row in values {
                        if col_idx < row.len() {
                            let value = &row[col_idx];
                            if value.is_empty() || value.to_lowercase() == "null" || value == "-" || value.to_lowercase() == "n/a" {
                                bool_values.push(None);
                            } else {
                                let lower = value.to_lowercase();
                                match lower.as_str() {
                                    "true" | "1" | "yes" | "y" => bool_values.push(Some(true)),
                                    "false" | "0" | "no" | "n" => bool_values.push(Some(false)),
                                    _ => bool_values.push(None),
                                }
                            }
                        } else {
                            bool_values.push(None);
                        }
                    }
                    let array = BooleanArray::from(bool_values);
                    arrays.push(Arc::new(array) as datafusion::arrow::array::ArrayRef);
                },
                DataType::Timestamp(unit, tz) => {
                    // Create Timestamp array with the correct precision
                    let mut timestamp_values = Vec::new();
                    for row in values {
                        if col_idx < row.len() {
                            let value = &row[col_idx];
                            if value.is_empty() || value.to_lowercase() == "null" || value == "-" || value.to_lowercase() == "n/a" {
                                timestamp_values.push(None);
                            } else {
                                // Try to parse as datetime string first (YYYY-MM-DD HH:MM:SS)
                                if let Some(timestamp) = Self::parse_datetime_string_to_timestamp(value, unit) {
                                    timestamp_values.push(Some(timestamp));
                                } else if let Some(timestamp) = Self::parse_time_string_to_timestamp(value, unit) {
                                    // Try to parse time string in HH:MM:SS format
                                    timestamp_values.push(Some(timestamp));
                                } else {
                                    // Try to parse as regular timestamp number
                                    match value.parse::<i64>() {
                                        Ok(ts) => {
                                            // Convert to the correct unit
                                            let converted_ts = match unit {
                                                TimeUnit::Second => ts,
                                                TimeUnit::Millisecond => ts * 1_000,
                                                TimeUnit::Microsecond => ts * 1_000_000,
                                                TimeUnit::Nanosecond => ts * 1_000_000_000,
                                            };
                                            timestamp_values.push(Some(converted_ts));
                                        }
                                        Err(_) => {
                                            // If not a number, try to parse as date string
                                            // For now, just store as null if we can't parse
                                            timestamp_values.push(None);
                                        }
                                    }
                                }
                            }
                        } else {
                            timestamp_values.push(None);
                        }
                    }
                    
                    // Create the appropriate timestamp array based on the unit
                    let array: datafusion::arrow::array::ArrayRef = match unit {
                        TimeUnit::Second => {
                            let array = TimestampSecondArray::from(timestamp_values);
                            Arc::new(array)
                        },
                        TimeUnit::Millisecond => {
                            let array = TimestampMillisecondArray::from(timestamp_values);
                            Arc::new(array)
                        },
                        TimeUnit::Microsecond => {
                            let array = TimestampMicrosecondArray::from(timestamp_values);
                            Arc::new(array)
                        },
                        TimeUnit::Nanosecond => {
                            let array = TimestampNanosecondArray::from(timestamp_values);
                            Arc::new(array)
                        },
                    };
                    arrays.push(array);
                },
                DataType::Date32 => {
                    // Create Date32 array
                    let mut date_values = Vec::new();
                    for row in values {
                        if col_idx < row.len() {
                            let value = &row[col_idx];
                            if value.is_empty() || value.to_lowercase() == "null" || value == "-" || value.to_lowercase() == "n/a" {
                                date_values.push(None);
                            } else {
                                // Try to parse as date string (YYYY-MM-DD format)
                                match Self::parse_date_string(value) {
                                    Some(days) => date_values.push(Some(days)),
                                    None => date_values.push(None),
                                }
                            }
                        } else {
                            date_values.push(None);
                        }
                    }
                    let array = Date32Array::from(date_values);
                    arrays.push(Arc::new(array) as datafusion::arrow::array::ArrayRef);
                },
                _ => {
                    // Default to String array for all other types
                    let mut string_values = Vec::new();
                    for row in values {
                        if col_idx < row.len() {
                            let value = &row[col_idx];
                            // Treat null values as empty strings for display
                            if value.is_empty() || value.to_lowercase() == "null" || value == "-" || value.to_lowercase() == "n/a" {
                                string_values.push(String::new());
                            } else {
                                string_values.push(value.clone());
                            }
                        } else {
                            string_values.push(String::new());
                        }
                    }
                    let array = StringArray::from(string_values);
                    arrays.push(Arc::new(array) as datafusion::arrow::array::ArrayRef);
                }
            }
        }
        
        Ok(arrays)
    }

    fn sql_type_to_arrow_type(&self, sql_type: &str) -> DataType {
        match sql_type.to_uppercase().as_str() {
            "INTEGER" | "INT" | "BIGINT" => DataType::Int64,
            "REAL" | "DOUBLE" | "FLOAT" => DataType::Float64,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "TEXT" | "VARCHAR" | "STRING" => DataType::Utf8,
            "TIMESTAMP" => DataType::Timestamp(TimeUnit::Nanosecond, None),
            _ => DataType::Utf8, // Default to string
        }
    }

    fn arrow_type_to_sql_type(&self, arrow_type: &DataType) -> &'static str {
        match arrow_type {
            DataType::Int64 => "INTEGER",
            DataType::Float64 => "REAL",
            DataType::Boolean => "BOOLEAN",
            DataType::Utf8 => "TEXT",
            DataType::Timestamp(_, _) => "TIMESTAMP",
            _ => "TEXT",
        }
    }

    // Placeholder methods for compatibility
    pub fn get_views(&self) -> Result<Vec<ViewInfo>> {
        Ok(vec![]) // DataFusion doesn't have views in the same way
    }

    pub fn table_exists(&self, table_name: &str) -> Result<bool> {
        Ok(self.registered_tables.contains_key(table_name))
    }

    pub fn get_column_names(&self, query: &str) -> Result<Vec<String>> {
        let ctx = &self.ctx;
        let rt = Runtime::new()?;
        
        rt.block_on(async {
            let df = ctx.sql(query).await.map_err(|e| LeafError::Database(e.to_string()))?;
            let schema = df.schema();
            Ok(schema.fields().iter().map(|f| f.name().clone()).collect())
        })
    }
    
    pub fn get_column_types(&self, query: &str) -> Result<Vec<DataType>> {
        let ctx = &self.ctx;
        let rt = Runtime::new()?;
        
        rt.block_on(async {
            let df = ctx.sql(query).await.map_err(|e| LeafError::Database(e.to_string()))?;
            let schema = df.schema();
            Ok(schema.fields().iter().map(|f| f.data_type().clone()).collect())
        })
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<()> {
        let ctx = self.ctx.clone();
        self.runtime.block_on(async {
            ctx.sql(sql).await
        }).map_err(|e| LeafError::Custom(format!("Failed to execute SQL: {}", e)))?;
        Ok(())
    }

    pub fn create_table_with_types(&mut self, table_name: &str, columns: &[(&str, &str)]) -> Result<()> {
        self.create_table(table_name, columns)
    }

    pub fn stream_insert_csv(&mut self, table_name: &str, csv_path: &Path, delimiter: char, has_header: bool) -> Result<()> {
        // Read CSV with proper header handling
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(delimiter as u8)
            .has_headers(has_header)
            .from_path(csv_path)
            .map_err(|e| LeafError::Custom(format!("Failed to read CSV: {}", e)))?;
        
        let mut rows = Vec::new();
        let mut headers = Vec::new();
        
        if has_header {
            // Get headers from the first row
            match rdr.headers() {
                Ok(header_result) => {
                    headers = header_result.iter().map(|s| s.to_string()).collect();
                }
                Err(e) => {
                    return Err(LeafError::Custom(format!("Failed to read CSV headers: {}", e)));
                }
            }
        }
        
        // Read data rows
        for result in rdr.records() {
            let record = result.map_err(|e| LeafError::Custom(format!("Failed to read CSV record: {}", e)))?;
            let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
            rows.push(row);
        }
        
        // If no headers were found, create default column names
        if headers.is_empty() {
            headers = (0..rows.first().map(|r| r.len()).unwrap_or(0))
                .map(|i| format!("col_{}", i))
                .collect();
        }
        
        // If the table already exists with a schema, we need to ensure the data matches
        if let Some(existing_batch) = self.registered_tables.get(table_name) {
            let existing_schema = existing_batch.schema();
            let expected_columns = existing_schema.fields().len();
            
            // Ensure all rows have the expected number of columns
            for (row_idx, row) in rows.iter().enumerate() {
                if row.len() != expected_columns {
                    return Err(LeafError::Custom(format!(
                        "Row {} has {} columns, but table schema expects {} columns",
                        row_idx + 1, row.len(), expected_columns
                    )));
                }
            }
        }
        
        // Create schema with proper column names and type inference
        let fields: Vec<Field> = headers.iter().enumerate()
            .map(|(col_idx, name)| {
                // Try to infer the data type from the first few rows
                let data_type = if !rows.is_empty() {
                    let mut is_numeric = true;
                    let mut is_integer = true;
                    
                    // Check first 10 rows to infer type
                    for row in rows.iter().take(10) {
                        if col_idx < row.len() {
                            let value: &str = &row[col_idx];
                            if !value.is_empty() {
                                // Check if it's numeric
                                if !value.chars().all(|c| c.is_numeric() || c == '.' || c == '-') {
                                    is_numeric = false;
                                    break;
                                }
                                // Check if it's integer
                                if value.contains('.') {
                                    is_integer = false;
                                }
                            }
                        }
                    }
                    
                    if is_numeric {
                        if is_integer {
                            DataType::Int64
                        } else {
                            DataType::Float64
                        }
                    } else {
                        DataType::Utf8
                    }
                } else {
                    DataType::Utf8
                };
                
                Field::new(name, data_type, true)
            })
            .collect();
        
        let schema = Schema::new(fields);
        
        // Convert rows to Arrow arrays
        let arrays = self.string_rows_to_arrow_arrays(&headers, &rows)?;
        
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|e| LeafError::Custom(format!("Failed to create record batch: {}", e)))?;
        
        // Register the table with proper schema, handling replacement if it already exists
        self.register_or_replace_table(table_name, batch.clone())?;
        
        // Store in our cache
        self.registered_tables.insert(table_name.to_string(), Arc::new(batch));
        
        Ok(())
    }

    /// Enhanced CSV import that can skip lines and select a specific row as header
    pub fn stream_insert_csv_with_header_row(&mut self, table_name: &str, csv_path: &Path, delimiter: char, header_row: usize) -> Result<char> {
        use std::io::{BufRead, BufReader};
        use std::fs::File;
        
        let file = File::open(csv_path)
            .map_err(|e| LeafError::Custom(format!("Failed to open CSV file: {}", e)))?;
        
        let reader = BufReader::new(file);
        let lines: Vec<String> = reader.lines()
            .map(|line| line.unwrap_or_default())
            .collect();
        
        // Validate header row (using original lines to match UI)
        if header_row >= lines.len() {
            return Err(LeafError::Custom("Header row exceeds file length".to_string()));
        }
        
        // Start reading from the header row (using original lines to match UI)
        let data_lines = lines.into_iter().skip(header_row).collect::<Vec<String>>();
        
        if data_lines.is_empty() {
            return Err(LeafError::Custom("No data lines found after header row".to_string()));
        }
        
        // Extract headers from the first row (which is now the header)
        let headers: Vec<String> = if !data_lines.is_empty() {
            let header_line = &data_lines[0];
            Self::parse_csv_line(header_line, delimiter)
        } else {
            return Err(LeafError::Custom("No header row found".to_string()));
        };
        
        // Infer delimiter from header if not already specified
        let inferred_delimiter = if delimiter == ',' {
            Self::infer_delimiter_from_header(&data_lines[0])
        } else {
            delimiter
        };
        
        // Parse data rows (skip the header row) and filter using delimiter-based validation
        let mut rows = Vec::new();
        let expected_columns = headers.len();
        
        for line in data_lines.iter().skip(1) {
            // Use delimiter-based validation instead of hardcoded string checks
            if Self::is_valid_csv_line(line, inferred_delimiter, expected_columns) {
                let row = Self::parse_csv_line(line, inferred_delimiter);
                rows.push(row);
            }
        }
        
        // If no headers were found, create default column names
        let mut final_headers = if headers.is_empty() {
            (0..rows.first().map(|r| r.len()).unwrap_or(0))
                .map(|i| format!("col_{}", i))
                .collect()
        } else {
            headers
        };
        
        // Deduplicate column names to avoid DataFusion errors
        let mut seen_names = std::collections::HashSet::new();
        let mut deduplicated_headers = Vec::new();
        
        for (i, header) in final_headers.iter().enumerate() {
            let mut unique_name = header.clone();
            let mut counter = 1;
            
            while seen_names.contains(&unique_name) {
                unique_name = format!("{}_{}", header, counter);
                counter += 1;
            }
            
            seen_names.insert(unique_name.clone());
            deduplicated_headers.push(unique_name);
        }
        
        final_headers = deduplicated_headers;
        
        // If the table already exists with a schema, we need to ensure the data matches
        if let Some(existing_batch) = self.registered_tables.get(table_name) {
            let existing_schema = existing_batch.schema();
            let expected_columns = existing_schema.fields().len();
            
            // Ensure all rows have the expected number of columns
            for (row_idx, row) in rows.iter().enumerate() {
                if row.len() != expected_columns {
                    return Err(LeafError::Custom(format!(
                        "Row {} has {} columns, but table schema expects {} columns",
                        row_idx + 1, row.len(), expected_columns
                    )));
                }
            }
        }
        
        // Use the same type inference system as the UI (with null awareness)
        
        // Convert rows to the format expected by TypeInferrer
        let sample_data: Vec<Vec<String>> = rows.iter().take(20).cloned().collect();
        
        // Infer types using the null-aware system (same as UI)
        // For now, use default null values since we don't have access to user config here
        let default_null_values = vec!["null".to_string(), "NULL".to_string(), "N/A".to_string(), "".to_string()];
        let inferred_types = TypeInferrer::infer_column_types_with_nulls(&final_headers, &sample_data, &default_null_values);
        
        // Convert to Arrow schema
        let fields: Vec<Field> = inferred_types.iter()
            .map(|(name, col_type)| {
                let data_type = col_type.to_arrow_type();
                Field::new(name, data_type, true)
            })
            .collect();
        
        let schema = Schema::new(fields);
        
        // Apply time normalization to columns that are detected as time types
        let mut normalized_rows = rows.clone();
        for (col_idx, (name, col_type)) in inferred_types.iter().enumerate() {
            if col_type.is_time_type() {
                // Extract column values
                let mut column_values = Vec::new();
                for row in &normalized_rows {
                    if col_idx < row.len() {
                        column_values.push(row[col_idx].clone());
                    } else {
                        column_values.push(String::new());
                    }
                }
                
                // Normalize the time column
                let normalized_values = Self::normalize_time_column(&column_values);
                
                // Update the rows with normalized values
                for (row_idx, normalized_value) in normalized_values.iter().enumerate() {
                    if row_idx < normalized_rows.len() && col_idx < normalized_rows[row_idx].len() {
                        normalized_rows[row_idx][col_idx] = normalized_value.clone();
                    }
                }
            }
        }
        
        // Convert rows to Arrow arrays using the same type inference
        let arrays = self.string_rows_to_arrow_arrays_with_schema(&final_headers, &normalized_rows, &schema)?;
        
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|e| LeafError::Custom(format!("Failed to create record batch: {}", e)))?;
        
        // Register the table with proper schema, handling replacement if it already exists
        self.register_or_replace_table(table_name, batch.clone())?;
        
        // Store in our cache
        self.registered_tables.insert(table_name.to_string(), Arc::new(batch));
        
        Ok(inferred_delimiter)
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        // DataFusion doesn't support transactions in the same way
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> Result<()> {
        // DataFusion doesn't support transactions in the same way
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<()> {
        // DataFusion doesn't support transactions in the same way
        Ok(())
    }

    pub fn insert_record(&mut self, table_name: &str, values: &[String]) -> Result<()> {
        self.insert_data(table_name, &[values.to_vec()])
    }

    pub fn batch_insert(&mut self, table_name: &str, all_values: &[Vec<String>]) -> Result<()> {
        self.insert_data(table_name, all_values)
    }

    pub fn create_table_with_schema(&mut self, table_name: &str, columns: &[(&str, DataType)]) -> Result<()> {
        let fields: Vec<Field> = columns.iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect();
        
        let schema = Schema::new(fields);
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        
        self.ctx.register_batch(table_name, empty_batch)
            .map_err(|e| LeafError::Custom(format!("Failed to register table with schema: {}", e)))?;
        
        Ok(())
    }

    pub fn insert_record_batch(&mut self, table_name: &str, batch: &RecordBatch) -> Result<()> {
        // Clear cache for this table since data changed
        self.batch_cache.remove(table_name);
        
        // Register the batch as a table
        self.ctx.register_batch(table_name, batch.clone())
            .map_err(|e| LeafError::Custom(format!("Failed to register record batch: {}", e)))?;
        
        // Store in our cache
        self.registered_tables.insert(table_name.to_string(), Arc::new(batch.clone()));
        
        Ok(())
    }

    pub fn load_table_arrow_batch(&mut self, table_name: &str) -> Result<Arc<RecordBatch>> {
        if let Some(batch) = self.registered_tables.get(table_name) {
            return Ok(batch.clone());
        }
        
        // Try to query the table to get its data
        let query = format!("SELECT * FROM '{}'", table_name);
        let ctx = self.ctx.clone();
        
        let result = self.runtime.block_on(async {
            ctx.sql(&query).await
        }).map_err(|e| LeafError::Custom(format!("Failed to load table: {}", e)))?;
        
        let record_batches = self.runtime.block_on(async {
            result.collect().await
        }).map_err(|e| LeafError::Custom(format!("Failed to collect table data: {}", e)))?;
        
        if record_batches.is_empty() {
            return Err(LeafError::Custom("No data found in table".to_string()));
        }
        
        let batch = Arc::new(record_batches[0].clone());
        self.registered_tables.insert(table_name.to_string(), batch.clone());
        
        Ok(batch)
    }

    /// Non-mutable version of load_table_arrow_batch for use with Arc<Database>
    pub fn get_table_arrow_batch(&self, table_name: &str) -> Result<Arc<RecordBatch>> {
        if let Some(batch) = self.registered_tables.get(table_name) {
            return Ok(batch.clone());
        }
        
        // Try to query the table to get its data
        let query = format!("SELECT * FROM '{}'", table_name);
        let ctx = self.ctx.clone();
        
        let result = self.runtime.block_on(async {
            ctx.sql(&query).await
        }).map_err(|e| LeafError::Custom(format!("Failed to load table: {}", e)))?;
        
        let record_batches = self.runtime.block_on(async {
            result.collect().await
        }).map_err(|e| LeafError::Custom(format!("Failed to collect table data: {}", e)))?;
        
        if record_batches.is_empty() {
            return Err(LeafError::Custom("No data found in table".to_string()));
        }
        
        let batch = Arc::new(record_batches[0].clone());
        
        // Note: We can't insert into registered_tables here since we don't have mutable access
        // This is a limitation when working with Arc<Database>
        
        Ok(batch)
    }

    fn warn_if_cloud_folder(_path: &Path) {
        // DataFusion is in-memory, so no file locking concerns
    }

    // === HYBRID PERSISTENCE METHODS ===

    /// Save a table in both Arrow IPC (fast cache) and Parquet (persistent) formats
    pub fn save_table_dual(&mut self, table_name: &str, base_path: &Path) -> Result<()> {
        // Get the table data as RecordBatch
        let batch = self.load_table_arrow_batch(table_name)?;
        
        // Create directory if it doesn't exist
        std::fs::create_dir_all(base_path)
            .map_err(|e| LeafError::Custom(format!("Failed to create directory: {}", e)))?;
        
        // Save as Arrow IPC (fast cache)
        let arrow_path = base_path.join(format!("{}.arrow", table_name));
        self.save_table_arrow_ipc(table_name, &arrow_path)?;
        
        // For now, skip Parquet due to version conflicts
        // TODO: Add Parquet support when Arrow versions are compatible
                // Table saved successfully
        
        Ok(())
    }

    /// Save a table as Arrow IPC file (fast cache format)
    pub fn save_table_arrow_ipc(&mut self, table_name: &str, path: &Path) -> Result<()> {
        use datafusion::arrow::ipc::writer::FileWriter;
        use std::fs::File;
        
        let batch = self.load_table_arrow_batch(table_name)?;
        
        let file = File::create(path)
            .map_err(|e| LeafError::Custom(format!("Failed to create Arrow IPC file: {}", e)))?;
        
        let mut writer = FileWriter::try_new(file, &batch.schema())
            .map_err(|e| LeafError::Custom(format!("Failed to create Arrow IPC writer: {}", e)))?;
        
        writer.write(&batch)
            .map_err(|e| LeafError::Custom(format!("Failed to write Arrow IPC data: {}", e)))?;
        
        writer.finish()
            .map_err(|e| LeafError::Custom(format!("Failed to finish Arrow IPC file: {}", e)))?;
        
        Ok(())
    }

    /// Load a table from Arrow IPC file (fast cache format)
    pub fn load_table_arrow_ipc(&mut self, table_name: &str, path: &Path) -> Result<()> {
        use datafusion::arrow::ipc::reader::FileReader;
        use std::fs::File;
        
        let file = File::open(path)
            .map_err(|e| LeafError::Custom(format!("Failed to open Arrow IPC file: {}", e)))?;
        
        let reader = FileReader::try_new(file, None)
            .map_err(|e| LeafError::Custom(format!("Failed to create Arrow IPC reader: {}", e)))?;
        
        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| LeafError::Custom(format!("Failed to read Arrow IPC batch: {}", e)))?;
            batches.push(batch);
        }
        
        if batches.is_empty() {
            return Err(LeafError::Custom("No data found in Arrow IPC file".to_string()));
        }
        
        // Combine all batches into one (for simplicity)
        let combined_batch = if batches.len() == 1 {
            batches.remove(0)
        } else {
            // For multiple batches, we'd need to concatenate them
            // For now, just use the first batch
            batches.remove(0)
        };
        
        // Register the table
        self.insert_record_batch(table_name, &combined_batch)?;
        
        Ok(())
    }

    /// Load all tables from a directory (Arrow IPC format only for now)
    pub fn load_all_tables_from_directory(&mut self, directory: &Path) -> Result<Vec<String>> {
        let mut loaded_tables = Vec::new();
        
        if !directory.exists() {
            return Ok(loaded_tables);
        }
        
        for entry in std::fs::read_dir(directory)
            .map_err(|e| LeafError::Custom(format!("Failed to read directory: {}", e)))? {
            
            let entry = entry
                .map_err(|e| LeafError::Custom(format!("Failed to read directory entry: {}", e)))?;
            
            let path = entry.path();
            let file_name = path.file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| LeafError::Custom("Invalid file name".to_string()))?;
            
            if let Some(table_name) = file_name.strip_suffix(".arrow") {
                // Load Arrow IPC files
                match self.load_table_arrow_ipc(table_name, &path) {
                    Ok(_) => {
                        loaded_tables.push(table_name.to_string());
                        // Table loaded successfully
                    }
                    Err(e) => {
                        eprintln!("[Database] Failed to load table '{}' from Arrow IPC: {}", table_name, e);
                    }
                }
            }
        }
        
        Ok(loaded_tables)
    }

    /// Save all current tables in Arrow IPC format
    pub fn save_all_tables(&mut self, base_path: &Path) -> Result<Vec<String>> {
        let mut saved_tables = Vec::new();
        
        // Collect table names first to avoid borrowing issues
        let table_names: Vec<String> = self.registered_tables.keys().cloned().collect();
        
        for table_name in table_names {
            match self.save_table_dual(&table_name, base_path) {
                Ok(_) => {
                    saved_tables.push(table_name);
                }
                Err(e) => {
                    eprintln!("[Database] Failed to save table '{}': {}", table_name, e);
                }
            }
        }
        
        Ok(saved_tables)
    }
} 