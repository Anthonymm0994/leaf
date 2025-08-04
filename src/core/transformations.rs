use datafusion::arrow::array::{ArrayRef, StringArray, Int64Array, Float64Array, BooleanArray, TimestampNanosecondArray, Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::compute;
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use std::sync::Arc;
use std::path::PathBuf;
use chrono::{DateTime, Utc, NaiveDateTime};

#[derive(Debug, Clone, PartialEq)]
pub enum TransformationType {
    Delta,
    TimeBin,
    RowId,
    CumulativeSum,
    Percentage,
    Ratio,
    MovingAverage,
    ZScore,
}

#[derive(Debug, Clone)]
pub struct TransformationConfig {
    pub transformation_type: TransformationType,
    pub selected_columns: Vec<String>,
    pub output_column_names: Vec<String>, // For delta transformations with multiple columns
    pub output_column_name: String, // For single column transformations
    pub bin_size: Option<String>,
    pub time_column: Option<String>,
    pub grouping_columns: Option<Vec<String>>,
}

pub struct DataTransformer;

impl DataTransformer {
    pub fn new() -> Self {
        Self
    }

    /// Apply delta transformation to compute differences between consecutive rows
    pub fn apply_delta(&self, batch: &RecordBatch, column_name: &str, output_name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        let column_idx = schema.column_with_name(column_name)
            .ok_or_else(|| anyhow!("Column '{}' not found", column_name))?.0;

        let array = batch.column(column_idx);
        let delta_array = self.compute_delta(array)?;

        // Create new schema with additional column
        let mut new_fields = schema.fields().to_vec();
        new_fields.push(Arc::new(Field::new(output_name, delta_array.data_type().clone(), true)));
        let new_schema = Arc::new(Schema::new(new_fields));

        // Create new arrays with the delta column
        let mut new_arrays = batch.columns().to_vec();
        new_arrays.push(delta_array);

        Ok(RecordBatch::try_new(new_schema, new_arrays)?)
    }

    /// Apply delta transformation to multiple columns with custom output names
    pub fn apply_delta_multiple_custom(&self, batch: &RecordBatch, columns: &[String], output_names: &[String]) -> Result<RecordBatch> {
        if columns.len() != output_names.len() {
            return Err(anyhow!("Number of columns must match number of output names"));
        }
        
        let mut current_batch = batch.clone();
        
        for (column_name, output_name) in columns.iter().zip(output_names.iter()) {
            current_batch = self.apply_delta(&current_batch, column_name, output_name)?;
        }

        Ok(current_batch)
    }

    /// Apply time binning transformation
    pub fn apply_time_bin(&self, batch: &RecordBatch, time_column: &str, bin_size_seconds: f64, output_name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        let time_column_idx = schema.column_with_name(time_column)
            .ok_or_else(|| anyhow!("Time column '{}' not found", time_column))?.0;

        let time_array = batch.column(time_column_idx);
        let bin_array = self.compute_time_bins(time_array, bin_size_seconds)?;

        // Create new schema with bin column
        let mut new_fields = schema.fields().to_vec();
        new_fields.push(Arc::new(Field::new(output_name, DataType::Int64, true)));
        let new_schema = Arc::new(Schema::new(new_fields));

        // Create new arrays with the bin column
        let mut new_arrays = batch.columns().to_vec();
        new_arrays.push(bin_array);

        Ok(RecordBatch::try_new(new_schema, new_arrays)?)
    }

    /// Apply row ID transformation
    pub fn apply_row_id(&self, batch: &RecordBatch, output_name: &str, grouping_columns: Option<&[String]>) -> Result<RecordBatch> {
        let schema = batch.schema();
        let row_count = batch.num_rows();
        
        // Create row ID array
        let row_ids: Vec<i64> = (0..row_count as i64).collect();
        let row_id_array = Arc::new(Int64Array::from(row_ids));

        // Create new schema with row ID column
        let mut new_fields = schema.fields().to_vec();
        new_fields.push(Arc::new(Field::new(output_name, DataType::Int64, false)));
        let new_schema = Arc::new(Schema::new(new_fields));

        // Create new arrays with the row ID column
        let mut new_arrays = batch.columns().to_vec();
        new_arrays.push(row_id_array);

        let mut result_batch = RecordBatch::try_new(new_schema, new_arrays)?;

        // If grouping columns are specified, create group IDs
        if let Some(grouping_cols) = grouping_columns {
            let group_id_name = format!("{}_group", output_name);
            result_batch = self.apply_group_id(&result_batch, grouping_cols, &group_id_name)?;
        }

        Ok(result_batch)
    }

    /// Compute delta between consecutive values in an array
    fn compute_delta(&self, array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut deltas = Vec::with_capacity(int_array.len());
                
                for i in 0..int_array.len() {
                    if i == 0 {
                        deltas.push(None); // First row has no previous value
                    } else {
                        // Check if current value is null
                        if int_array.is_null(i) {
                            deltas.push(None);
                        } else {
                            let current = int_array.value(i);
                            // Check if previous value is null
                            if int_array.is_null(i - 1) {
                                deltas.push(None); // Can't compute delta if previous value is null
                            } else {
                                let previous = int_array.value(i - 1);
                                deltas.push(Some(current - previous));
                            }
                        }
                    }
                }
                
                // Use the builder pattern correctly
                let mut builder = Int64Array::builder(deltas.len());
                for delta in deltas {
                    match delta {
                        Some(val) => builder.append_value(val),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let mut deltas = Vec::with_capacity(float_array.len());
                
                for i in 0..float_array.len() {
                    if i == 0 {
                        deltas.push(None); // First row has no previous value
                    } else {
                        // Check if current value is null
                        if float_array.is_null(i) {
                            deltas.push(None);
                        } else {
                            let current = float_array.value(i);
                            // Check if previous value is null
                            if float_array.is_null(i - 1) {
                                deltas.push(None); // Can't compute delta if previous value is null
                            } else {
                                let previous = float_array.value(i - 1);
                                deltas.push(Some(current - previous));
                            }
                        }
                    }
                }
                
                // Use the builder pattern correctly
                let mut builder = Float64Array::builder(deltas.len());
                for delta in deltas {
                    match delta {
                        Some(val) => builder.append_value(val),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => Err(anyhow!("Unsupported data type for delta computation: {:?}", array.data_type())),
        }
    }

    /// Compute time bins based on timestamp values
    fn compute_time_bins(&self, time_array: &ArrayRef, bin_size_seconds: f64) -> Result<ArrayRef> {
        match time_array.data_type() {
            DataType::Timestamp(_, _) => {
                let timestamp_array = time_array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                let mut bins = Vec::with_capacity(timestamp_array.len());
                
                for i in 0..timestamp_array.len() {
                    if timestamp_array.is_null(i) {
                        bins.push(None);
                    } else {
                        let timestamp_nanos = timestamp_array.value(i);
                        let timestamp_seconds = timestamp_nanos as f64 / 1_000_000_000.0;
                        let bin = (timestamp_seconds / bin_size_seconds).floor() as i64;
                        bins.push(Some(bin));
                    }
                }
                
                Ok(Arc::new(Int64Array::from(bins)))
            }
            _ => Err(anyhow!("Unsupported data type for time binning: {:?}", time_array.data_type())),
        }
    }

    /// Apply group ID transformation based on grouping columns
    fn apply_group_id(&self, batch: &RecordBatch, grouping_columns: &[String], output_name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut group_ids = Vec::with_capacity(batch.num_rows());
        let mut current_group_id = 0i64;
        let mut group_key = String::new();
        let mut previous_group_key = String::new();

        for row_idx in 0..batch.num_rows() {
            // Build group key from grouping columns
            group_key.clear();
            for col_name in grouping_columns {
                let col_idx = schema.column_with_name(col_name)
                    .ok_or_else(|| anyhow!("Grouping column '{}' not found", col_name))?.0;
                let array = batch.column(col_idx);
                
                let value = self.format_array_value(array, row_idx);
                group_key.push_str(&value);
                group_key.push('|');
            }

            // Check if this is a new group
            if row_idx == 0 || group_key != previous_group_key {
                current_group_id += 1;
                previous_group_key = group_key.clone();
            }

            group_ids.push(current_group_id);
        }

        let group_id_array = Arc::new(Int64Array::from(group_ids));

        // Create new schema with group ID column
        let mut new_fields = schema.fields().to_vec();
        new_fields.push(Arc::new(Field::new(output_name, DataType::Int64, false)));
        let new_schema = Arc::new(Schema::new(new_fields));

        // Create new arrays with the group ID column
        let mut new_arrays = batch.columns().to_vec();
        new_arrays.push(group_id_array);

        Ok(RecordBatch::try_new(new_schema, new_arrays)?)
    }

    /// Format array value as string for grouping
    fn format_array_value(&self, array: &ArrayRef, row_idx: usize) -> String {
        if row_idx >= array.len() || array.is_null(row_idx) {
            return "null".to_string();
        }

        match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                string_array.value(row_idx).to_string()
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                int_array.value(row_idx).to_string()
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                format!("{:.2}", float_array.value(row_idx))
            }
            DataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                bool_array.value(row_idx).to_string()
            }
            _ => format!("{:?}", array.data_type()),
        }
    }

    /// Save transformed data to a new Arrow file
    pub fn save_transformed_data(&self, batch: &RecordBatch, output_path: &PathBuf) -> Result<()> {
        use datafusion::arrow::ipc::writer::FileWriter;
        use std::fs::File;

        let file = File::create(output_path)?;
        let mut writer = FileWriter::try_new(file, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
        
        Ok(())
    }

    /// Get available numeric columns from a batch
    pub fn get_numeric_columns(&self, batch: &RecordBatch) -> Vec<String> {
        let schema = batch.schema();
        let mut numeric_columns = Vec::new();

        for field in schema.fields() {
            match field.data_type() {
                DataType::Int64 | DataType::Float64 => {
                    numeric_columns.push(field.name().to_string());
                }
                _ => {}
            }
        }

        numeric_columns
    }

    /// Get available timestamp columns from a batch
    pub fn get_timestamp_columns(&self, batch: &RecordBatch) -> Vec<String> {
        let schema = batch.schema();
        let mut timestamp_columns = Vec::new();

        for field in schema.fields() {
            match field.data_type() {
                DataType::Timestamp(_, _) => {
                    timestamp_columns.push(field.name().to_string());
                }
                _ => {}
            }
        }

        timestamp_columns
    }

    /// Apply cumulative sum transformation
    pub fn apply_cumulative_sum(&self, batch: &RecordBatch, column_name: &str, output_name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        let column_idx = schema.column_with_name(column_name)
            .ok_or_else(|| anyhow!("Column '{}' not found", column_name))?.0;

        let array = batch.column(column_idx);
        let cumsum_array = self.compute_cumulative_sum(array)?;

        // Create new schema with additional column
        let mut new_fields = schema.fields().to_vec();
        new_fields.push(Arc::new(Field::new(output_name, cumsum_array.data_type().clone(), true)));
        let new_schema = Arc::new(Schema::new(new_fields));

        // Create new arrays with the cumulative sum column
        let mut new_arrays = batch.columns().to_vec();
        new_arrays.push(cumsum_array);

        Ok(RecordBatch::try_new(new_schema, new_arrays)?)
    }

    /// Compute cumulative sum of an array
    fn compute_cumulative_sum(&self, array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut cumsum = 0i64;
                let mut values = Vec::with_capacity(int_array.len());
                
                for i in 0..int_array.len() {
                    if int_array.is_null(i) {
                        values.push(None);
                    } else {
                        cumsum += int_array.value(i);
                        values.push(Some(cumsum));
                    }
                }
                
                Ok(Arc::new(Int64Array::from(values)))
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let mut cumsum = 0.0f64;
                let mut values = Vec::with_capacity(float_array.len());
                
                for i in 0..float_array.len() {
                    if float_array.is_null(i) {
                        values.push(None);
                    } else {
                        cumsum += float_array.value(i);
                        values.push(Some(cumsum));
                    }
                }
                
                Ok(Arc::new(Float64Array::from(values)))
            }
            _ => Err(anyhow!("Unsupported data type for cumulative sum: {:?}", array.data_type())),
        }
    }

    /// Apply percentage transformation (each value as percentage of total)
    pub fn apply_percentage(&self, batch: &RecordBatch, column_name: &str, output_name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        let column_idx = schema.column_with_name(column_name)
            .ok_or_else(|| anyhow!("Column '{}' not found", column_name))?.0;

        let array = batch.column(column_idx);
        let pct_array = self.compute_percentage(array)?;

        // Create new schema with additional column
        let mut new_fields = schema.fields().to_vec();
        new_fields.push(Arc::new(Field::new(output_name, DataType::Float64, true)));
        let new_schema = Arc::new(Schema::new(new_fields));

        // Create new arrays with the percentage column
        let mut new_arrays = batch.columns().to_vec();
        new_arrays.push(pct_array);

        Ok(RecordBatch::try_new(new_schema, new_arrays)?)
    }

    /// Compute percentage of total for each value
    fn compute_percentage(&self, array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut total = 0i64;
                
                // First pass: calculate total
                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        total += int_array.value(i);
                    }
                }
                
                // Second pass: calculate percentages
                let mut values = Vec::with_capacity(int_array.len());
                for i in 0..int_array.len() {
                    if int_array.is_null(i) || total == 0 {
                        values.push(None);
                    } else {
                        let pct = (int_array.value(i) as f64 / total as f64) * 100.0;
                        values.push(Some(pct));
                    }
                }
                
                Ok(Arc::new(Float64Array::from(values)))
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let mut total = 0.0f64;
                
                // First pass: calculate total
                for i in 0..float_array.len() {
                    if !float_array.is_null(i) {
                        total += float_array.value(i);
                    }
                }
                
                // Second pass: calculate percentages
                let mut values = Vec::with_capacity(float_array.len());
                for i in 0..float_array.len() {
                    if float_array.is_null(i) || total == 0.0 {
                        values.push(None);
                    } else {
                        let pct = (float_array.value(i) / total) * 100.0;
                        values.push(Some(pct));
                    }
                }
                
                Ok(Arc::new(Float64Array::from(values)))
            }
            _ => Err(anyhow!("Unsupported data type for percentage: {:?}", array.data_type())),
        }
    }

    /// Apply ratio transformation (column A / column B)
    pub fn apply_ratio(&self, batch: &RecordBatch, numerator: &str, denominator: &str, output_name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        
        let num_idx = schema.column_with_name(numerator)
            .ok_or_else(|| anyhow!("Numerator column '{}' not found", numerator))?.0;
        let den_idx = schema.column_with_name(denominator)
            .ok_or_else(|| anyhow!("Denominator column '{}' not found", denominator))?.0;

        let num_array = batch.column(num_idx);
        let den_array = batch.column(den_idx);
        let ratio_array = self.compute_ratio(num_array, den_array)?;

        // Create new schema with additional column
        let mut new_fields = schema.fields().to_vec();
        new_fields.push(Arc::new(Field::new(output_name, DataType::Float64, true)));
        let new_schema = Arc::new(Schema::new(new_fields));

        // Create new arrays with the ratio column
        let mut new_arrays = batch.columns().to_vec();
        new_arrays.push(ratio_array);

        Ok(RecordBatch::try_new(new_schema, new_arrays)?)
    }

    /// Compute ratio between two arrays
    fn compute_ratio(&self, numerator: &ArrayRef, denominator: &ArrayRef) -> Result<ArrayRef> {
        if numerator.len() != denominator.len() {
            return Err(anyhow!("Arrays must have the same length for ratio calculation"));
        }

        let mut values = Vec::with_capacity(numerator.len());

        match (numerator.data_type(), denominator.data_type()) {
            (DataType::Float64, DataType::Float64) => {
                let num_array = numerator.as_any().downcast_ref::<Float64Array>().unwrap();
                let den_array = denominator.as_any().downcast_ref::<Float64Array>().unwrap();
                
                for i in 0..num_array.len() {
                    if num_array.is_null(i) || den_array.is_null(i) || den_array.value(i) == 0.0 {
                        values.push(None);
                    } else {
                        values.push(Some(num_array.value(i) / den_array.value(i)));
                    }
                }
            }
            (DataType::Int64, DataType::Int64) => {
                let num_array = numerator.as_any().downcast_ref::<Int64Array>().unwrap();
                let den_array = denominator.as_any().downcast_ref::<Int64Array>().unwrap();
                
                for i in 0..num_array.len() {
                    if num_array.is_null(i) || den_array.is_null(i) || den_array.value(i) == 0 {
                        values.push(None);
                    } else {
                        values.push(Some(num_array.value(i) as f64 / den_array.value(i) as f64));
                    }
                }
            }
            (DataType::Float64, DataType::Int64) => {
                let num_array = numerator.as_any().downcast_ref::<Float64Array>().unwrap();
                let den_array = denominator.as_any().downcast_ref::<Int64Array>().unwrap();
                
                for i in 0..num_array.len() {
                    if num_array.is_null(i) || den_array.is_null(i) || den_array.value(i) == 0 {
                        values.push(None);
                    } else {
                        values.push(Some(num_array.value(i) / den_array.value(i) as f64));
                    }
                }
            }
            (DataType::Int64, DataType::Float64) => {
                let num_array = numerator.as_any().downcast_ref::<Int64Array>().unwrap();
                let den_array = denominator.as_any().downcast_ref::<Float64Array>().unwrap();
                
                for i in 0..num_array.len() {
                    if num_array.is_null(i) || den_array.is_null(i) || den_array.value(i) == 0.0 {
                        values.push(None);
                    } else {
                        values.push(Some(num_array.value(i) as f64 / den_array.value(i)));
                    }
                }
            }
            _ => return Err(anyhow!("Unsupported data types for ratio: {:?} / {:?}", 
                                   numerator.data_type(), denominator.data_type())),
        }

        Ok(Arc::new(Float64Array::from(values)))
    }

    /// Test function to verify null handling in delta computation
    pub fn test_delta_null_handling(&self) -> Result<()> {
        use datafusion::arrow::array::Int64Array;
        
        // Create a test array: [100, 200, 300, 400]
        let test_values = vec![100, 200, 300, 400];
        let test_array = Arc::new(Int64Array::from(test_values.clone())) as ArrayRef;
        
        // Compute delta
        let delta_array = self.compute_delta(&test_array)?;
        let delta_int_array = delta_array.as_any().downcast_ref::<Int64Array>().unwrap();
        
        println!("Test array: {:?}", test_values);
        println!("Delta array length: {}", delta_int_array.len());
        
        // Check each value
        for i in 0..delta_int_array.len() {
            if delta_int_array.is_null(i) {
                println!("Row {}: NULL", i);
            } else {
                println!("Row {}: {}", i, delta_int_array.value(i));
            }
        }
        
        // Verify first row is null
        assert!(delta_int_array.is_null(0), "First row should be null");
        assert!(!delta_int_array.is_null(1), "Second row should not be null");
        assert_eq!(delta_int_array.value(1), 100, "Second row should be 100");
        
        Ok(())
    }
} 