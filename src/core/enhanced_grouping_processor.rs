use crate::core::Database;
use crate::ui::{EnhancedGroupingRequest, GroupingConfig, GroupingRule};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::ipc::writer::FileWriter;
use anyhow::{Result, anyhow};
use std::path::Path;
use std::sync::Arc;
use std::fs::File;

pub struct EnhancedGroupingProcessor;

impl EnhancedGroupingProcessor {
    pub fn new() -> Self {
        Self
    }
    
    pub fn process_request(
        &self,
        request: &EnhancedGroupingRequest,
        database: &Database,
        output_dir: &Path,
    ) -> Result<String> {
        // Load the source table
        let batch = database.get_table_arrow_batch(&request.table_name)?;
        let batch = Arc::try_unwrap(batch).unwrap_or_else(|arc| (*arc).clone());
        
        // Apply each grouping configuration
        let mut current_batch = batch;
        for config in &request.configurations {
            current_batch = self.apply_grouping(current_batch, config)?;
        }
        
        // Generate output filename
        let output_filename = if let Some(custom_name) = &request.output_filename {
            // Ensure .arrow extension
            if custom_name.ends_with(".arrow") {
                custom_name.clone()
            } else {
                format!("{}.arrow", custom_name)
            }
        } else {
            self.generate_output_filename(&request.table_name, &request.configurations)
        };
        let output_path = output_dir.join(&output_filename);
        
        // Save the transformed data
        self.save_batch(&current_batch, &output_path)?;
        
        Ok(output_filename)
    }
    
    fn apply_grouping(
        &self,
        batch: RecordBatch,
        config: &GroupingConfig,
    ) -> Result<RecordBatch> {
        let group_ids = match &config.rule {
            GroupingRule::ValueChange { column } => {
                self.calculate_value_change_ids(&batch, column, config.reset_on_change)?
            }
            GroupingRule::ValueEquals { column, value } => {
                self.calculate_value_equals_ids(&batch, column, value, config.reset_on_change)?
            }
            GroupingRule::IsEmpty { column } => {
                self.calculate_is_empty_ids(&batch, column, config.reset_on_change)?
            }

        };
        
        // Add the new column to the batch
        let mut new_fields = batch.schema().fields().to_vec();
        new_fields.push(Arc::new(Field::new(&config.output_column, DataType::Int64, false)));
        let new_schema = Arc::new(Schema::new(new_fields));
        
        let mut new_arrays = batch.columns().to_vec();
        new_arrays.push(group_ids);
        
        Ok(RecordBatch::try_new(new_schema, new_arrays)?)
    }
    
    fn calculate_value_change_ids(
        &self,
        batch: &RecordBatch,
        column: &str,
        reset_on_change: bool,
    ) -> Result<ArrayRef> {
        let schema = batch.schema();
        let column_idx = schema.column_with_name(column)
            .ok_or_else(|| anyhow!("Column '{}' not found", column))?.0;
        let array = batch.column(column_idx);
        
        let mut ids = Vec::with_capacity(array.len());
        let mut current_id = 0i64;
        let mut previous_value: Option<String> = None;
        
        for i in 0..array.len() {
            let current_value = self.get_value_as_string(array, i)?;
            
            if i > 0 && Some(&current_value) != previous_value.as_ref() {
                if reset_on_change {
                    current_id = 0;
                } else {
                    current_id += 1;
                }
            }
            
            ids.push(current_id);
            previous_value = Some(current_value);
        }
        
        Ok(Arc::new(Int64Array::from(ids)))
    }
    
    fn calculate_value_equals_ids(
        &self,
        batch: &RecordBatch,
        column: &str,
        target_value: &str,
        reset_on_change: bool,
    ) -> Result<ArrayRef> {
        let schema = batch.schema();
        let column_idx = schema.column_with_name(column)
            .ok_or_else(|| anyhow!("Column '{}' not found", column))?.0;
        let array = batch.column(column_idx);
        
        let mut ids = Vec::with_capacity(array.len());
        let mut current_id = 0i64;
        let mut in_matching_group = false;
        
        for i in 0..array.len() {
            let current_value = self.get_value_as_string(array, i)?;
            let matches = current_value == target_value;
            
            if matches && !in_matching_group {
                if !reset_on_change {
                    current_id += 1;
                } else {
                    current_id = 0;
                }
                in_matching_group = true;
            } else if !matches && in_matching_group {
                in_matching_group = false;
            }
            
            ids.push(if in_matching_group { current_id } else { -1 });
        }
        
        Ok(Arc::new(Int64Array::from(ids)))
    }
    
    fn calculate_is_empty_ids(
        &self,
        batch: &RecordBatch,
        column: &str,
        reset_on_change: bool,
    ) -> Result<ArrayRef> {
        let schema = batch.schema();
        let column_idx = schema.column_with_name(column)
            .ok_or_else(|| anyhow!("Column '{}' not found", column))?.0;
        let array = batch.column(column_idx);
        
        let mut ids = Vec::with_capacity(array.len());
        let mut current_id = 0i64;
        let mut in_group = false;
        let mut first_group = true;
        
        for i in 0..array.len() {
            let is_empty = if array.is_null(i) {
                true
            } else {
                let value = self.get_value_as_string(array, i)?;
                value.is_empty()
            };
            
            if is_empty && !in_group {
                if !first_group {
                    if !reset_on_change {
                        current_id += 1;
                    } else {
                        current_id = 0;
                    }
                }
                first_group = false;
                in_group = true;
            } else if !is_empty {
                in_group = false;
            }
            
            ids.push(current_id);
        }
        
        Ok(Arc::new(Int64Array::from(ids)))
    }

    
    fn get_value_as_string(&self, array: &ArrayRef, idx: usize) -> Result<String> {
        if array.is_null(idx) {
            return Ok("".to_string());
        }
        
        match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("Failed to cast to string array"))?;
                Ok(string_array.value(idx).to_string())
            }
            DataType::Int64 => {
                let int_array = array.as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow!("Failed to cast to int64 array"))?;
                Ok(int_array.value(idx).to_string())
            }
            DataType::Float64 => {
                use datafusion::arrow::array::Float64Array;
                let float_array = array.as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| anyhow!("Failed to cast to float64 array"))?;
                Ok(float_array.value(idx).to_string())
            }
            DataType::Boolean => {
                use datafusion::arrow::array::BooleanArray;
                let bool_array = array.as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| anyhow!("Failed to cast to boolean array"))?;
                Ok(bool_array.value(idx).to_string())
            }
            DataType::Int32 => {
                use datafusion::arrow::array::Int32Array;
                let int_array = array.as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| anyhow!("Failed to cast to int32 array"))?;
                Ok(int_array.value(idx).to_string())
            }
            DataType::Float32 => {
                use datafusion::arrow::array::Float32Array;
                let float_array = array.as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| anyhow!("Failed to cast to float32 array"))?;
                Ok(float_array.value(idx).to_string())
            }
            DataType::Date32 => {
                use datafusion::arrow::array::Date32Array;
                let date_array = array.as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| anyhow!("Failed to cast to date32 array"))?;
                Ok(date_array.value(idx).to_string())
            }
            DataType::Date64 => {
                use datafusion::arrow::array::Date64Array;
                let date_array = array.as_any()
                    .downcast_ref::<Date64Array>()
                    .ok_or_else(|| anyhow!("Failed to cast to date64 array"))?;
                Ok(date_array.value(idx).to_string())
            }
            DataType::Timestamp(unit, _) => {
                use chrono::NaiveDateTime;
                match unit {
                    TimeUnit::Nanosecond => {
                        let timestamp_array = array.as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| anyhow!("Failed to cast to timestamp nanosecond array"))?;
                        let ts = timestamp_array.value(idx);
                        let dt = NaiveDateTime::from_timestamp_opt(ts / 1_000_000_000, (ts % 1_000_000_000) as u32)
                            .ok_or_else(|| anyhow!("Invalid timestamp"))?;
                        Ok(dt.format("%H:%M:%S.%3f").to_string())
                    }
                    TimeUnit::Millisecond => {
                        use datafusion::arrow::array::TimestampMillisecondArray;
                        let timestamp_array = array.as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .ok_or_else(|| anyhow!("Failed to cast to timestamp millisecond array"))?;
                        let ts = timestamp_array.value(idx);
                        let dt = NaiveDateTime::from_timestamp_opt(ts / 1_000, ((ts % 1_000) * 1_000_000) as u32)
                            .ok_or_else(|| anyhow!("Invalid timestamp"))?;
                        Ok(dt.format("%H:%M:%S.%3f").to_string())
                    }
                    TimeUnit::Microsecond => {
                        use datafusion::arrow::array::TimestampMicrosecondArray;
                        let timestamp_array = array.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .ok_or_else(|| anyhow!("Failed to cast to timestamp microsecond array"))?;
                        let ts = timestamp_array.value(idx);
                        let dt = NaiveDateTime::from_timestamp_opt(ts / 1_000_000, ((ts % 1_000_000) * 1_000) as u32)
                            .ok_or_else(|| anyhow!("Invalid timestamp"))?;
                        Ok(dt.format("%H:%M:%S.%3f").to_string())
                    }
                    TimeUnit::Second => {
                        use datafusion::arrow::array::TimestampSecondArray;
                        let timestamp_array = array.as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .ok_or_else(|| anyhow!("Failed to cast to timestamp second array"))?;
                        let ts = timestamp_array.value(idx);
                        let dt = NaiveDateTime::from_timestamp_opt(ts, 0)
                            .ok_or_else(|| anyhow!("Invalid timestamp"))?;
                        Ok(dt.format("%H:%M:%S").to_string())
                    }
                }
            }
            _ => Err(anyhow!("Unsupported data type: {:?}", array.data_type()))
        }
    }
    
    fn generate_output_filename(
        &self,
        table_name: &str,
        configurations: &[GroupingConfig],
    ) -> String {
        let base_name = table_name.trim_end_matches(".arrow")
            .trim_end_matches(".csv")
            .trim_end_matches(".parquet");
        
        let mut suffixes = Vec::new();
        for config in configurations {
            suffixes.push(config.output_column.clone());
        }
        
        let suffix = if suffixes.len() > 3 {
            format!("{}_and_{}_more", suffixes[..3].join("_"), suffixes.len() - 3)
        } else {
            suffixes.join("_")
        };
        
        format!("{}_with_{}.arrow", base_name, suffix)
    }
    
    fn save_batch(&self, batch: &RecordBatch, output_path: &Path) -> Result<()> {
        let file = File::create(output_path)?;
        let mut writer = FileWriter::try_new(file, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
        Ok(())
    }
}