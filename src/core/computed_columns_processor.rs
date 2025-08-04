use crate::core::{Database, DataTransformer, TransformationType};
use crate::ui::{ComputedColumnsRequest, ComputedColumnConfig, ComputationType};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::ipc::writer::FileWriter;
use anyhow::{Result, anyhow};
use std::path::Path;
use std::sync::Arc;
use std::fs::File;

pub struct ComputedColumnsProcessor {
    transformer: DataTransformer,
}

impl ComputedColumnsProcessor {
    pub fn new() -> Self {
        Self {
            transformer: DataTransformer::new(),
        }
    }
    
    pub fn process_request(
        &self,
        request: &ComputedColumnsRequest,
        database: &Database,
        output_dir: &Path,
    ) -> Result<String> {
        // Load the source table
        let batch = database.get_table_arrow_batch(&request.table_name)?;
        let batch = Arc::try_unwrap(batch).unwrap_or_else(|arc| (*arc).clone());
        
        // Apply each transformation
        let mut current_batch = batch;
        for config in &request.configurations {
            current_batch = self.apply_single_transformation(current_batch, config)?;
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
    
    fn apply_single_transformation(
        &self,
        batch: RecordBatch,
        config: &ComputedColumnConfig,
    ) -> Result<RecordBatch> {
        match &config.computation_type {
            ComputationType::Delta => {
                self.transformer.apply_delta(&batch, &config.source_column, &config.output_name)
            }
            ComputationType::CumulativeSum => {
                self.transformer.apply_cumulative_sum(&batch, &config.source_column, &config.output_name)
            }
            ComputationType::Percentage => {
                self.transformer.apply_percentage(&batch, &config.source_column, &config.output_name)
            }
            ComputationType::Ratio => {
                if let Some(second_column) = &config.second_column {
                    self.transformer.apply_ratio(&batch, &config.source_column, second_column, &config.output_name)
                } else {
                    Err(anyhow!("Ratio computation requires a second column"))
                }
            }
            ComputationType::MovingAverage => {
                // TODO: Implement moving average
                Err(anyhow!("Moving average not yet implemented"))
            }
            ComputationType::ZScore => {
                // TODO: Implement z-score
                Err(anyhow!("Z-score normalization not yet implemented"))
            }
        }
    }
    
    fn generate_output_filename(
        &self,
        table_name: &str,
        configurations: &[ComputedColumnConfig],
    ) -> String {
        // Extract base name without extension
        let base_name = table_name.trim_end_matches(".arrow")
            .trim_end_matches(".csv")
            .trim_end_matches(".parquet");
        
        // Create suffix based on transformations
        let mut suffixes = Vec::new();
        for config in configurations {
            let suffix = match &config.computation_type {
                ComputationType::Delta => format!("delta_{}", config.source_column),
                ComputationType::CumulativeSum => format!("cumsum_{}", config.source_column),
                ComputationType::Percentage => format!("pct_{}", config.source_column),
                ComputationType::Ratio => format!("ratio_{}_{}", 
                    config.source_column, 
                    config.second_column.as_ref().unwrap_or(&"unknown".to_string())
                ),
                ComputationType::MovingAverage => format!("ma{}_{}", config.window_size, config.source_column),
                ComputationType::ZScore => format!("zscore_{}", config.source_column),
            };
            suffixes.push(suffix);
        }
        
        // Limit suffix length to avoid overly long filenames
        let suffix = if suffixes.len() > 3 {
            format!("{}_and_{}_more", suffixes[..3].join("_"), suffixes.len() - 3)
        } else {
            suffixes.join("_")
        };
        
        format!("{}_{}.arrow", base_name, suffix)
    }
    
    fn save_batch(&self, batch: &RecordBatch, output_path: &Path) -> Result<()> {
        let file = File::create(output_path)?;
        let mut writer = FileWriter::try_new(file, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
        Ok(())
    }
    
    pub fn generate_preview(
        &self,
        database: &Database,
        table_name: &str,
        config: &ComputedColumnConfig,
        limit: usize,
    ) -> Result<Vec<(usize, String, Option<String>, String)>> {
        // For now, return mock data
        // TODO: Implement actual preview generation
        Ok(vec![
            (1, "63.78".to_string(), None, "NULL".to_string()),
            (2, "116.97".to_string(), None, "53.19".to_string()),
            (3, "194.03".to_string(), None, "77.06".to_string()),
        ])
    }
    
    fn format_array_value(&self, array: &Arc<dyn datafusion::arrow::array::Array>, idx: usize) -> String {
        use datafusion::arrow::array::{Int64Array, Float64Array, StringArray};
        use datafusion::arrow::datatypes::DataType;
        
        if array.is_null(idx) {
            return "NULL".to_string();
        }
        
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                arr.value(idx).to_string()
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                format!("{:.2}", arr.value(idx))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                arr.value(idx).to_string()
            }
            _ => "?".to_string(),
        }
    }
}