use egui;
use datafusion::arrow::array::{ArrayRef, StringArray, Int64Array, Float64Array, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use crate::core::{Database, TableInfo, DataTransformer, TransformationType, TransformationConfig};
use std::sync::Arc;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct TransformationDialog {
    pub visible: bool,
    pub selected_table: Option<String>,
    pub transformations: Vec<DeltaTransformation>, // Multiple delta transformations
    pub available_tables: Vec<TableInfo>,
    pub available_columns: Vec<String>,
    pub error_message: Option<String>,
    pub success_message: Option<String>,
    // Local state for new column selection
    pub selected_column: String,
    pub output_name: String,
}

#[derive(Debug, Clone)]
pub struct DeltaTransformation {
    pub selected_columns: Vec<String>,
    pub output_column_names: Vec<String>,
}

impl Default for TransformationDialog {
    fn default() -> Self {
        Self {
            visible: false,
            selected_table: None,
            transformations: Vec::new(),
            available_tables: Vec::new(),
            available_columns: Vec::new(),
            error_message: None,
            success_message: None,
            selected_column: String::new(),
            output_name: String::new(),
        }
    }
}

impl TransformationDialog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn show(&mut self, ctx: &egui::Context, database: &Database) -> Option<TransformationRequest> {
        if !self.visible {
            return None;
        }

        let mut result = None;
        let mut table_selected = false;
        let mut selected_table_name = None;
        let mut apply_clicked = false;
        let mut cancel_clicked = false;
        let mut add_column_clicked = false;
        let mut remove_column_clicked = false;
        let mut column_to_remove = None;

        egui::Window::new("Delta Transformations")
            .open(&mut self.visible)
            .resizable(true)
            .default_size([500.0, 600.0])
            .show(ctx, |ui| {
                ui.heading("Delta Transformations");
                ui.label("Add delta columns to your data");
                ui.separator();

                // Table selection
                ui.label("Select Table:");
                egui::ComboBox::from_id_source("table_select")
                    .selected_text(self.selected_table.as_deref().unwrap_or("Select a table"))
                    .show_ui(ui, |ui| {
                        for table in &self.available_tables {
                            if ui.selectable_label(
                                self.selected_table.as_deref() == Some(&table.name),
                                &table.name,
                            ).clicked() {
                                selected_table_name = Some(table.name.clone());
                                table_selected = true;
                            }
                        }
                    });

                if let Some(_table_name) = &self.selected_table {
                    ui.separator();
                    
                                         // Show current transformations in a compact table
                     if !self.transformations.is_empty() {
                         ui.label("Current Delta Columns:");
                         ui.group(|ui| {
                             // Compact header
                             ui.horizontal(|ui| {
                                 ui.set_enabled(false);
                                 ui.label("Column → Delta Name");
                                 ui.label("Actions");
                             });
                             ui.set_enabled(true);
                             ui.separator();
                             
                             // Compact rows
                             let mut updated_names = Vec::new();
                             for (i, transformation) in self.transformations.iter_mut().enumerate() {
                                 for (j, (col, output_name)) in transformation.selected_columns.iter().zip(transformation.output_column_names.iter()).enumerate() {
                                     ui.horizontal(|ui| {
                                         ui.label(format!("{} →", col));
                                         let mut name = output_name.clone();
                                         if ui.text_edit_singleline(&mut name).changed() {
                                             updated_names.push((i, j, name));
                                         }
                                         if ui.button("🗑️").clicked() {
                                             remove_column_clicked = true;
                                             column_to_remove = Some((i, j));
                                         }
                                     });
                                 }
                             }
                             
                             // Apply name changes
                             for (transformation_idx, column_idx, new_name) in updated_names {
                                 if transformation_idx < self.transformations.len() {
                                     let transformation = &mut self.transformations[transformation_idx];
                                     if column_idx < transformation.output_column_names.len() {
                                         transformation.output_column_names[column_idx] = new_name;
                                     }
                                 }
                             }
                         });
                         ui.separator();
                     }

                                         // Add new column section - more compact
                     ui.label("Add New Delta Column:");
                     ui.group(|ui| {
                         ui.horizontal(|ui| {
                             ui.label("Column:");
                             egui::ComboBox::from_id_source("column_select")
                                 .selected_text(if self.selected_column.is_empty() { "Select column" } else { &self.selected_column })
                                 .show_ui(ui, |ui| {
                                     for column in &self.available_columns {
                                         if ui.selectable_label(
                                             self.selected_column == *column,
                                             column,
                                         ).clicked() {
                                             self.selected_column = column.clone();
                                             if self.output_name.is_empty() {
                                                 self.output_name = format!("delta_{}", column);
                                             }
                                         }
                                     }
                                 });
                         });
                         
                         ui.horizontal(|ui| {
                             ui.label("Delta Name:");
                             ui.text_edit_singleline(&mut self.output_name);
                         });
                         
                         if !self.selected_column.is_empty() && !self.output_name.is_empty() {
                             ui.horizontal(|ui| {
                                 if ui.button("➕ Add Column").clicked() {
                                     add_column_clicked = true;
                                 }
                             });
                         }
                     });

                    // Apply/Cancel buttons
                    if !self.transformations.is_empty() {
                        ui.separator();
                        ui.horizontal(|ui| {
                            if ui.button("💾 Export with Delta Columns").clicked() {
                                apply_clicked = true;
                            }
                            if ui.button("Cancel").clicked() {
                                cancel_clicked = true;
                            }
                        });
                    }

                    // Error/success messages
                    if let Some(ref error) = self.error_message {
                        ui.colored_label(egui::Color32::from_rgb(255, 100, 100), error);
                    }
                    if let Some(ref success) = self.success_message {
                        ui.colored_label(egui::Color32::from_rgb(100, 255, 100), success);
                    }
                }
            });

        // Handle table selection outside the closure
        if table_selected {
            if let Some(table_name) = selected_table_name {
                self.selected_table = Some(table_name);
                self.update_available_columns(database);
                self.reset_transformation_state();
            }
        }

        // Handle adding new column
        if add_column_clicked {
            if !self.selected_column.is_empty() && !self.output_name.is_empty() {
                // Check if this column is already added
                let already_exists = self.transformations.iter().any(|t| 
                    t.selected_columns.contains(&self.selected_column)
                );
                
                if !already_exists {
                    self.transformations.push(DeltaTransformation {
                        selected_columns: vec![self.selected_column.clone()],
                        output_column_names: vec![self.output_name.clone()],
                    });
                    // Clear the form
                    self.selected_column.clear();
                    self.output_name.clear();
                } else {
                    self.error_message = Some("Column already added".to_string());
                }
            }
        }

        // Handle removing column
        if remove_column_clicked {
            if let Some((transformation_idx, column_idx)) = column_to_remove {
                if transformation_idx < self.transformations.len() {
                    let transformation = &mut self.transformations[transformation_idx];
                    if column_idx < transformation.selected_columns.len() {
                        transformation.selected_columns.remove(column_idx);
                        transformation.output_column_names.remove(column_idx);
                        
                        // Remove empty transformations
                        if transformation.selected_columns.is_empty() {
                            self.transformations.remove(transformation_idx);
                        }
                    }
                }
            }
        }

        // Handle button clicks outside the closure
        if apply_clicked {
            if let Some(table_name) = &self.selected_table {
                if !self.transformations.is_empty() {
                    let transformations = self.transformations.iter().map(|t| {
                        SingleTransformation {
                            transformation_type: TransformationType::Delta,
                            selected_columns: t.selected_columns.clone(),
                            output_column_names: t.output_column_names.clone(),
                            output_column_name: String::new(), // Not used for delta
                            bin_size: String::new(),
                            time_column: None,
                            grouping_columns: None,
                        }
                    }).collect();
                    
                    result = Some(TransformationRequest {
                        table_name: table_name.clone(),
                        transformations,
                    });
                    self.success_message = Some("Transformations applied successfully!".to_string());
                    self.visible = false;
                } else {
                    self.error_message = Some("No transformations to apply".to_string());
                }
            }
        }
        if cancel_clicked {
            self.visible = false;
            self.reset();
        }
        result
    }



    pub fn update_available_tables(&mut self, database: &Database) {
        self.available_tables = database.get_tables().unwrap_or_default();
    }

    pub fn update_available_columns(&mut self, database: &Database) {
        if let Some(table_name) = &self.selected_table {
            // Get all columns and their types
            let query = format!("SELECT * FROM {}", table_name);
            if let Ok(columns) = database.get_column_names(&query) {
                if let Ok(types) = database.get_column_types(&query) {
                    // Filter to only numeric columns for delta transformations
                    let mut numeric_columns = Vec::new();
                    for (column, data_type) in columns.iter().zip(types.iter()) {
                        match data_type {
                            DataType::Int64 | DataType::Float64 => {
                                numeric_columns.push(column.clone());
                            }
                            _ => {}
                        }
                    }
                    self.available_columns = numeric_columns;
                } else {
                    // Fallback to all columns if we can't get types
                    self.available_columns = columns;
                }
            }
        }
    }

    fn reset_transformation_state(&mut self) {
        self.transformations.clear();
        self.selected_column.clear();
        self.output_name.clear();
        self.error_message = None;
        self.success_message = None;
    }

    fn reset(&mut self) {
        self.selected_table = None;
        self.reset_transformation_state();
    }
}

#[derive(Debug)]
pub struct TransformationRequest {
    pub table_name: String,
    pub transformations: Vec<SingleTransformation>, // Multiple transformations in one request
}

#[derive(Debug)]
pub struct SingleTransformation {
    pub transformation_type: TransformationType,
    pub selected_columns: Vec<String>,
    pub output_column_names: Vec<String>, // For delta transformations with multiple columns
    pub output_column_name: String, // For single column transformations
    pub bin_size: String,
    pub time_column: Option<String>,
    pub grouping_columns: Option<Vec<String>>,
}

pub struct TransformationManager {
    pub transformer: DataTransformer,
}

impl TransformationManager {
    pub fn new() -> Self {
        Self {
            transformer: DataTransformer::new(),
        }
    }

    pub fn apply_transformation(&self, request: &TransformationRequest, database: &Database, output_dir: &std::path::Path) -> Result<String> {
        // Get the data from the database
        let query = format!("SELECT * FROM {}", request.table_name);
        let rows = database.execute_query(&query)?;
        
        if rows.is_empty() {
            return Err(anyhow!("No data found in table"));
        }

        // Get column names from the database
        let column_names = database.get_column_names(&query)?;
        
        // Convert the rows to a RecordBatch with proper column names
        let mut current_batch = self.convert_rows_to_batch(&rows, &column_names)?;
        
        // Get numeric columns for validation
        let numeric_columns = self.transformer.get_numeric_columns(&current_batch);
        
        // Apply all transformations sequentially
        for transformation in &request.transformations {
            current_batch = match transformation.transformation_type {
                TransformationType::Delta => {
                    if transformation.selected_columns.is_empty() {
                        return Err(anyhow!("Delta transformation requires at least one column"));
                    }
                    
                    // Validate that all selected columns are numeric
                    for column in &transformation.selected_columns {
                        if !numeric_columns.contains(column) {
                            return Err(anyhow!("Column '{}' is not numeric. Delta transformations only work on numeric columns (Int64, Float64). Available numeric columns: {}", 
                                column, numeric_columns.join(", ")));
                        }
                    }
                    
                    if transformation.selected_columns.len() == 1 {
                        // Single column delta
                        let output_name = if !transformation.output_column_names.is_empty() {
                            &transformation.output_column_names[0]
                        } else {
                            &transformation.output_column_name
                        };
                        self.transformer.apply_delta(&current_batch, &transformation.selected_columns[0], output_name)?
                    } else {
                        // Multiple column delta with custom names
                        if transformation.output_column_names.len() != transformation.selected_columns.len() {
                            return Err(anyhow!("Number of output column names must match number of selected columns"));
                        }
                        self.transformer.apply_delta_multiple_custom(&current_batch, &transformation.selected_columns, &transformation.output_column_names)?
                    }
                }
                TransformationType::TimeBin => {
                    let time_column = transformation.time_column.as_ref()
                        .ok_or_else(|| anyhow!("Time column is required for time binning"))?;
                    let bin_size: f64 = transformation.bin_size.parse()
                        .map_err(|_| anyhow!("Invalid bin size"))?;
                    self.transformer.apply_time_bin(&current_batch, time_column, bin_size, &transformation.output_column_name)?
                }
                TransformationType::RowId => {
                    self.transformer.apply_row_id(&current_batch, &transformation.output_column_name, transformation.grouping_columns.as_deref())?
                }
                TransformationType::CumulativeSum => {
                    // Not implemented in this dialog - handled by ComputedColumnsDialog
                    return Err(anyhow!("Cumulative sum should be handled by Computed Columns dialog"));
                }
                TransformationType::Percentage => {
                    // Not implemented in this dialog - handled by ComputedColumnsDialog
                    return Err(anyhow!("Percentage should be handled by Computed Columns dialog"));
                }
                TransformationType::Ratio => {
                    // Not implemented in this dialog - handled by ComputedColumnsDialog
                    return Err(anyhow!("Ratio should be handled by Computed Columns dialog"));
                }
                TransformationType::MovingAverage => {
                    // Not implemented in this dialog - handled by ComputedColumnsDialog
                    return Err(anyhow!("Moving average should be handled by Computed Columns dialog"));
                }
                TransformationType::ZScore => {
                    // Not implemented in this dialog - handled by ComputedColumnsDialog
                    return Err(anyhow!("Z-score should be handled by Computed Columns dialog"));
                }
            };
        }

        // Save the transformed data with a generic name since we have multiple transformations
        let output_filename = format!("{}_with_deltas.arrow", request.table_name);
        let output_path = output_dir.join(output_filename);
        self.transformer.save_transformed_data(&current_batch, &output_path)?;

        Ok(output_path.to_string_lossy().to_string())
    }

    fn convert_rows_to_batch(&self, rows: &Vec<Vec<String>>, column_names: &[String]) -> Result<RecordBatch> {
        let num_rows = rows.len();
        let num_cols = if num_rows > 0 { rows[0].len() } else { 0 };
        
        if num_rows == 0 || num_cols == 0 {
            return Err(anyhow!("No data to convert"));
        }
        
        if num_cols != column_names.len() {
            return Err(anyhow!("Column count mismatch: {} columns in data, {} column names provided", num_cols, column_names.len()));
        }
        
        // Create arrays for each column with proper data type detection
        let mut arrays: Vec<ArrayRef> = Vec::new();
        let mut fields: Vec<Arc<Field>> = Vec::new();
        
        for (col_idx, column_name) in column_names.iter().enumerate() {
            let mut column_data: Vec<String> = Vec::new();
            for row in rows {
                if col_idx < row.len() {
                    column_data.push(row[col_idx].clone());
                } else {
                    column_data.push("".to_string());
                }
            }
            
            // Try to detect the data type and convert appropriately
            let (array, data_type) = self.detect_and_convert_column(&column_data)?;
            arrays.push(array);
            fields.push(Arc::new(Field::new(column_name, data_type, true)));
        }
        
        let schema = Arc::new(Schema::new(fields));
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
    
    fn detect_and_convert_column(&self, column_data: &[String]) -> Result<(ArrayRef, DataType)> {
        // Try to parse as integers first
        let mut int_values: Vec<Option<i64>> = Vec::new();
        let mut all_integers = true;
        
        for value in column_data {
            let trimmed = value.trim();
            if trimmed.is_empty() || trimmed.to_uppercase() == "NULL" {
                int_values.push(None);
            } else {
                match trimmed.parse::<i64>() {
                    Ok(int_val) => int_values.push(Some(int_val)),
                    Err(_) => {
                        all_integers = false;
                        break;
                    }
                }
            }
        }
        
        if all_integers {
            return Ok((Arc::new(Int64Array::from(int_values)), DataType::Int64));
        }
        
        // Try to parse as floats
        let mut float_values: Vec<Option<f64>> = Vec::new();
        let mut all_floats = true;
        
        for value in column_data {
            let trimmed = value.trim();
            if trimmed.is_empty() || trimmed.to_uppercase() == "NULL" {
                float_values.push(None);
            } else {
                match trimmed.parse::<f64>() {
                    Ok(float_val) => float_values.push(Some(float_val)),
                    Err(_) => {
                        all_floats = false;
                        break;
                    }
                }
            }
        }
        
        if all_floats {
            return Ok((Arc::new(Float64Array::from(float_values)), DataType::Float64));
        }
        
        // Default to string if not numeric
        let string_array = StringArray::from(column_data.to_vec());
        Ok((Arc::new(string_array), DataType::Utf8))
    }
} 

// Refactored config methods to use local variables
fn show_delta_config_with_data(ui: &mut egui::Ui, available_columns: &[String], selected_columns: &mut Vec<String>) {
    ui.label("Select Columns:");
    ui.label("Choose one or more columns to compute deltas for");
    
    egui::ScrollArea::vertical()
        .max_height(150.0)
        .show(ui, |ui| {
            for column in available_columns {
                let mut is_selected = selected_columns.contains(column);
                if ui.checkbox(&mut is_selected, column).clicked() {
                    if is_selected {
                        if !selected_columns.contains(column) {
                            selected_columns.push(column.clone());
                        }
                    } else {
                        selected_columns.retain(|c| c != column);
                    }
                }
            }
        });
}

fn show_time_bin_config_with_data(ui: &mut egui::Ui, available_columns: &[String], time_column: &mut Option<String>, bin_size: &mut String) {
    ui.label("Time Column:");
    egui::ComboBox::from_id_source("time_column_select")
        .selected_text(time_column.as_deref().unwrap_or("Select time column"))
        .show_ui(ui, |ui| {
            for column in available_columns {
                if ui.selectable_label(
                    time_column.as_deref() == Some(column),
                    column,
                ).clicked() {
                    *time_column = Some(column.clone());
                }
            }
        });
    ui.label("Bin Size (seconds):");
    ui.text_edit_singleline(bin_size);
}
fn show_row_id_config_with_data(ui: &mut egui::Ui, available_columns: &[String], grouping_columns: &mut Vec<String>) {
    ui.label("Grouping Columns (Optional):");
    ui.label("Leave empty for global row IDs only");
    egui::ScrollArea::vertical()
        .max_height(150.0)
        .show(ui, |ui| {
            for column in available_columns {
                let mut is_selected = grouping_columns.contains(column);
                if ui.checkbox(&mut is_selected, column).clicked() {
                    if is_selected {
                        if !grouping_columns.contains(column) {
                            grouping_columns.push(column.clone());
                        }
                    } else {
                        grouping_columns.retain(|c| c != column);
                    }
                }
            }
        });
} 