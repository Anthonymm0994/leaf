use egui;
use datafusion::arrow::datatypes::DataType;
use crate::core::{Database, TableInfo, TransformationType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum ComputationType {
    Delta,
    CumulativeSum,
    Percentage,
    Ratio,
    MovingAverage,
    ZScore,
}

impl ComputationType {
    fn all() -> Vec<Self> {
        vec![
            Self::Delta,
            Self::CumulativeSum,
            Self::Percentage,
            Self::Ratio,
            Self::MovingAverage,
            Self::ZScore,
        ]
    }
    
    fn display_name(&self) -> &'static str {
        match self {
            Self::Delta => "Delta (Row-to-Row Difference)",
            Self::CumulativeSum => "Cumulative Sum",
            Self::Percentage => "Percentage of Total",
            Self::Ratio => "Ratio (Column A / Column B)",
            Self::MovingAverage => "Moving Average",
            Self::ZScore => "Z-Score Normalization",
        }
    }
    
    fn description(&self) -> &'static str {
        match self {
            Self::Delta => "Shows the change from one row to the next (e.g., daily temperature change)",
            Self::CumulativeSum => "Running total that adds up as you go down (e.g., total sales to date)",
            Self::Percentage => "What percent each value is of the total (e.g., market share)",
            Self::Ratio => "Divide one column by another (e.g., revenue per employee)",
            Self::MovingAverage => "Smooth out variations by averaging nearby values",
            Self::ZScore => "Show how many standard deviations from average (for outlier detection)",
        }
    }
    
    fn requires_second_column(&self) -> bool {
        matches!(self, Self::Ratio)
    }
    
    fn supports_window_size(&self) -> bool {
        matches!(self, Self::MovingAverage)
    }
}

#[derive(Debug, Clone)]
pub struct ComputedColumnConfig {
    pub computation_type: ComputationType,
    pub source_column: String,
    pub second_column: Option<String>,
    pub output_name: String,
    pub window_size: usize,
    pub null_handling: NullHandling,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NullHandling {
    SkipNulls,
    PropagateNulls,
    FillWithZero,
}

#[derive(Debug, Clone)]
pub struct ComputedColumnsDialog {
    pub visible: bool,
    pub selected_table: Option<String>,
    pub available_tables: Vec<TableInfo>,
    pub available_columns: Vec<String>,
    pub numeric_columns: Vec<String>,
    
    // Current configuration
    pub computation_type: ComputationType,
    pub source_column: String,
    pub second_column: String,
    pub output_name: String,
    pub window_size: String,
    pub null_handling: NullHandling,
    
    // Configurations to apply
    pub configurations: Vec<ComputedColumnConfig>,
    
    // UI state
    pub error_message: Option<String>,
    pub success_message: Option<String>,
    pub show_preview: bool,
    pub preview_data: Option<PreviewData>,
    pub output_filename: String,
}

#[derive(Debug, Clone)]
pub struct PreviewData {
    pub rows: Vec<PreviewRow>,
}

#[derive(Debug, Clone)]
pub struct PreviewRow {
    pub row_num: usize,
    pub source_value: String,
    pub second_value: Option<String>,
    pub result_value: String,
}

impl Default for ComputedColumnsDialog {
    fn default() -> Self {
        Self {
            visible: false,
            selected_table: None,
            available_tables: Vec::new(),
            available_columns: Vec::new(),
            numeric_columns: Vec::new(),
            computation_type: ComputationType::Delta,
            source_column: String::new(),
            second_column: String::new(),
            output_name: String::new(),
            window_size: "5".to_string(),
            null_handling: NullHandling::SkipNulls,
            configurations: Vec::new(),
            error_message: None,
            success_message: None,
            show_preview: false,
            preview_data: None,
            output_filename: String::new(),
        }
    }
}

impl ComputedColumnsDialog {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn show(&mut self, ctx: &egui::Context, database: &Database) -> Option<ComputedColumnsRequest> {
        if !self.visible {
            return None;
        }
        
        let mut result = None;
        let mut should_update_columns = false;
        let mut should_add_config = false;
        let mut should_apply = false;
        let mut should_cancel = false;
        let mut should_preview = false;
        let mut config_to_remove = None;
        let mut should_update_output_name = false;
        let mut new_computation_type = None;
        let mut new_source_column = None;
        let mut new_selected_table = None;
        
        egui::Window::new("Add Computed Column")
            .open(&mut self.visible)
            .resizable(true)
            .default_size([600.0, 700.0])
            .show(ctx, |ui| {
                ui.heading("Add Computed Column");
                ui.separator();
                
                // Computation Type Selection
                ui.horizontal(|ui| {
                    ui.label("Computation Type:");
                    egui::ComboBox::from_label("")
                        .selected_text(self.computation_type.display_name())
                        .show_ui(ui, |ui| {
                            for comp_type in ComputationType::all() {
                                if ui.selectable_label(
                                    self.computation_type == comp_type,
                                    comp_type.display_name()
                                ).clicked() {
                                    new_computation_type = Some(comp_type.clone());
                                    should_update_output_name = true;
                                }
                            }
                        });
                });
                
                ui.label(self.computation_type.description());
                ui.separator();
                
                // Table Selection
                ui.horizontal(|ui| {
                    ui.label("Table:");
                    egui::ComboBox::from_label("table_select")
                        .selected_text(self.selected_table.as_deref().unwrap_or("Select a table"))
                        .show_ui(ui, |ui| {
                            for table in &self.available_tables {
                                if ui.selectable_label(
                                    self.selected_table.as_deref() == Some(&table.name),
                                    &table.name
                                ).clicked() {
                                    new_selected_table = Some(table.name.clone());
                                    should_update_columns = true;
                                }
                            }
                        });
                });
                
                if self.selected_table.is_some() {
                    ui.separator();
                    
                    // Configuration Section
                    ui.group(|ui| {
                        ui.heading("Configuration");
                        
                        // Source Column
                        ui.horizontal(|ui| {
                            ui.label("Source Column:");
                            egui::ComboBox::from_label("source_col")
                                .selected_text(&self.source_column)
                                .show_ui(ui, |ui| {
                                    for col in &self.numeric_columns {
                                        if ui.selectable_label(
                                            &self.source_column == col,
                                            col
                                        ).clicked() {
                                            new_source_column = Some(col.clone());
                                            should_update_output_name = true;
                                        }
                                    }
                                });
                        });
                        
                        // Second Column (for ratio)
                        if self.computation_type.requires_second_column() {
                            ui.horizontal(|ui| {
                                ui.label("Divide by:");
                                egui::ComboBox::from_label("second_col")
                                    .selected_text(&self.second_column)
                                    .show_ui(ui, |ui| {
                                        for col in &self.numeric_columns {
                                            if col != &self.source_column {
                                                ui.selectable_value(
                                                    &mut self.second_column, 
                                                    col.clone(), 
                                                    col
                                                );
                                            }
                                        }
                                    });
                            });
                        }
                        
                        // Window Size (for moving average)
                        if self.computation_type.supports_window_size() {
                            ui.horizontal(|ui| {
                                ui.label("Window Size:");
                                ui.add(egui::TextEdit::singleline(&mut self.window_size)
                                    .desired_width(60.0));
                                ui.label("rows");
                            });
                        }
                        
                        // Output Column Name
                        ui.horizontal(|ui| {
                            ui.label("Output Column Name:");
                            ui.text_edit_singleline(&mut self.output_name);
                            
                            if ui.button("Auto").clicked() && !self.source_column.is_empty() {
                                self.output_name = match &self.computation_type {
                                    ComputationType::Delta => format!("{}_change", self.source_column),
                                    ComputationType::CumulativeSum => format!("{}_total", self.source_column),
                                    ComputationType::Percentage => format!("{}_percent", self.source_column),
                                    ComputationType::Ratio => {
                                        if !self.second_column.is_empty() {
                                            format!("{}_per_{}", self.source_column, self.second_column)
                                        } else {
                                            format!("{}_ratio", self.source_column)
                                        }
                                    },
                                    ComputationType::MovingAverage => format!("{}_ma{}", self.source_column, self.window_size),
                                    ComputationType::ZScore => format!("{}_zscore", self.source_column),
                                };
                            }
                        });
                        
                        // Null Handling
                        ui.horizontal(|ui| {
                            ui.label("Empty values:");
                            ui.radio_value(&mut self.null_handling, NullHandling::SkipNulls, "Ignore")
                                .on_hover_text("Skip empty values in calculations");
                            ui.radio_value(&mut self.null_handling, NullHandling::PropagateNulls, "Keep empty")
                                .on_hover_text("Result is empty if input is empty");
                            ui.radio_value(&mut self.null_handling, NullHandling::FillWithZero, "Use zero")
                                .on_hover_text("Treat empty values as 0");
                        });
                        
                        ui.separator();
                        
                        // Action buttons
                        ui.horizontal(|ui| {
                            if ui.button("Preview").clicked() {
                                should_preview = true;
                            }
                            
                            if ui.button("Add to List").clicked() {
                                should_add_config = true;
                            }
                        });
                    });
                    
                    // Preview Section
                    if self.show_preview {
                        ui.separator();
                        ui.heading("Preview");
                        
                        if let Some(preview) = &self.preview_data {
                            egui::ScrollArea::vertical()
                                .max_height(200.0)
                                .show(ui, |ui| {
                                    egui::Grid::new("preview_grid")
                                        .striped(true)
                                        .show(ui, |ui| {
                                            // Header
                                            ui.label("Row");
                                            ui.label(&self.source_column);
                                            if self.computation_type.requires_second_column() {
                                                ui.label(&self.second_column);
                                            }
                                            ui.label(&self.output_name);
                                            ui.end_row();
                                            
                                            // Data rows
                                            for row in &preview.rows {
                                                ui.label(row.row_num.to_string());
                                                ui.label(&row.source_value);
                                                if let Some(second) = &row.second_value {
                                                    ui.label(second);
                                                }
                                                ui.label(&row.result_value);
                                                ui.end_row();
                                            }
                                        });
                                });
                        }
                    }
                    
                    // Configured Columns List
                    if !self.configurations.is_empty() {
                        ui.separator();
                        ui.heading("Columns to Add");
                        
                        egui::ScrollArea::vertical()
                            .max_height(150.0)
                            .show(ui, |ui| {
                                for (idx, config) in self.configurations.iter().enumerate() {
                                    ui.horizontal(|ui| {
                                        ui.label("✓");
                                        ui.label(format!("{} of '{}' → {}", 
                                            config.computation_type.display_name(),
                                            config.source_column,
                                            config.output_name
                                        ));
                                        if ui.small_button("Remove").clicked() {
                                            config_to_remove = Some(idx);
                                        }
                                    });
                                }
                            });
                        
                        // Output filename
                        ui.separator();
                        ui.horizontal(|ui| {
                            ui.label("Output filename (optional):");
                            ui.text_edit_singleline(&mut self.output_filename)
                                .on_hover_text("Leave empty to auto-generate filename");
                            if !self.output_filename.is_empty() && !self.output_filename.ends_with(".arrow") {
                                ui.label(egui::RichText::new("(.arrow will be added)").weak());
                            }
                        });
                    }
                }
                
                // Error/Success Messages
                if let Some(error) = &self.error_message {
                    ui.colored_label(egui::Color32::RED, error);
                }
                if let Some(success) = &self.success_message {
                    ui.colored_label(egui::Color32::GREEN, success);
                }
                
                ui.separator();
                
                // Bottom buttons
                ui.horizontal(|ui| {
                    if ui.button("Apply").clicked() && !self.configurations.is_empty() {
                        should_apply = true;
                    }
                    if ui.button("Cancel").clicked() {
                        should_cancel = true;
                    }
                });
            });
        
        // Handle actions outside the closure
        if let Some(table) = new_selected_table {
            self.selected_table = Some(table);
        }
        
        if let Some(comp_type) = new_computation_type {
            self.computation_type = comp_type;
        }
        
        if let Some(source) = new_source_column {
            self.source_column = source;
        }
        
        if should_update_output_name {
            self.update_output_name();
        }
        
        if should_update_columns {
            self.update_available_columns(database);
        }
        
        if should_add_config {
            if self.validate_current_config() {
                self.configurations.push(ComputedColumnConfig {
                    computation_type: self.computation_type.clone(),
                    source_column: self.source_column.clone(),
                    second_column: if self.computation_type.requires_second_column() {
                        Some(self.second_column.clone())
                    } else {
                        None
                    },
                    output_name: self.output_name.clone(),
                    window_size: self.window_size.parse().unwrap_or(5),
                    null_handling: self.null_handling.clone(),
                });
                self.clear_current_config();
                self.success_message = Some("Column added to list".to_string());
            }
        }
        
        if should_preview {
            self.generate_preview(database);
        }
        
        if let Some(idx) = config_to_remove {
            self.configurations.remove(idx);
        }
        
        if should_apply {
            if let Some(table_name) = &self.selected_table {
                result = Some(ComputedColumnsRequest {
                    table_name: table_name.clone(),
                    configurations: self.configurations.clone(),
                    output_filename: if self.output_filename.is_empty() {
                        None
                    } else {
                        Some(self.output_filename.clone())
                    },
                });
                self.visible = false;
            }
        }
        
        if should_cancel {
            self.visible = false;
            self.reset();
        }
        
        result
    }
    
    fn update_available_columns(&mut self, database: &Database) {
        if let Some(table_name) = &self.selected_table {
            let query = format!("SELECT * FROM {} LIMIT 1", table_name);
            if let Ok(columns) = database.get_column_names(&query) {
                if let Ok(types) = database.get_column_types(&query) {
                    self.available_columns = columns.clone();
                    self.numeric_columns = columns.into_iter()
                        .zip(types.into_iter())
                        .filter_map(|(col, dtype)| {
                            match dtype {
                                DataType::Int64 | DataType::Float64 => Some(col),
                                _ => None,
                            }
                        })
                        .collect();
                }
            }
        }
    }
    
    fn update_output_name(&mut self) {
        if !self.source_column.is_empty() {
            self.output_name = match self.computation_type {
                ComputationType::Delta => format!("{}_delta", self.source_column),
                ComputationType::CumulativeSum => format!("{}_cumsum", self.source_column),
                ComputationType::Percentage => format!("{}_pct", self.source_column),
                ComputationType::Ratio => format!("{}_ratio", self.source_column),
                ComputationType::MovingAverage => format!("{}_ma", self.source_column),
                ComputationType::ZScore => format!("{}_zscore", self.source_column),
            };
        }
    }
    
    fn validate_current_config(&mut self) -> bool {
        self.error_message = None;
        
        if self.source_column.is_empty() {
            self.error_message = Some("Please select a source column".to_string());
            return false;
        }
        
        if self.computation_type.requires_second_column() && self.second_column.is_empty() {
            self.error_message = Some("Please select a second column for ratio".to_string());
            return false;
        }
        
        if self.output_name.is_empty() {
            self.error_message = Some("Please provide an output column name".to_string());
            return false;
        }
        
        // Check for duplicate output names
        if self.configurations.iter().any(|c| c.output_name == self.output_name) {
            self.error_message = Some("Output column name already exists".to_string());
            return false;
        }
        
        if self.computation_type.supports_window_size() {
            if let Err(_) = self.window_size.parse::<usize>() {
                self.error_message = Some("Invalid window size".to_string());
                return false;
            }
        }
        
        true
    }
    
    fn clear_current_config(&mut self) {
        self.source_column.clear();
        self.second_column.clear();
        self.output_name.clear();
        self.error_message = None;
    }
    
    fn generate_preview(&mut self, database: &Database) {
        // This would generate preview data
        // For now, just show the preview section
        self.show_preview = true;
        
        // Mock preview data
        self.preview_data = Some(PreviewData {
            rows: vec![
                PreviewRow { row_num: 1, source_value: "63.78".to_string(), second_value: None, result_value: "NULL".to_string() },
                PreviewRow { row_num: 2, source_value: "116.97".to_string(), second_value: None, result_value: "53.19".to_string() },
                PreviewRow { row_num: 3, source_value: "194.03".to_string(), second_value: None, result_value: "77.06".to_string() },
            ],
        });
    }
    
    pub fn update_available_tables(&mut self, database: &Database) {
        self.available_tables = database.get_tables().unwrap_or_default();
    }
    
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug, Clone)]
pub struct ComputedColumnsRequest {
    pub table_name: String,
    pub configurations: Vec<ComputedColumnConfig>,
    pub output_filename: Option<String>,
}