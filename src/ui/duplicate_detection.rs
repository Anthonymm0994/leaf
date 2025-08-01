use egui::{Ui, Window, Button, Checkbox, ComboBox, TextEdit, Label, ScrollArea, CollapsingHeader, Grid, Response, Color32};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use crate::core::{Database, DuplicateDetector, DuplicateDetectionConfig, DuplicateDetectionResult, duplicate_detector::DuplicateGroup};

/// UI state for duplicate detection dialog
pub struct DuplicateDetectionDialog {
    /// Whether the dialog is visible
    pub visible: bool,
    /// Selected table name
    pub selected_table: String,
    /// Available tables in the database
    pub available_tables: Vec<String>,
    /// Selected group column
    pub selected_group_column: String,
    /// Available columns in the current table
    pub available_columns: Vec<String>,
    /// Columns to ignore during comparison
    pub ignore_columns: HashSet<String>,
    /// Whether to treat null values as equal
    pub null_equals_null: bool,
    /// Current detection result
    pub detection_result: Option<DuplicateDetectionResult>,
    /// Whether detection is in progress
    pub is_detecting: bool,
    /// Error message if detection failed
    pub error_message: Option<String>,
    /// Output directory for clean Arrow files
    pub output_directory: PathBuf,
    /// Success message after creating clean file
    pub success_message: Option<String>,
    /// Whether to show success message
    pub show_success: bool,
    /// Database reference for updating columns
    pub db: Option<Arc<Database>>,
}

impl Default for DuplicateDetectionDialog {
    fn default() -> Self {
        Self {
            visible: false,
            selected_table: String::new(),
            available_tables: Vec::new(),
            selected_group_column: String::new(),
            available_columns: Vec::new(),
            ignore_columns: HashSet::new(),
            null_equals_null: true,
            detection_result: None,
            is_detecting: false,
            error_message: None,
            output_directory: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            success_message: None,
            show_success: false,
            db: None,
        }
    }
}

impl DuplicateDetectionDialog {
    /// Show the duplicate detection dialog
    pub fn show(&mut self, ctx: &egui::Context, db: &Arc<Database>) {
        if !self.visible {
            return;
        }

        // Store database reference for column updates
        self.db = Some(db.clone());

        let mut visible = self.visible;
        Window::new("Detect Duplicate Groups")
            .open(&mut visible)
            .resizable(true)
            .default_size([700.0, 600.0])
            .min_size([500.0, 400.0])
            .show(ctx, |ui| {
                self.render_dialog(ui, db);
            });
        self.visible = visible;
    }

    /// Render the main dialog content
    fn render_dialog(&mut self, ui: &mut Ui, db: &Arc<Database>) {
        // Configuration section
        ui.heading("Configuration");
        ui.separator();

        // Table selection
        ui.horizontal(|ui| {
            ui.label("Table:");
            let mut table_changed = false;
            ComboBox::from_id_source("table_selection")
                .selected_text(&self.selected_table)
                .show_ui(ui, |ui| {
                    for table in &self.available_tables {
                        if ui.selectable_value(&mut self.selected_table, table.clone(), table).clicked() {
                            table_changed = true;
                        }
                    }
                });
            
            // Update columns when table changes
            if table_changed {
                self.update_columns_for_selected_table(db);
                // Clear group column selection when table changes
                self.selected_group_column.clear();
                // Clear ignore columns when table changes
                self.ignore_columns.clear();
            }
        });

        // Group column selection
        ui.horizontal(|ui| {
            ui.label("Group by column:");
            ComboBox::from_id_source("group_column")
                .selected_text(&self.selected_group_column)
                .show_ui(ui, |ui| {
                    for column in &self.available_columns {
                        ui.selectable_value(&mut self.selected_group_column, column.clone(), column);
                    }
                });
        });

        ui.separator();

        // Column ignore selection with improved layout
        ui.heading("Columns to Ignore");
        ui.label("Select columns to exclude from comparison (e.g., timestamps, IDs):");

        // Use a more flexible layout for many columns
        ScrollArea::vertical()
            .max_height(200.0)
            .show(ui, |ui| {
                // Use a responsive grid layout
                let available_width = ui.available_width();
                let min_checkbox_width = 180.0;
                let columns_per_row = (available_width / min_checkbox_width).max(1.0) as usize;
                
                // Group columns into rows
                let mut columns_to_show: Vec<&String> = self.available_columns
                    .iter()
                    .filter(|col| col != &&self.selected_group_column)
                    .collect();
                
                for chunk in columns_to_show.chunks(columns_per_row) {
                    ui.horizontal(|ui| {
                        for column in chunk {
                            let mut is_ignored = self.ignore_columns.contains(*column);
                            if ui.checkbox(&mut is_ignored, *column).clicked() {
                                if is_ignored {
                                    self.ignore_columns.insert((*column).clone());
                                } else {
                                    self.ignore_columns.remove(*column);
                                }
                            }
                        }
                    });
                }
            });

        ui.separator();

        // Action buttons
        ui.horizontal(|ui| {
            if ui.button("Detect Duplicates").clicked() {
                self.run_detection(db);
            }

            if let Some(_result) = &self.detection_result {
                if ui.button("Export Clean Arrow File").clicked() {
                    self.export_clean_arrow_file(db);
                }
            }
        });

        // Status and error messages
        if self.is_detecting {
            ui.label("🔍 Detecting duplicates...");
        }

        if let Some(error) = &self.error_message {
            ui.colored_label(egui::Color32::RED, format!("Error: {}", error));
        }

        // Results section
        if let Some(result) = &self.detection_result {
            ui.separator();
            ui.heading("Detection Results");
            
            ui.label(format!("Total duplicate groups: {}", result.total_duplicates));
            ui.label(format!("Total duplicate rows: {}", result.total_duplicate_rows));
            ui.label(format!("Groups processed: {}", result.stats.groups_processed));
            ui.label(format!("Groups analyzed: {}", result.stats.groups_analyzed));
            ui.label(format!("Unique groups found: {}", result.stats.unique_groups));

            if result.total_duplicates > 0 {
                ui.separator();
                ui.heading("Export Options");
                
                ui.label("💾 Click 'Export Clean Arrow File' to save with first occurrence kept, subsequent duplicates removed");
            }
        }

        // Success message
        if self.show_success {
            if let Some(success_msg) = &self.success_message {
                ui.separator();
                ui.colored_label(Color32::GREEN, success_msg);
                if ui.button("OK").clicked() {
                    self.show_success = false;
                    self.success_message = None;
                }
            }
        }
    }

    /// Update columns for the currently selected table
    fn update_columns_for_selected_table(&mut self, db: &Arc<Database>) {
        let tables = db.get_tables().unwrap_or_default();
        
        if let Some(table) = tables.iter().find(|t| t.name == self.selected_table) {
            self.available_columns = table.columns.iter()
                .map(|col| col.name.clone())
                .collect();
        } else {
            self.available_columns.clear();
        }
    }

    /// Run the duplicate detection
    fn run_detection(&mut self, db: &Arc<Database>) {
        if self.selected_table.is_empty() {
            self.error_message = Some("Please select a table".to_string());
            return;
        }

        if self.selected_group_column.is_empty() {
            self.error_message = Some("Please select a group column".to_string());
            return;
        }

        self.is_detecting = true;
        self.error_message = None;

        // Load the table data using the non-mutable version
        match db.get_table_arrow_batch(&self.selected_table) {
            Ok(batch) => {
                // Create detector configuration
                let config = DuplicateDetectionConfig {
                    group_column: self.selected_group_column.clone(),
                    ignore_columns: self.ignore_columns.clone(),
                    null_equals_null: self.null_equals_null,
                };

                let detector = DuplicateDetector::new(config);
                
                // Run detection
                match detector.detect_duplicates(&batch) {
                    Ok(result) => {
                        self.detection_result = Some(result);
                        self.error_message = None;
                    }
                    Err(e) => {
                        self.error_message = Some(format!("Detection failed: {}", e));
                    }
                }
            }
            Err(e) => {
                self.error_message = Some(format!("Failed to load table: {}", e));
            }
        }

        self.is_detecting = false;
    }

    /// Export clean Arrow file
    fn export_clean_arrow_file(&mut self, db: &Arc<Database>) {
        if self.selected_table.is_empty() {
            self.error_message = Some("No table selected".to_string());
            return;
        }

        let result = match &self.detection_result {
            Some(result) => result,
            None => {
                self.error_message = Some("No detection result available".to_string());
                return;
            }
        };
        
        // Create detector configuration
        let config = DuplicateDetectionConfig {
            group_column: self.selected_group_column.clone(),
            ignore_columns: self.ignore_columns.clone(),
            null_equals_null: self.null_equals_null,
        };

        let detector = DuplicateDetector::new(config);
        
        // Load the table as Arrow batch using the non-mutable version
        match db.get_table_arrow_batch(&self.selected_table) {
            Ok(batch) => {
                // Open file dialog to select export location
                if let Some(output_path) = rfd::FileDialog::new()
                    .set_title("Save Clean Arrow File")
                    .set_directory(&self.output_directory)
                    .add_filter("Arrow Files", &["arrow"])
                    .save_file() {
                    
                    // Create clean Arrow file
                    match detector.create_clean_arrow_file(
                        &batch,
                        result,
                        &output_path,
                    ) {
                        Ok(kept_rows) => {
                            self.success_message = Some(format!(
                                "✅ Created clean Arrow file: {}\nKept {} rows (first occurrence of each group), removed {} duplicate rows",
                                output_path.display(),
                                kept_rows,
                                batch.num_rows() - kept_rows
                            ));
                            self.show_success = true;
                        }
                        Err(e) => {
                            self.error_message = Some(format!("Failed to create clean Arrow file: {}", e));
                        }
                    }
                }
            }
            Err(e) => {
                self.error_message = Some(format!("Failed to load table: {}", e));
            }
        }
    }

    /// Update available tables and columns from the database
    pub fn update_available_tables_and_columns(&mut self, db: &Arc<Database>) {
        let tables = db.get_tables().unwrap_or_default();
        
        // Update available tables
        self.available_tables = tables.iter()
            .map(|table| table.name.clone())
            .collect();
        
        // If no table is selected and we have tables, select the first one
        if self.selected_table.is_empty() && !self.available_tables.is_empty() {
            self.selected_table = self.available_tables[0].clone();
        }
        
        // Update available columns for the selected table
        if let Some(table) = tables.iter().find(|t| t.name == self.selected_table) {
            self.available_columns = table.columns.iter()
                .map(|col| col.name.clone())
                .collect();
        } else {
            self.available_columns.clear();
        }
    }
}

/// Results viewer for duplicate groups
pub struct DuplicateResultsViewer {
    pub visible: bool,
    pub result: Option<DuplicateDetectionResult>,
}

impl Default for DuplicateResultsViewer {
    fn default() -> Self {
        Self {
            visible: false,
            result: None,
        }
    }
}

impl DuplicateResultsViewer {
    /// Show the results viewer
    pub fn show(&mut self, ctx: &egui::Context, db: &Arc<Database>) {
        if !self.visible || self.result.is_none() {
            return;
        }

        let result = self.result.as_ref().unwrap().clone();
        let mut visible = self.visible;

        Window::new("Duplicate Group Results")
            .open(&mut visible)
            .resizable(true)
            .default_size([800.0, 600.0])
            .min_size([400.0, 300.0])
            .show(ctx, |ui| {
                self.render_results(ui, &result, db);
            });
        
        self.visible = visible;
    }

    /// Render the results view
    fn render_results(&mut self, ui: &mut Ui, result: &DuplicateDetectionResult, db: &Arc<Database>) {
        ui.heading(format!("Found {} Duplicate Groups", result.total_duplicates));
        ui.separator();

        ScrollArea::vertical().show(ui, |ui| {
            for (group_idx, group) in result.duplicate_groups.iter().enumerate() {
                CollapsingHeader::new(format!(
                    "Group {} (ID: {}, {} occurrences)",
                    group_idx + 1,
                    group.group_id,
                    group.row_indices.len()
                ))
                .default_open(false)
                .show(ui, |ui| {
                    self.render_group_details(ui, group, db);
                });
            }
        });
    }

    /// Render details for a specific group
    fn render_group_details(&mut self, ui: &mut Ui, group: &DuplicateGroup, db: &Arc<Database>) {
        ui.label(format!("Group Hash: {:x}", group.group_hash));
        ui.label(format!("Group ID: {}", group.group_id));
        ui.label(format!("Group Size: {}", group.group_size));
        ui.label(format!("Occurrences: {}", group.row_indices.len()));

        // Show row indices for each occurrence
        for (occurrence_idx, row_indices) in group.row_indices.iter().enumerate() {
            CollapsingHeader::new(format!("Occurrence {}", occurrence_idx + 1))
                .show(ui, |ui| {
                    ui.label(format!("Row indices: {:?}", row_indices));
                    
                    // Show sample data for this occurrence
                    let tables = db.get_tables().unwrap_or_default();
                    if let Some(table) = tables.first() {
                        ui.label(format!("Table: {}", table.name));
                    }
                });
        }
    }
} 