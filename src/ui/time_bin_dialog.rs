use egui::{self, RichText, Color32};
use std::sync::Arc;
use crate::core::database::Database;
use crate::core::error::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum TimeBinStrategy {
    FixedInterval {
        interval_seconds: u64,
        interval_format: String,
    },
    ManualIntervals {
        intervals: Vec<String>,
        interval_string: String,
    },
    ThresholdBased {
        threshold_seconds: u64,
        threshold_format: String,
    },
}

#[derive(Debug, Clone)]
pub struct TimeBinConfig {
    pub selected_table: String,
    pub selected_column: String,
    pub strategy: TimeBinStrategy,
    pub output_column_name: String,
    pub output_filename: Option<String>,
}

pub struct TimeBinDialog {
    pub visible: bool,
    pub available_tables: Vec<String>,
    pub available_columns: Vec<String>,
    pub selected_table: String,
    pub selected_column: String,
    pub strategy: TimeBinStrategy,
    pub output_column_name: String,
    pub output_filename: String,
    pub error_message: Option<String>,
    last_updated_table: Option<String>,
    pub success_message: Option<String>,
    pub pending_apply: bool,
    pub preview_data: Option<String>,
    pub preview_info: Option<TimeBinPreview>,
}

#[derive(Debug, Clone)]
pub struct TimeBinPreview {
    pub total_rows: usize,
    pub bin_count: usize,
    pub min_bin_size: usize,
    pub max_bin_size: usize,
    pub avg_bin_size: f64,
    pub sample_bins: Vec<(String, usize)>, // (bin_label, count)
}

impl Default for TimeBinDialog {
    fn default() -> Self {
        Self {
            visible: false,
            available_tables: Vec::new(),
            available_columns: Vec::new(),
            selected_table: String::new(),
            selected_column: String::new(),
            strategy: TimeBinStrategy::FixedInterval {
                interval_seconds: 10,
                interval_format: "10".to_string(),
            },
            output_column_name: String::new(),
            output_filename: String::new(),
            error_message: None,
            last_updated_table: None,
            success_message: None,
            pending_apply: false,
            preview_data: None,
            preview_info: None,
        }
    }
}

impl TimeBinDialog {
    pub fn update_available_tables(&mut self, database: &Arc<Database>) {
        match database.get_tables() {
            Ok(tables) => {
                self.available_tables = tables.into_iter().map(|t| t.name).collect();
                if !self.available_tables.is_empty() && self.selected_table.is_empty() {
                    self.selected_table = self.available_tables[0].clone();
                }
            }
            Err(_) => {
                self.available_tables.clear();
            }
        }
    }

    pub fn update_available_columns(&mut self, database: &Arc<Database>) {
        if self.selected_table.is_empty() {
            self.available_columns.clear();
            return;
        }

        // Get column names and types
        let query = format!("SELECT * FROM \"{}\" LIMIT 1", self.selected_table);
        let (columns, types) = match (database.get_column_names(&query), database.get_column_types(&query)) {
            (Ok(cols), Ok(types)) => (cols, types),
            _ => {
                self.available_columns.clear();
                return;
            }
        };

        // Filter to show timestamp columns and time-like columns
        use datafusion::arrow::datatypes::DataType;
        let mut timestamp_columns = Vec::new();
        let mut string_time_columns = Vec::new();
        
        // First pass: collect all potential time columns
        for (col, dtype) in columns.iter().zip(types.iter()) {
            // Include actual timestamp columns
            if matches!(dtype, DataType::Timestamp(_, _)) {
                timestamp_columns.push(col.clone());
            }
            // Collect string columns that might contain time data
            else if matches!(dtype, DataType::Utf8) && col.contains("time") {
                string_time_columns.push(col.clone());
            }
        }
        
        // Second pass: check string columns in a single query if there are any
        if !string_time_columns.is_empty() {
            // Build a query to sample all potential time columns at once
            let column_samples: Vec<String> = string_time_columns.iter()
                .map(|col| format!("MAX(CASE WHEN \"{}\" IS NOT NULL AND \"{}\" != '' THEN \"{}\" END) as \"{}\"", col, col, col, col))
                .collect();
            
            let sample_query = format!("SELECT {} FROM \"{}\" LIMIT 100", column_samples.join(", "), self.selected_table);
            
            if let Ok(rows) = database.execute_query(&sample_query) {
                if let Some(first_row) = rows.first() {
                    for (idx, col) in string_time_columns.iter().enumerate() {
                        if idx < first_row.len() {
                            let value = &first_row[idx];
                            if !value.is_empty() && Self::can_parse_as_timestamp(value) {
                                timestamp_columns.push(col.clone());
                            }
                        }
                    }
                }
            }
        }
        
        self.available_columns = timestamp_columns;
        
        // Auto-select if there's only one timestamp column
        if self.available_columns.len() == 1 && self.selected_column.is_empty() {
            self.selected_column = self.available_columns[0].clone();
        }
    }

    pub fn show(&mut self, ctx: &egui::Context, database: Arc<Database>, output_dir: &std::path::Path) {
        if !self.visible {
            return;
        }

        // Update columns only if table changed
        if !self.selected_table.is_empty() {
            let should_update = match &self.last_updated_table {
                Some(last_table) => last_table != &self.selected_table,
                None => true,
            };
            
            if should_update {
                self.update_available_columns(&database);
                self.last_updated_table = Some(self.selected_table.clone());
            }
        }

        // Handle pending apply
        if self.pending_apply {
            self.apply_time_bin(&database, output_dir);
            self.pending_apply = false;
        }

        // Create state tracking variables
        let mut visible = self.visible;
        let mut should_generate_preview = false;
        let mut pending_apply = false;
        let mut new_selected_table: Option<String> = None;
        let mut new_selected_column: Option<String> = None;
        let mut new_strategy: Option<TimeBinStrategy> = None;
        let mut new_output_column_name: Option<String> = None;
        let mut new_output_filename: Option<String> = None;
        
        // Clone values we need in the closure
        let error_message = self.error_message.clone();
        let success_message = self.success_message.clone();
        let available_tables = self.available_tables.clone();
        let available_columns = self.available_columns.clone();
        let selected_table = self.selected_table.clone();
        let selected_column = self.selected_column.clone();
        let mut strategy = self.strategy.clone();
        let mut output_column_name = self.output_column_name.clone();
        let mut output_filename = self.output_filename.clone();
        let preview_data = self.preview_data.clone();
        
        let window_result = egui::Window::new("Add Time Bin Column")
            .open(&mut visible)
            .default_size([500.0, 600.0])
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading(RichText::new("Add Time Bin Column").size(20.0));
                ui.separator();

                // Error/Success messages
                if let Some(error) = &error_message {
                    // Truncate very long error messages and make them more user-friendly
                    let display_error = if error.len() > 100 {
                        format!("{}...", &error[..100])
                    } else {
                        error.clone()
                    };
                    ui.colored_label(Color32::RED, format!("Error: {}", display_error));
                    ui.separator();
                }

                if let Some(success) = &success_message {
                    ui.colored_label(Color32::GREEN, format!("✅ {}", success));
                    ui.separator();
                }

                // Table selection
                ui.group(|ui| {
                    ui.label(RichText::new("Select Table").strong());
                    egui::ComboBox::from_id_salt("table_selection")
                        .selected_text(if selected_table.is_empty() {
                            "Select a table".to_string()
                        } else {
                            selected_table.clone()
                        })
                        .show_ui(ui, |ui| {
                            for table in &available_tables {
                                let mut table_value = selected_table.clone();
                                if ui.selectable_value(&mut table_value, table.clone(), table).clicked() {
                                    new_selected_table = Some(table.clone());
                                    // Table selection changed, will update columns after UI
                                }
                            }
                        });
                });

                if !selected_table.is_empty() {
                    // Column selection
                    ui.group(|ui| {
                        ui.label(RichText::new("Select Time Column").strong());
                        
                        if available_columns.is_empty() {
                            ui.colored_label(egui::Color32::from_rgb(255, 150, 0), "⚠️ No timestamp columns found in this table");
                            ui.label("Time binning requires columns with timestamp data type.");
                        } else {
                            ui.label("Choose a timestamp column:");
                            egui::ComboBox::from_id_salt("column_selection")
                                .selected_text(if selected_column.is_empty() {
                                    "Select a time column".to_string()
                                } else {
                                    selected_column.clone()
                                })
                                .show_ui(ui, |ui| {
                                    for column in &available_columns {
                                        let mut column_value = selected_column.clone();
                                        if ui.selectable_value(&mut column_value, column.clone(), column).clicked() {
                                            new_selected_column = Some(column.clone());
                                            // Column selected
                                        }
                                    }
                                });
                        }
                    });

                    // Only show strategy selection if a time column is selected
                    if !selected_column.is_empty() {
                        // Strategy selection
                        ui.group(|ui| {
                        ui.label(RichText::new("Time Bin Strategy").strong());
                        
                        ui.vertical(|ui| {
                            ui.radio_value(&mut strategy, TimeBinStrategy::FixedInterval {
                                interval_seconds: 10,
                                interval_format: "10".to_string(),
                            }, "Regular Intervals");
                            ui.add_space(2.0);
                            ui.label(egui::RichText::new("    Split time into equal chunks (e.g., every hour, minute, or 10 seconds)").weak());
                            
                            ui.add_space(4.0);
                            ui.radio_value(&mut strategy, TimeBinStrategy::ManualIntervals {
                                intervals: Vec::new(),
                                interval_string: String::new(),
                            }, "Custom Boundaries");
                            ui.add_space(2.0);
                            ui.label(egui::RichText::new("    Define your own time boundaries (e.g., 9:00, 12:00, 17:00)").weak());
                            
                            ui.add_space(4.0);
                            ui.radio_value(&mut strategy, TimeBinStrategy::ThresholdBased {
                                threshold_seconds: 60,
                                threshold_format: "60".to_string(),
                            }, "Auto-detect Gaps");
                            ui.add_space(2.0);
                            ui.label(egui::RichText::new("    Start a new group when there's a time gap larger than your threshold").weak());
                        });

                        ui.separator();

                        // Handle strategy-specific UI
                        match &mut strategy {
                            TimeBinStrategy::FixedInterval { interval_seconds, interval_format } => {
                                ui.label("Enter interval in seconds or HH:MM:SS format:");
                                
                                let mut new_interval_seconds = *interval_seconds;
                                let format_str = interval_format.clone();
                                let parse_result = if let Ok(seconds) = format_str.parse::<u64>() {
                                    Some(seconds)
                                } else {
                                    Self::parse_time_format_static(&format_str)
                                };
                                
                                ui.horizontal(|ui| {
                                    ui.label("Interval:");
                                    if ui.text_edit_singleline(interval_format).changed() {
                                        if let Some(parsed) = parse_result {
                                            new_interval_seconds = parsed;
                                        }
                                    }
                                    
                                    // Common presets
                                    if ui.small_button("1s").clicked() {
                                        *interval_format = "1".to_string();
                                        new_interval_seconds = 1;
                                    }
                                    if ui.small_button("10s").clicked() {
                                        *interval_format = "10".to_string();
                                        new_interval_seconds = 10;
                                    }
                                    if ui.small_button("1m").clicked() {
                                        *interval_format = "60".to_string();
                                        new_interval_seconds = 60;
                                    }
                                    if ui.small_button("5m").clicked() {
                                        *interval_format = "300".to_string();
                                        new_interval_seconds = 300;
                                    }
                                    if ui.small_button("1h").clicked() {
                                        *interval_format = "3600".to_string();
                                        new_interval_seconds = 3600;
                                    }
                                });
                                *interval_seconds = new_interval_seconds;

                                ui.label(format!("Current interval: {} seconds", interval_seconds));
                                
                                // Preview estimation
                                if !selected_column.is_empty() {
                                    ui.label(egui::RichText::new("This will create time bins of equal duration.").weak());
                                }
                            }

                            TimeBinStrategy::ManualIntervals { intervals, interval_string } => {
                                ui.label("Define custom time boundaries for your bins");
                                ui.label("Enter times separated by commas (e.g., 09:00, 12:00, 17:00):");
                                
                                ui.horizontal(|ui| {
                                    ui.label("Intervals:");
                                    if ui.text_edit_singleline(interval_string).changed() {
                                        *intervals = interval_string
                                            .split(',')
                                            .map(|s| s.trim().to_string())
                                            .filter(|s| !s.is_empty())
                                            .collect();
                                    }
                                });

                                if !intervals.is_empty() {
                                    ui.label("Defined intervals:");
                                    for (i, interval) in intervals.iter().enumerate() {
                                        ui.label(format!("  {}. {}", i + 1, interval));
                                    }
                                }
                            }

                            TimeBinStrategy::ThresholdBased { threshold_seconds, threshold_format } => {
                                ui.label("Automatically detect groups based on time gaps");
                                ui.label("Start a new group when the gap between timestamps exceeds:");
                                
                                let mut new_threshold_seconds = *threshold_seconds;
                                let format_str = threshold_format.clone();
                                let parse_result = if let Ok(seconds) = format_str.parse::<u64>() {
                                    Some(seconds)
                                } else {
                                    Self::parse_time_format_static(&format_str)
                                };
                                
                                ui.horizontal(|ui| {
                                    ui.label("Threshold:");
                                    if ui.text_edit_singleline(threshold_format).changed() {
                                        if let Some(parsed) = parse_result {
                                            new_threshold_seconds = parsed;
                                        }
                                    }
                                });
                                *threshold_seconds = new_threshold_seconds;

                                ui.label(format!("Current threshold: {} seconds", threshold_seconds));
                            }
                        }
                    });

                    // Output configuration
                    ui.group(|ui| {
                        ui.label(RichText::new("Output Configuration").strong());
                        
                        // Output column name
                        ui.horizontal(|ui| {
                            ui.label("Output column name:");
                            ui.text_edit_singleline(&mut output_column_name);
                            if ui.button("Auto").clicked() && !selected_column.is_empty() {
                                output_column_name = format!("{}_bin", selected_column);
                            }
                        });
                        
                        // Output filename
                        ui.horizontal(|ui| {
                            ui.label("Output filename:");
                            ui.text_edit_singleline(&mut output_filename);
                            if ui.button("Auto").clicked() && !selected_table.is_empty() {
                                let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
                                output_filename = format!("{}_timebin_{}", selected_table, timestamp);
                            }
                            if !output_filename.is_empty() && !output_filename.ends_with(".arrow") {
                                ui.label(egui::RichText::new("(.arrow will be added)").weak());
                            }
                        });
                    });
                    
                    // Preview button and data
                    ui.separator();
                    if ui.button("Preview Results").clicked() && !selected_column.is_empty() {
                        should_generate_preview = true;
                    }
                    
                    if let Some(preview) = &preview_data {
                        ui.group(|ui| {
                            ui.label(egui::RichText::new("Preview Results:").strong());
                            ui.separator();
                            
                            // Show preview in a scrollable area
                            egui::ScrollArea::vertical()
                                .max_height(200.0)
                                .show(ui, |ui| {
                                    ui.label(egui::RichText::new(preview).weak().monospace());
                                });
                        });
                    }

                        // Apply button
                        ui.separator();
                        let can_apply = !output_column_name.is_empty() && !available_columns.contains(&output_column_name);
                        ui.add_enabled_ui(can_apply, |ui| {
                            if ui.button(RichText::new("Add Time Bin Column").size(16.0)).clicked() {
                                pending_apply = true;
                            }
                        });
                    } // End of column selected check
                }
            });
        
        // Apply state changes after the window
        self.visible = visible;
        
        if let Some(table) = new_selected_table {
            self.selected_table = table;
            self.error_message = None; // Clear errors when changing table
        }
        
        if let Some(column) = new_selected_column {
            self.selected_column = column;
            self.error_message = None; // Clear errors when changing column
        }
        
        if new_strategy.is_some() || self.strategy != strategy {
            self.strategy = strategy;
        }
        
        if new_output_column_name.is_some() || self.output_column_name != output_column_name {
            self.output_column_name = output_column_name;
        }
        
        if new_output_filename.is_some() || self.output_filename != output_filename {
            self.output_filename = output_filename;
        }
        
        if should_generate_preview {
            // Generate detailed preview info
            if let Err(e) = self.generate_preview_info(&database) {
                self.error_message = Some(format!("Preview error: {}", e));
            }
            self.preview_data = Some(self.generate_preview());
        }
        
        if pending_apply {
            self.pending_apply = true;
        }
    }

    fn parse_time_format_static(time_str: &str) -> Option<u64> {
        // Parse HH:MM:SS format
        let parts: Vec<&str> = time_str.split(':').collect();
        match parts.len() {
            1 => time_str.parse::<u64>().ok(),
            2 => {
                let minutes: u64 = parts[0].parse().ok()?;
                let seconds: u64 = parts[1].parse().ok()?;
                Some(minutes * 60 + seconds)
            }
            3 => {
                let hours: u64 = parts[0].parse().ok()?;
                let minutes: u64 = parts[1].parse().ok()?;
                let seconds: u64 = parts[2].parse().ok()?;
                Some(hours * 3600 + minutes * 60 + seconds)
            }
            _ => None,
        }
    }

    fn apply_time_bin(&mut self, database: &Arc<Database>, output_dir: &std::path::Path) {
        // Clear previous messages
        self.error_message = None;
        self.success_message = None;

        // Validate inputs
        if self.selected_table.is_empty() {
            self.error_message = Some("Please select a table".to_string());
            return;
        }

        if self.selected_column.is_empty() {
            self.error_message = Some("Please select a time column".to_string());
            return;
        }

        if self.output_column_name.is_empty() {
            self.error_message = Some("Please enter an output column name".to_string());
            return;
        }

        // Validate that the selected column contains time-like data
        match self.validate_time_column(database) {
            Ok(_) => {
                // Create the time bin configuration
                let config = TimeBinConfig {
                    selected_table: self.selected_table.clone(),
                    selected_column: self.selected_column.clone(),
                    strategy: self.strategy.clone(),
                    output_column_name: self.output_column_name.clone(),
                    output_filename: if self.output_filename.is_empty() {
                        None
                    } else {
                        Some(self.output_filename.clone())
                    },
                };

                // Apply the time bin logic
                match self.execute_time_bin(database, &config, output_dir) {
                    Ok(_) => {
                        self.success_message = Some(format!(
                            "Successfully added time bin column to table '{}'",
                            self.selected_table
                        ));
                    }
                    Err(e) => {
                        // Simplify common error messages
                        let error_msg = e.to_string();
                        let simple_error = if error_msg.contains("already exists") {
                            "A column with that name already exists. Please choose a different name."
                        } else if error_msg.contains("parse") || error_msg.contains("timestamp") {
                            "Unable to parse the time values. Please check the data format."
                        } else if error_msg.len() > 100 {
                            "An error occurred while creating the time bins. Please check your settings."
                        } else {
                            &error_msg
                        };
                        self.error_message = Some(simple_error.to_string());
                    }
                }
            }
            Err(_) => {
                self.error_message = Some("The selected column doesn't contain valid time data. Please select a column with timestamps.".to_string());
            }
        }
    }

    fn validate_time_column(&self, database: &Arc<Database>) -> Result<()> {
        // Get a sample of data from the selected column to validate it's time-like
        let query = format!(
            "SELECT \"{}\" FROM \"{}\" LIMIT 10",
            self.selected_column, self.selected_table
        );
        
        match database.execute_query(&query) {
            Ok(rows) => {
                if rows.is_empty() {
                    return Err(crate::core::error::LeafError::Custom(
                        "No data found in the selected column".to_string()
                    ));
                }

                // Check if the first few values can be parsed as timestamps
                let mut valid_count = 0;
                for (i, row) in rows.iter().enumerate() {
                    if !row.is_empty() {
                        let time_str = &row[0];
                        // Debug logging
                        if i < 3 {
                            println!("DEBUG validate_time_column: Row {}: '{}'", i, time_str);
                        }
                        if Self::can_parse_as_timestamp(time_str) {
                            valid_count += 1;
                        }
                    }
                }

                if valid_count == 0 {
                    return Err(crate::core::error::LeafError::Custom(
                        format!("Column '{}' does not appear to contain valid timestamp data", self.selected_column)
                    ));
                }

                Ok(())
            }
            Err(e) => {
                Err(crate::core::error::LeafError::Custom(
                    format!("Failed to validate column: {}", e)
                ))
            }
        }
    }

    fn can_parse_as_timestamp(time_str: &str) -> bool {
        // Try different timestamp formats
        if time_str.parse::<i64>().is_ok() {
            return true; // Unix timestamp
        }
        
        if chrono::DateTime::parse_from_rfc3339(time_str).is_ok() {
            return true; // ISO 8601
        }
        
        // Try naive datetime formats
        let formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%H:%M:%S%.f",  // Added for HH:MM:SS.sss format
            "%H:%M:%S",
            "%H:%M",
        ];
        
        for format in &formats {
            if chrono::NaiveDateTime::parse_from_str(time_str, format).is_ok() {
                return true;
            }
        }
        
        // Try time-only format
        if chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f").is_ok() {
            return true;
        }
        
        if chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S").is_ok() {
            return true;
        }
        
        if chrono::NaiveTime::parse_from_str(time_str, "%H:%M").is_ok() {
            return true;
        }
        
        false
    }

    fn execute_time_bin(&self, database: &Arc<Database>, config: &TimeBinConfig, output_dir: &std::path::Path) -> Result<()> {
        // Use the TimeGroupingEngine to apply the time bin logic
        let output_table_name = crate::core::TimeGroupingEngine::apply_grouping(database, config, output_dir)?;
        
        // Store the output table name for reference
        println!("Created time bin table: {}", output_table_name);
        
        Ok(())
    }
    
    fn generate_preview(&self) -> String {
        if let Some(ref preview) = self.preview_info {
            let mut result = format!(
                "Total rows: {}\n\
                Total bins: {}\n\
                Bin sizes: min={}, max={}, avg={:.1}\n\n\
                All bins:\n",
                preview.total_rows,
                preview.bin_count,
                preview.min_bin_size,
                preview.max_bin_size,
                preview.avg_bin_size
            );
            
            // Add all bins
            for (label, count) in &preview.sample_bins {
                result.push_str(&format!("  {} : {} rows\n", label, count));
            }
            
            result
        } else {
            match &self.strategy {
                TimeBinStrategy::FixedInterval { interval_seconds, .. } => {
                    format!("Will create bins every {} seconds", interval_seconds)
                }
                TimeBinStrategy::ManualIntervals { intervals, .. } => {
                    if intervals.is_empty() {
                        "No valid intervals parsed yet".to_string()
                    } else {
                        format!("Will create {} manual time bins", intervals.len())
                    }
                }
                TimeBinStrategy::ThresholdBased { threshold_seconds, .. } => {
                    format!("Will create new bins when gaps exceed {} seconds", threshold_seconds)
                }
            }
        }
    }
    
    pub fn generate_preview_info(&mut self, database: &Arc<Database>) -> Result<()> {
        if self.selected_table.is_empty() || self.selected_column.is_empty() {
            return Ok(());
        }
        
        // Create a temporary config to run the binning
        let config = TimeBinConfig {
            selected_table: self.selected_table.clone(),
            selected_column: self.selected_column.clone(),
            strategy: self.strategy.clone(),
            output_column_name: "preview_bin".to_string(),
            output_filename: None,
        };
        
        // Get the table data
        let query = format!("SELECT \"{}\" FROM \"{}\"", config.selected_column, config.selected_table);
        let rows = database.execute_query(&query)?;
        
        if rows.is_empty() {
            self.preview_info = None;
            return Ok(());
        }
        
        // Parse time values and create bins based on strategy
        let bins = self.create_preview_bins(&rows, &config.strategy)?;
        
        // Debug: Check what we're getting
        if !rows.is_empty() && !rows[0].is_empty() {
            println!("DEBUG: First few time values from query:");
            for (i, row) in rows.iter().take(5).enumerate() {
                if let Some(time_val) = row.get(0) {
                    println!("  Row {}: '{}'", i, time_val);
                }
            }
        }
        
        // Calculate statistics
        let mut bin_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for bin in &bins {
            *bin_counts.entry(bin.clone()).or_insert(0) += 1;
        }
        
        let total_rows = rows.len();
        let bin_count = bin_counts.len();
        let counts: Vec<usize> = bin_counts.values().cloned().collect();
        let min_bin_size = *counts.iter().min().unwrap_or(&0);
        let max_bin_size = *counts.iter().max().unwrap_or(&0);
        let avg_bin_size = if bin_count > 0 {
            total_rows as f64 / bin_count as f64
        } else {
            0.0
        };
        
        // Get all bins sorted
        let mut all_bins: Vec<(String, usize)> = bin_counts.into_iter().collect();
        all_bins.sort_by(|a, b| {
            // Try to sort numerically if bins are like "Bin_0", "Bin_1", etc.
            let a_num = a.0.strip_prefix("Bin_").and_then(|s| s.parse::<i32>().ok());
            let b_num = b.0.strip_prefix("Bin_").and_then(|s| s.parse::<i32>().ok());
            
            match (a_num, b_num) {
                (Some(a_n), Some(b_n)) => a_n.cmp(&b_n),
                _ => a.0.cmp(&b.0)
            }
        });
        
        self.preview_info = Some(TimeBinPreview {
            total_rows,
            bin_count,
            min_bin_size,
            max_bin_size,
            avg_bin_size,
            sample_bins: all_bins,
        });
        
        Ok(())
    }
    
    fn create_preview_bins(&self, rows: &[Vec<String>], strategy: &TimeBinStrategy) -> Result<Vec<String>> {
        let mut bins = Vec::with_capacity(rows.len());
        
        match strategy {
            TimeBinStrategy::FixedInterval { interval_seconds, .. } => {
                for row in rows {
                    if let Some(time_str) = row.get(0) {
                        if let Ok(timestamp) = self.parse_timestamp(time_str) {
                            let bin = timestamp / interval_seconds;
                            bins.push(format!("Bin_{}", bin));
                        } else {
                            bins.push("Invalid".to_string());
                        }
                    }
                }
            }
            TimeBinStrategy::ManualIntervals { intervals, .. } => {
                let parsed_intervals = self.parse_manual_intervals(intervals)?;
                for row in rows {
                    if let Some(time_str) = row.get(0) {
                        if let Ok(timestamp) = self.parse_timestamp(time_str) {
                            let bin_idx = parsed_intervals.iter()
                                .position(|&interval| timestamp < interval)
                                .unwrap_or(parsed_intervals.len());
                            bins.push(format!("Interval_{}", bin_idx));
                        } else {
                            bins.push("Invalid".to_string());
                        }
                    }
                }
            }
            TimeBinStrategy::ThresholdBased { threshold_seconds, .. } => {
                let mut current_bin = 0;
                let mut last_timestamp = None;
                
                for row in rows {
                    if let Some(time_str) = row.get(0) {
                        if let Ok(timestamp) = self.parse_timestamp(time_str) {
                            if let Some(last) = last_timestamp {
                                if timestamp - last > *threshold_seconds {
                                    current_bin += 1;
                                }
                            }
                            bins.push(format!("Group_{}", current_bin));
                            last_timestamp = Some(timestamp);
                        } else {
                            bins.push("Invalid".to_string());
                        }
                    }
                }
            }
        }
        
        Ok(bins)
    }
    
    fn parse_timestamp(&self, time_str: &str) -> Result<u64> {
        // Try to parse as seconds since epoch
        if let Ok(timestamp) = time_str.parse::<u64>() {
            return Ok(timestamp);
        }
        
        // Try ISO format
        if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(time_str) {
            return Ok(datetime.timestamp() as u64);
        }
        
        // Try other formats
        let formats = [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%H:%M:%S%.f",
            "%H:%M:%S",
            "%H:%M",
        ];
        
        for format in &formats {
            if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(time_str, format) {
                return Ok(datetime.and_utc().timestamp() as u64);
            }
        }
        
        // Try time-only formats (for HH:MM:SS.sss)
        let time_formats = ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"];
        for format in &time_formats {
            if let Ok(time) = chrono::NaiveTime::parse_from_str(time_str, format) {
                // Convert time to seconds since midnight
                let datetime = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap().and_time(time);
                let seconds = datetime.timestamp() % 86400; // seconds in a day
                return Ok(seconds as u64);
            }
        }
        
        Err(crate::core::error::LeafError::Custom(format!("Unable to parse timestamp: {}", time_str)))
    }
    
    fn parse_manual_intervals(&self, intervals: &[String]) -> Result<Vec<u64>> {
        let mut parsed = Vec::new();
        for interval in intervals {
            if let Ok(timestamp) = self.parse_timestamp(interval) {
                parsed.push(timestamp);
            }
        }
        parsed.sort_unstable();
        Ok(parsed)
    }
} 