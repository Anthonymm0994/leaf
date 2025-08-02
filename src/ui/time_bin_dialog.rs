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
}

pub struct TimeBinDialog {
    pub visible: bool,
    pub available_tables: Vec<String>,
    pub available_columns: Vec<String>,
    pub selected_table: String,
    pub selected_column: String,
    pub strategy: TimeBinStrategy,
    pub output_column_name: String,
    pub error_message: Option<String>,
    pub success_message: Option<String>,
    pub pending_apply: bool,
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
            output_column_name: "time_bin".to_string(),
            error_message: None,
            success_message: None,
            pending_apply: false,
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

        // Get all columns first
        let all_columns = match database.get_column_names(&format!("SELECT * FROM \"{}\"", self.selected_table)) {
            Ok(columns) => columns,
            Err(_) => {
                self.available_columns.clear();
                return;
            }
        };

        // For now, show all columns but we'll validate them when the user tries to apply
        // In a more sophisticated implementation, we could check the actual data types
        self.available_columns = all_columns;
    }

    pub fn show(&mut self, ctx: &egui::Context, database: Arc<Database>, output_dir: &std::path::Path) {
        if !self.visible {
            return;
        }

        // Update columns if table changed
        if !self.selected_table.is_empty() {
            self.update_available_columns(&database);
        }

        // Handle pending apply
        if self.pending_apply {
                                    self.apply_time_bin(&database, output_dir);
            self.pending_apply = false;
        }

        egui::Window::new("Add Time Bin Column")
            .open(&mut self.visible)
            .default_size([500.0, 600.0])
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading(RichText::new("Add Time Bin Column").size(20.0));
                ui.separator();

                // Error/Success messages
                if let Some(error) = &self.error_message {
                    ui.colored_label(Color32::RED, format!("❌ Error: {}", error));
                    ui.separator();
                }

                if let Some(success) = &self.success_message {
                    ui.colored_label(Color32::GREEN, format!("✅ {}", success));
                    ui.separator();
                }

                // Table selection
                ui.group(|ui| {
                    ui.label(RichText::new("Select Table").strong());
                    egui::ComboBox::from_id_source("table_selection")
                        .selected_text(if self.selected_table.is_empty() {
                            "Select a table".to_string()
                        } else {
                            self.selected_table.clone()
                        })
                        .show_ui(ui, |ui| {
                            for table in &self.available_tables {
                                if ui.selectable_value(&mut self.selected_table, table.clone(), table).clicked() {
                                    // Table selection changed, will update columns after UI
                                }
                            }
                        });
                });

                if !self.selected_table.is_empty() {
                    // Column selection
                    ui.group(|ui| {
                        ui.label(RichText::new("Select Time Column").strong());
                        ui.label("Choose a column containing timestamp data:");
                        egui::ComboBox::from_id_source("column_selection")
                            .selected_text(if self.selected_column.is_empty() {
                                "Select a time column".to_string()
                            } else {
                                self.selected_column.clone()
                            })
                            .show_ui(ui, |ui| {
                                for column in &self.available_columns {
                                    if ui.selectable_value(&mut self.selected_column, column.clone(), column).clicked() {
                                        // Column selected
                                    }
                                }
                            });
                    });

                    // Strategy selection
                    ui.group(|ui| {
                        ui.label(RichText::new("Time Bin Strategy").strong());
                        
                        ui.horizontal(|ui| {
                            ui.radio_value(&mut self.strategy, TimeBinStrategy::FixedInterval {
                                interval_seconds: 10,
                                interval_format: "10".to_string(),
                            }, "Fixed Interval");
                            ui.radio_value(&mut self.strategy, TimeBinStrategy::ManualIntervals {
                                intervals: Vec::new(),
                                interval_string: String::new(),
                            }, "Manual Intervals");
                            ui.radio_value(&mut self.strategy, TimeBinStrategy::ThresholdBased {
                                threshold_seconds: 60,
                                threshold_format: "60".to_string(),
                            }, "Threshold-Based");
                        });

                        ui.separator();

                        // Handle strategy-specific UI
                        match &mut self.strategy {
                            TimeBinStrategy::FixedInterval { interval_seconds, interval_format } => {
                                ui.label("Fixed interval time bins");
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
                                });
                                *interval_seconds = new_interval_seconds;

                                ui.label(format!("Current interval: {} seconds", interval_seconds));
                            }

                            TimeBinStrategy::ManualIntervals { intervals, interval_string } => {
                                ui.label("Manual interval time bins");
                                ui.label("Enter comma-separated times (e.g., 10:00, 15:30, 20:00):");
                                
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
                                ui.label("Threshold-based time bins");
                                ui.label("Start a new bin when time gap exceeds threshold:");
                                
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

                    // Output column name
                    ui.group(|ui| {
                        ui.label(RichText::new("Output Configuration").strong());
                        ui.horizontal(|ui| {
                            ui.label("Output column name:");
                            ui.text_edit_singleline(&mut self.output_column_name);
                        });
                    });

                    // Apply button
                    ui.separator();
                    if ui.button(RichText::new("Add Time Bin Column").size(16.0)).clicked() {
                        self.pending_apply = true;
                    }
                }
            });
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
                        self.error_message = Some(format!("Failed to add time bin column: {}", e));
                    }
                }
            }
            Err(e) => {
                self.error_message = Some(format!("Invalid time column: {}", e));
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
                for row in &rows {
                    if !row.is_empty() {
                        let time_str = &row[0];
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
            "%H:%M:%S",
            "%H:%M",
        ];
        
        for format in &formats {
            if chrono::NaiveDateTime::parse_from_str(time_str, format).is_ok() {
                return true;
            }
        }
        
        // Try time-only format
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
} 