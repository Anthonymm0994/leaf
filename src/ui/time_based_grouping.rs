use egui::{self, RichText, Color32};
use std::sync::Arc;
use crate::core::database::Database;
use crate::core::error::Result;
use crate::ui::time_bin_dialog::{TimeBinStrategy as GroupingStrategy, TimeBinConfig as TimeBasedGroupingConfig};

pub struct TimeBasedGroupingDialog {
    pub visible: bool,
    pub available_tables: Vec<String>,
    pub available_columns: Vec<String>,
    pub selected_table: String,
    pub selected_column: String,
    pub strategy: GroupingStrategy,
    pub output_column_name: String,
    pub error_message: Option<String>,
    pub success_message: Option<String>,
    pub pending_apply: bool,
}

impl Default for TimeBasedGroupingDialog {
    fn default() -> Self {
        Self {
            visible: false,
            available_tables: Vec::new(),
            available_columns: Vec::new(),
            selected_table: String::new(),
            selected_column: String::new(),
            strategy: GroupingStrategy::FixedInterval {
                interval_seconds: 10,
                interval_format: "10".to_string(),
            },
            output_column_name: "time_group".to_string(),
            error_message: None,
            success_message: None,
            pending_apply: false,
        }
    }
}

impl TimeBasedGroupingDialog {
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

        match database.get_column_names(&format!("SELECT * FROM \"{}\"", self.selected_table)) {
            Ok(columns) => {
                self.available_columns = columns;
                if !self.available_columns.is_empty() && self.selected_column.is_empty() {
                    self.selected_column = self.available_columns[0].clone();
                }
            }
            Err(_) => {
                self.available_columns.clear();
            }
        }
    }

    pub fn show(&mut self, ctx: &egui::Context, database: Arc<Database>) {
        if !self.visible {
            return;
        }

        // Update columns if table changed
        if !self.selected_table.is_empty() {
            self.update_available_columns(&database);
        }

        // Handle pending apply
        if self.pending_apply {
            self.apply_grouping(&database);
            self.pending_apply = false;
        }

        egui::Window::new("Time-Based Grouping")
            .open(&mut self.visible)
            .default_size([500.0, 600.0])
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading(RichText::new("Time-Based Grouping").size(20.0));
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
                        ui.label(RichText::new("Grouping Strategy").strong());
                        
                        ui.horizontal(|ui| {
                            ui.radio_value(&mut self.strategy, GroupingStrategy::FixedInterval {
                                interval_seconds: 10,
                                interval_format: "10".to_string(),
                            }, "Fixed Interval");
                            ui.radio_value(&mut self.strategy, GroupingStrategy::ManualIntervals {
                                intervals: Vec::new(),
                                interval_string: String::new(),
                            }, "Manual Intervals");
                            ui.radio_value(&mut self.strategy, GroupingStrategy::ThresholdBased {
                                threshold_seconds: 60,
                                threshold_format: "60".to_string(),
                            }, "Threshold-Based");
                        });

                        ui.separator();

                        // Handle strategy-specific UI
                        match &mut self.strategy {
                            GroupingStrategy::FixedInterval { interval_seconds, interval_format } => {
                                ui.label("Fixed interval grouping");
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

                            GroupingStrategy::ManualIntervals { intervals, interval_string } => {
                                ui.label("Manual interval grouping");
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

                            GroupingStrategy::ThresholdBased { threshold_seconds, threshold_format } => {
                                ui.label("Threshold-based grouping");
                                ui.label("Start a new group when time gap exceeds threshold:");
                                
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
                    if ui.button(RichText::new("Apply Time-Based Grouping").size(16.0)).clicked() {
                        // Defer the apply_grouping call to avoid borrow checker issues
                        // We'll set a flag and handle it in the next frame
                        self.pending_apply = true;
                    }
                }
            });
    }

    fn parse_time_format(&self, time_str: &str) -> Option<u64> {
        Self::parse_time_format_static(time_str)
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

    fn apply_grouping(&mut self, database: &Arc<Database>) {
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

        // Create the grouping configuration
        let config = TimeBasedGroupingConfig {
            selected_table: self.selected_table.clone(),
            selected_column: self.selected_column.clone(),
            strategy: self.strategy.clone(),
            output_column_name: self.output_column_name.clone(),
        };

        // Apply the grouping (this would be implemented in the core logic)
        match self.execute_grouping(database, &config) {
            Ok(_) => {
                self.success_message = Some(format!(
                    "Successfully applied time-based grouping to table '{}'",
                    self.selected_table
                ));
            }
            Err(e) => {
                self.error_message = Some(format!("Failed to apply grouping: {}", e));
            }
        }
    }

    fn execute_grouping(&self, database: &Arc<Database>, config: &TimeBasedGroupingConfig) -> Result<()> {
        // Use the TimeGroupingEngine to apply the grouping
        let output_table_name = crate::core::TimeGroupingEngine::apply_grouping(database, config)?;
        
        // Store the output table name for reference
        println!("Created grouped table: {}", output_table_name);
        
        Ok(())
    }
} 