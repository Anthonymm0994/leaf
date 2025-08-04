use egui;
use datafusion::arrow::datatypes::DataType;
use crate::core::{Database, TableInfo};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum GroupingRule {
    ValueChange { column: String },
    ValueEquals { column: String, value: String },
    IsEmpty { column: String },
    // TimeGap removed - this belongs in Time Bin dialog
}

impl GroupingRule {
    fn display_name(&self) -> String {
        match self {
            Self::ValueChange { column } => format!("When '{}' changes", column),
            Self::ValueEquals { column, value } => format!("When '{}' = '{}'", column, value),
            Self::IsEmpty { column } => format!("When '{}' is empty", column),

        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupingConfig {
    pub rule: GroupingRule,
    pub output_column: String,
    pub reset_on_change: bool,
}

#[derive(Debug, Clone)]
pub struct EnhancedGroupingDialog {
    pub visible: bool,
    pub selected_table: Option<String>,
    pub available_tables: Vec<TableInfo>,
    pub available_columns: Vec<String>,
    
    // Current configuration
    pub rule_type: String,
    pub selected_column: String,
    pub value_input: String,
    pub threshold_input: String,
    pub output_name: String,
    pub reset_on_change: bool,
    
    // Configurations to apply
    pub configurations: Vec<GroupingConfig>,
    
    // UI state
    pub error_message: Option<String>,
    pub success_message: Option<String>,
    pub show_preview: bool,
    pub example_type: String,
    pub output_filename: String,
}

impl Default for EnhancedGroupingDialog {
    fn default() -> Self {
        Self {
            visible: false,
            selected_table: None,
            available_tables: Vec::new(),
            available_columns: Vec::new(),
            rule_type: "value_change".to_string(),
            selected_column: String::new(),
            value_input: String::new(),
            threshold_input: "60".to_string(),
            output_name: String::new(),
            reset_on_change: true,
            configurations: Vec::new(),
            error_message: None,
            success_message: None,
            show_preview: false,
            example_type: "Value Change".to_string(),
            output_filename: String::new(),
        }
    }
}

impl EnhancedGroupingDialog {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn show(&mut self, ctx: &egui::Context, database: &Database) -> Option<EnhancedGroupingRequest> {
        if !self.visible {
            return None;
        }
        
        let mut result = None;
        let mut should_update_columns = false;
        let mut should_add_config = false;
        let mut should_apply = false;
        let mut should_cancel = false;
        let mut config_to_remove = None;
        let mut new_selected_table = None;
        
        egui::Window::new("Add Group ID Columns")
            .open(&mut self.visible)
            .resizable(true)
            .default_size([600.0, 700.0])
            .show(ctx, |ui| {
                ui.heading("Add Group ID Columns");
                ui.label("Create auto-incrementing IDs based on data patterns");
                ui.separator();
                
                // Table Selection
                ui.horizontal(|ui| {
                    ui.label("Table:");
                    egui::ComboBox::from_label("grouping_table_select")
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
                    
                    // Rule Configuration
                    ui.group(|ui| {
                        ui.heading("Grouping Rule");
                        
                        // Rule Type Selection
                        ui.horizontal(|ui| {
                            ui.label("Rule Type:");
                            egui::ComboBox::from_label("rule_type")
                                .selected_text(match self.rule_type.as_str() {
                                    "value_change" => "When value changes",
                                    "value_equals" => "When value matches",
                                    "is_empty" => "When value is blank",
                    
                                    _ => "Select a rule",
                                })
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut self.rule_type, "value_change".to_string(), "When value changes")
                                        .on_hover_text("Create a new group each time the value changes\nExample: A,A,B,B,C → groups 0,0,1,1,2");
                                    ui.selectable_value(&mut self.rule_type, "value_equals".to_string(), "When value matches")
                                        .on_hover_text("Create groups when the value equals a specific value\nExample: Find all rows where status='active'");
                                    ui.selectable_value(&mut self.rule_type, "is_empty".to_string(), "When value is blank")
                                        .on_hover_text("Create groups based on empty/blank values\nExample: Group records with missing data");

                                });
                        });
                        
                        // Column Selection
                        ui.horizontal(|ui| {
                            ui.label("Column:");
                            egui::ComboBox::from_label("grouping_column")
                                .selected_text(&self.selected_column)
                                .show_ui(ui, |ui| {
                                    for col in &self.available_columns {
                                        ui.selectable_value(&mut self.selected_column, col.clone(), col);
                                    }
                                });
                        });
                        
                        // Additional inputs based on rule type
                        match self.rule_type.as_str() {
                            "value_equals" => {
                                ui.horizontal(|ui| {
                                    ui.label("Value:");
                                    ui.text_edit_singleline(&mut self.value_input);
                                });
                            }

                            _ => {}
                        }
                        
                        // Output column name
                        ui.horizontal(|ui| {
                            ui.label("Output Column Name:");
                            ui.text_edit_singleline(&mut self.output_name);
                            if self.output_name.is_empty() && !self.selected_column.is_empty() {
                                if ui.small_button("Auto").clicked() {
                                    self.output_name = format!("{}_group_id", self.selected_column);
                                }
                            }
                        });
                        
                        // Reset option
                        ui.checkbox(&mut self.reset_on_change, "Reset ID to 0 on each group");
                        
                        ui.separator();
                        
                        // Action buttons
                        ui.horizontal(|ui| {
                            if ui.button("Add to List").clicked() {
                                should_add_config = true;
                            }
                        });
                    });
                    
                    // Configured Rules List
                    if !self.configurations.is_empty() {
                        ui.separator();
                        ui.heading("Rules to Apply");
                        
                        egui::ScrollArea::vertical()
                            .max_height(150.0)
                            .show(ui, |ui| {
                                for (idx, config) in self.configurations.iter().enumerate() {
                                    ui.horizontal(|ui| {
                                        ui.label(format!("{}:", config.output_column));
                                        ui.label(config.rule.display_name());
                                        if ui.small_button("🗑️").clicked() {
                                            config_to_remove = Some(idx);
                                        }
                                    });
                                }
                            });
                        
                        // Output filename
                        ui.separator();
                        ui.horizontal(|ui| {
                            ui.label("Output filename:");
                            ui.text_edit_singleline(&mut self.output_filename);
                            if ui.button("Auto").clicked() && self.selected_table.is_some() {
                                let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
                                self.output_filename = format!("{}_groupid_{}", 
                                    self.selected_table.as_ref().unwrap(), timestamp);
                            }
                            if !self.output_filename.is_empty() && !self.output_filename.ends_with(".arrow") {
                                ui.label(egui::RichText::new("(.arrow will be added)").weak());
                            }
                        });
                    }
                    
                    // Example Preview with dropdown
                    ui.separator();
                    ui.collapsing("Examples", |ui| {
                        ui.horizontal(|ui| {
                            ui.label("Select example:");
                            egui::ComboBox::from_label("")
                                .selected_text(&self.example_type)
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut self.example_type, "Value Change".to_string(), "Value Change");
                                    ui.selectable_value(&mut self.example_type, "Value Change (Reset)".to_string(), "Value Change (Reset)");
                                    ui.selectable_value(&mut self.example_type, "Is Empty".to_string(), "Is Empty");
                                    ui.selectable_value(&mut self.example_type, "Is Empty (Reset)".to_string(), "Is Empty (Reset)");
                                });
                        });
                        
                        ui.separator();
                        
                        match self.example_type.as_str() {
                            "Value Change" => {
                                ui.label("When 'good_time' changes (continuous numbering):");
                                ui.monospace("good_time    | good_time_group_id");
                                ui.monospace("00:00:00.000 | 0");
                                ui.monospace("00:00:00.000 | 0");
                                ui.monospace("00:00:00.000 | 0");
                                ui.monospace("00:00:01.000 | 1  ← value changed");
                                ui.monospace("00:00:01.000 | 1");
                                ui.monospace("00:00:02.000 | 2  ← value changed");
                            }
                            "Value Change (Reset)" => {
                                ui.label("When 'good_time' changes (reset to 0 on each group):");
                                ui.monospace("good_time    | row_in_group");
                                ui.monospace("00:00:00.000 | 0");
                                ui.monospace("00:00:00.000 | 1");
                                ui.monospace("00:00:00.000 | 2");
                                ui.monospace("00:00:01.000 | 0  ← reset to 0");
                                ui.monospace("00:00:01.000 | 1");
                                ui.monospace("00:00:02.000 | 0  ← reset to 0");
                            }
                            "Is Empty" => {
                                ui.label("When 'dumb_time' is empty (continuous numbering):");
                                ui.monospace("dumb_time    | block_id");
                                ui.monospace("[empty]      | 0  ← starts block 0");
                                ui.monospace("00:01:30.000 | 0");
                                ui.monospace("00:02:45.000 | 0");
                                ui.monospace("[empty]      | 1  ← starts block 1");
                                ui.monospace("00:01:15.000 | 1");
                                ui.monospace("[empty]      | 2  ← starts block 2");
                            }
                            "Is Empty (Reset)" => {
                                ui.label("When 'dumb_time' is empty (reset within each block):");
                                ui.monospace("dumb_time    | row_in_block");
                                ui.monospace("[empty]      | 0");
                                ui.monospace("00:01:30.000 | 1");
                                ui.monospace("00:02:45.000 | 2");
                                ui.monospace("[empty]      | 0  ← reset to 0");
                                ui.monospace("00:01:15.000 | 1");
                                ui.monospace("[empty]      | 0  ← reset to 0");
                            }
                            _ => {}
                        }
                    });
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
        
        if should_update_columns {
            self.update_available_columns(database);
        }
        
        if should_add_config {
            if self.validate_current_config() {
                let rule = match self.rule_type.as_str() {
                    "value_change" => GroupingRule::ValueChange { 
                        column: self.selected_column.clone() 
                    },
                    "value_equals" => GroupingRule::ValueEquals { 
                        column: self.selected_column.clone(),
                        value: self.value_input.clone()
                    },
                    "is_empty" => GroupingRule::IsEmpty { 
                        column: self.selected_column.clone() 
                    },

                    _ => GroupingRule::ValueChange { 
                        column: self.selected_column.clone() 
                    },
                };
                
                self.configurations.push(GroupingConfig {
                    rule,
                    output_column: self.output_name.clone(),
                    reset_on_change: self.reset_on_change,
                });
                
                self.clear_current_config();
                self.success_message = Some("Rule added to list".to_string());
            }
        }
        
        if let Some(idx) = config_to_remove {
            self.configurations.remove(idx);
        }
        
        if should_apply {
            if let Some(table_name) = &self.selected_table {
                result = Some(EnhancedGroupingRequest {
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
                self.available_columns = columns;
            }
        }
    }
    
    fn validate_current_config(&mut self) -> bool {
        self.error_message = None;
        
        if self.selected_column.is_empty() {
            self.error_message = Some("Please select a column".to_string());
            return false;
        }
        
        if self.output_name.is_empty() {
            self.error_message = Some("Please provide an output column name".to_string());
            return false;
        }
        
        if self.rule_type == "value_equals" && self.value_input.is_empty() {
            self.error_message = Some("Please provide a value to match".to_string());
            return false;
        }
        

        
        // Check for duplicate output names
        if self.configurations.iter().any(|c| c.output_column == self.output_name) {
            self.error_message = Some("Output column name already exists".to_string());
            return false;
        }
        
        true
    }
    
    fn clear_current_config(&mut self) {
        self.selected_column.clear();
        self.value_input.clear();
        self.output_name.clear();
        self.error_message = None;
    }
    
    pub fn update_available_tables(&mut self, database: &Database) {
        self.available_tables = database.get_tables().unwrap_or_default();
    }
    
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug, Clone)]
pub struct EnhancedGroupingRequest {
    pub table_name: String,
    pub configurations: Vec<GroupingConfig>,
    pub output_filename: Option<String>,
}