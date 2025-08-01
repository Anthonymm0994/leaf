use egui::{Context, Id, ScrollArea, Window, Ui};
use rfd::FileDialog;
use std::path::PathBuf;
use crate::infer::ColumnType;

pub struct CsvImportDialog {
    id: Id,
    csv_path: Option<PathBuf>,
    table_name: String,
    has_headers: bool,
    skip_rows: usize,
    delimiter: char,
    column_configs: Vec<ColumnConfig>,
    is_importing: bool,
    error: Option<String>,
}

#[derive(Clone)]
struct ColumnConfig {
    name: String,
    data_type: ColumnType,
    is_primary_key: bool,
    is_not_null: bool,
    is_unique: bool,
    create_index: bool,
}

impl CsvImportDialog {
    pub fn new(id: Id) -> Self {
        Self {
            id,
            csv_path: None,
            table_name: String::new(),
            has_headers: true,
            skip_rows: 0,
            delimiter: ',',
            column_configs: Vec::new(),
            is_importing: false,
            error: None,
        }
    }
    
    pub fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true;
        
        Window::new("Import CSV")
            .id(self.id)
            .default_size([600.0, 500.0])
            .resizable(true)
            .open(&mut open)
            .show(ctx, |ui| {
                self.render_content(ui);
            });
        
        open
    }
    
    fn render_content(&mut self, ui: &mut Ui) {
        ui.vertical(|ui| {
            // File selection
            ui.horizontal(|ui| {
                ui.label("CSV File:");
                if let Some(path) = &self.csv_path {
                    ui.label(path.display().to_string());
                } else {
                    ui.label("No file selected");
                }
                
                if ui.button("Browse...").clicked() {
                    if let Some(path) = FileDialog::new()
                        .add_filter("CSV", &["csv"])
                        .pick_file()
                    {
                        self.csv_path = Some(path);
                        self.analyze_csv();
                    }
                }
            });
            
            ui.separator();
            
            // Table name
            ui.horizontal(|ui| {
                ui.label("Table name:");
                ui.text_edit_singleline(&mut self.table_name);
            });
            
            // Import options
            ui.horizontal(|ui| {
                ui.checkbox(&mut self.has_headers, "First row contains headers");
                ui.separator();
                ui.label("Skip rows:");
                ui.add(egui::DragValue::new(&mut self.skip_rows).range(0..=100));
            });
            
            ui.horizontal(|ui| {
                ui.label("Delimiter:");
                ui.radio_value(&mut self.delimiter, ',', "Comma");
                ui.radio_value(&mut self.delimiter, '\t', "Tab");
                ui.radio_value(&mut self.delimiter, ';', "Semicolon");
                ui.radio_value(&mut self.delimiter, '|', "Pipe");
            });
            
            ui.separator();
            
            // Column configuration
            if !self.column_configs.is_empty() {
                ui.heading("Column Configuration");
                
                ScrollArea::vertical()
                    .max_height(300.0)
                    .show(ui, |ui| {
                        for config in &mut self.column_configs {
                            ui.group(|ui| {
                                ui.horizontal(|ui| {
                                    ui.label(&config.name);
                                    
                                    egui::ComboBox::from_label("Type")
                                        .selected_text(format!("{:?}", config.data_type))
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(&mut config.data_type, ColumnType::Integer, "Integer");
                                            ui.selectable_value(&mut config.data_type, ColumnType::Real, "Real");
                                            ui.selectable_value(&mut config.data_type, ColumnType::Text, "Text");
                                            ui.selectable_value(&mut config.data_type, ColumnType::Boolean, "Boolean");
                                            ui.selectable_value(&mut config.data_type, ColumnType::Date, "Date");
                                            ui.selectable_value(&mut config.data_type, ColumnType::DateTime, "DateTime");
                                        });
                                    
                                    ui.checkbox(&mut config.is_primary_key, "PK");
                                    ui.checkbox(&mut config.is_not_null, "NOT NULL");
                                    ui.checkbox(&mut config.is_unique, "UNIQUE");
                                    ui.checkbox(&mut config.create_index, "INDEX");
                                });
                            });
                        }
                    });
            }
            
            ui.separator();
            
            // Error display
            if let Some(error) = &self.error {
                ui.colored_label(egui::Color32::from_rgb(255, 100, 100), format!("Error: {}", error));
            }
            
            // Import button
            ui.horizontal(|ui| {
                ui.add_enabled_ui(!self.table_name.is_empty() && self.csv_path.is_some(), |ui| {
                    if ui.button("Import").clicked() {
                        self.import_to_database();
                    }
                });
                
                if ui.button("Cancel").clicked() {
                    self.error = Some("Import cancelled".to_string());
                }
            });
        });
    }
    
    fn analyze_csv(&mut self) {
        // Simplified CSV analysis
        if let Some(path) = &self.csv_path {
            // For now, just create some dummy columns
            self.column_configs = vec![
                ColumnConfig {
                    name: "column1".to_string(),
                    data_type: ColumnType::Text,
                    is_primary_key: false,
                    is_not_null: false,
                    is_unique: false,
                    create_index: false,
                },
            ];
            
            // Set default table name from filename
            if self.table_name.is_empty() {
                if let Some(stem) = path.file_stem() {
                    self.table_name = stem.to_string_lossy().to_string();
                }
            }
        }
    }
    
    fn import_to_database(&mut self) {
        // This method would need to be refactored to work with the app's database
        // For now, we'll just set an error
        self.error = Some("Import functionality needs to be connected to the app's database".to_string());
    }
} 