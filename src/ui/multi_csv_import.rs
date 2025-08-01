use egui::{Context, Id, ScrollArea};
use rfd::FileDialog;
use std::path::PathBuf;
use crate::core::{CsvReader, Database};
use crate::infer::{ColumnType, TypeInferrer};
use indexmap::IndexMap;

pub struct MultiCsvImportDialog {
    id: Id,
    show: bool,
    database_path: Option<PathBuf>,
    csv_files: Vec<CsvFileConfig>,
    error: Option<String>,
}

struct CsvFileConfig {
    path: PathBuf,
    table_name: String,
    headers: Vec<String>,
    column_types: IndexMap<String, ColumnType>,
    sample_data: Vec<Vec<String>>,
    create_indexes: IndexMap<String, bool>,
    include: bool,
}

impl MultiCsvImportDialog {
    pub fn new(id: Id) -> Self {
        Self {
            id,
            show: true,
            database_path: None,
            csv_files: Vec::new(),
            error: None,
        }
    }
    
    pub fn show(&mut self, ctx: &Context) -> Option<PathBuf> {
        if !self.show {
            return None;
        }
        
        let mut keep_open = true;
        let mut created_db_path = None;
        
        egui::Window::new("Create Database from CSV Files")
            .id(self.id)
            .default_size([800.0, 600.0])
            .resizable(true)
            .collapsible(false)
            .open(&mut keep_open)
            .show(ctx, |ui| {
                self.render_content(ui, &mut created_db_path);
            });
        
        if !keep_open {
            self.show = false;
        }
        
        created_db_path
    }
    
    fn render_content(&mut self, ui: &mut egui::Ui, created_db_path: &mut Option<PathBuf>) {
        ui.vertical(|ui| {
            // Database path selection
            ui.horizontal(|ui| {
                ui.label("Database Path:");
                if let Some(path) = &self.database_path {
                    ui.label(path.display().to_string());
                } else {
                    ui.label("No path selected");
                }
                
                if ui.button("Browse...").clicked() {
                    if let Some(path) = FileDialog::new()
                        .add_filter("SQLite", &["db", "sqlite", "sqlite3"])
                        .save_file()
                    {
                        self.database_path = Some(path);
                    }
                }
            });
            
            ui.separator();
            
            // CSV file management
            ui.horizontal(|ui| {
                ui.heading("CSV Files");
                
                if ui.button("➕ Add CSV Files...").clicked() {
                    if let Some(paths) = FileDialog::new()
                        .add_filter("CSV", &["csv"])
                        .pick_files()
                    {
                        for path in paths {
                            self.load_csv_file(path);
                        }
                    }
                }
                
                if ui.button("➕ Add Folder...").clicked() {
                    if let Some(folder) = FileDialog::new().pick_folder() {
                        self.load_csv_folder(folder);
                    }
                }
                
                if !self.csv_files.is_empty() && ui.button("🗑 Clear All").clicked() {
                    self.csv_files.clear();
                }
            });
            
            ui.separator();
            
            // Error display
            if let Some(error) = &self.error {
                ui.colored_label(egui::Color32::from_rgb(255, 100, 100), format!("❌ Error: {}", error));
                ui.separator();
            }
            
            // CSV files list
            if !self.csv_files.is_empty() {
                ScrollArea::vertical()
                    .max_height(400.0)
                    .show(ui, |ui| {
                        for (idx, csv_config) in self.csv_files.iter_mut().enumerate() {
                            ui.push_id(idx, |ui| {
                                Self::render_csv_config(ui, csv_config);
                                ui.separator();
                            });
                        }
                    });
            } else {
                ui.centered_and_justified(|ui| {
                    ui.label("Add CSV files to import");
                });
            }
            
            ui.separator();
            
            // Action buttons
            ui.horizontal(|ui| {
                let can_create = self.database_path.is_some() && 
                    !self.csv_files.is_empty() && 
                    self.csv_files.iter().any(|f| f.include);
                
                ui.add_enabled_ui(can_create, |ui| {
                    if ui.button("Create Database").clicked() {
                        if let Some(path) = self.create_database() {
                            *created_db_path = Some(path);
                            self.show = false;
                        }
                    }
                });
                
                if ui.button("Cancel").clicked() {
                    self.show = false;
                }
            });
        });
    }
    
    fn render_csv_config(ui: &mut egui::Ui, config: &mut CsvFileConfig) {
        ui.collapsing(config.path.file_name().unwrap_or_default().to_string_lossy(), |ui| {
            ui.horizontal(|ui| {
                ui.checkbox(&mut config.include, "Include");
                ui.separator();
                ui.label("Table name:");
                ui.text_edit_singleline(&mut config.table_name);
            });
            
            if config.include {
                ui.separator();
                
                // Column configuration
                ui.label("Columns:");
                egui::Grid::new("columns")
                    .num_columns(4)
                    .spacing([10.0, 4.0])
                    .striped(true)
                    .show(ui, |ui| {
                        ui.strong("Column");
                        ui.strong("Detected Type");
                        ui.strong("Override Type");
                        ui.strong("Index");
                        ui.end_row();
                        
                        for header in &config.headers {
                            ui.label(header);
                            
                            if let Some(detected_type) = config.column_types.get(header) {
                                ui.label(format!("{:?}", detected_type));
                                
                                let mut current_type = detected_type.clone();
                                egui::ComboBox::from_id_salt(format!("type_{}_{}", config.table_name, header))
                                    .selected_text(format!("{:?}", current_type))
                                    .show_ui(ui, |ui| {
                                        ui.selectable_value(&mut current_type, ColumnType::Integer, "Integer");
                                        ui.selectable_value(&mut current_type, ColumnType::Real, "Real");
                                        ui.selectable_value(&mut current_type, ColumnType::Text, "Text");
                                        ui.selectable_value(&mut current_type, ColumnType::Boolean, "Boolean");
                                        ui.selectable_value(&mut current_type, ColumnType::Date, "Date");
                                        ui.selectable_value(&mut current_type, ColumnType::DateTime, "DateTime");
                                    });
                                
                                if current_type != *detected_type {
                                    config.column_types.insert(header.clone(), current_type);
                                }
                                
                                let create_index = config.create_indexes.entry(header.clone()).or_insert(false);
                                ui.checkbox(create_index, "");
                            }
                            
                            ui.end_row();
                        }
                    });
            }
        });
    }
    
    fn load_csv_file(&mut self, path: PathBuf) {
        // Check if already loaded
        if self.csv_files.iter().any(|f| f.path == path) {
            return;
        }
        
        // Suggest table name from filename
        let table_name = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table")
            .to_string();
        
        match CsvReader::from_path(&path) {
            Ok(mut reader) => {
                match reader.headers() {
                    Ok(headers) => {
                        // Read sample data
                        match reader.sample_records(100) {
                            Ok(records) => {
                                let sample_data: Vec<Vec<String>> = records.into_iter()
                                    .map(|r| r.iter().map(|s| s.to_string()).collect())
                                    .collect();
                                
                                // Infer column types
                                let inferred = TypeInferrer::infer_column_types(&headers, &sample_data);
                                let column_types = inferred.into_iter().collect();
                                
                                let config = CsvFileConfig {
                                    path,
                                    table_name,
                                    headers,
                                    column_types,
                                    sample_data,
                                    create_indexes: IndexMap::new(),
                                    include: true,
                                };
                                
                                self.csv_files.push(config);
                            }
                            Err(e) => self.error = Some(format!("Failed to read sample data: {}", e)),
                        }
                    }
                    Err(e) => self.error = Some(format!("Failed to read headers: {}", e)),
                }
            }
            Err(e) => self.error = Some(format!("Failed to open CSV: {}", e)),
        }
    }
    
    fn load_csv_folder(&mut self, folder: PathBuf) {
        if let Ok(entries) = std::fs::read_dir(&folder) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
                    self.load_csv_file(path);
                }
            }
        }
    }
    
    fn create_database(&mut self) -> Option<PathBuf> {
        let db_path = self.database_path.as_ref()?;
        
        match Database::open_writable(db_path) {
            Ok(mut db) => {
                // Process each included CSV file
                for config in &self.csv_files {
                    if !config.include {
                        continue;
                    }
                    
                    // Create table
                    let columns: Vec<(&str, &str)> = config.headers.iter()
                        .map(|header| {
                            let col_type = config.column_types.get(header)
                                .unwrap_or(&ColumnType::Text);
                            (header.as_str(), col_type.to_sql_type())
                        })
                        .collect();
                    
                    if let Err(e) = db.create_table(&config.table_name, &columns) {
                        self.error = Some(format!("Failed to create table {}: {}", config.table_name, e));
                        return None;
                    }
                    
                    // Note: Index creation removed - DuckDB automatically creates indexes as needed
                    
                    // Import data
                    if let Ok(mut reader) = CsvReader::from_path(&config.path) {
                        if let Ok(records) = reader.records() {
                            if let Err(e) = db.begin_transaction() {
                                self.error = Some(format!("Failed to begin transaction: {}", e));
                                return None;
                            }
                            
                            for record in records {
                                let values: Vec<String> = record.iter().map(|s| s.to_string()).collect();
                                if let Err(e) = db.insert_record(&config.table_name, &values) {
                                    self.error = Some(format!("Failed to insert record: {}", e));
                                    let _ = db.rollback_transaction();
                                    return None;
                                }
                            }
                            
                            if let Err(e) = db.commit_transaction() {
                                self.error = Some(format!("Failed to commit transaction: {}", e));
                                let _ = db.rollback_transaction();
                                return None;
                            }
                        }
                    }
                }
                
                Some(db_path.clone())
            }
            Err(e) => {
                self.error = Some(format!("Failed to create database: {}", e));
                None
            }
        }
    }
} 