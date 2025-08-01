use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use egui::{Context, Id};
use crate::core::{Database, CsvReader};
use crate::infer::{TypeInferrer, ColumnType};

#[derive(Clone)]
pub struct FileConfig {
    pub path: PathBuf,
    pub table_name: String,
    pub header_row: usize,
    pub delimiter: char,
    pub sample_size: usize,
    pub columns: Vec<ColumnConfig>,
    pub null_values: Vec<String>,
    pub preview_data: Option<PreviewData>,
}

#[derive(Clone)]
pub struct ColumnConfig {
    pub name: String,
    pub data_type: ColumnType,
    pub included: bool,
}

#[derive(Clone)]
pub struct PreviewData {
    pub rows: Vec<Vec<String>>,
    pub original_row_numbers: Vec<usize>,
}

impl FileConfig {
    pub fn new(path: PathBuf) -> Self {
        let table_name = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table")
            .to_string();
        
        Self {
            path,
            table_name,
            header_row: 1, // Default to row 1 (1-indexed) instead of 0
            delimiter: ',',
            sample_size: 1000,
            columns: Vec::new(),
            null_values: vec!["", "NULL", "null", "N/A", "-"].into_iter().map(String::from).collect(),
            preview_data: None,
        }
    }
    
    pub fn file_name(&self) -> String {
        self.path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string()
    }
}

pub struct FileConfigDialog {
    pub show: bool,
    pub database_path: Option<PathBuf>,
    pub files: Vec<FileConfig>,
    pub current_file_index: usize,
    pub create_database: bool,
    
    // UI state
    null_value_input: String,
    pub error: Option<String>,
    processing_state: Arc<Mutex<ProcessingState>>,
    needs_resampling: bool,
}

#[derive(Clone)]
pub enum ProcessingState {
    Idle,
    Loading(f32, String),
    Processing(String, f32),
    Complete,
    Error(String),
}

impl FileConfigDialog {
    // Helper function to parse CSV line properly, handling quoted strings
    fn parse_csv_line(line: &str, delimiter: char) -> Vec<String> {
        let mut row = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();
        
        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        // End of quoted field
                        in_quotes = false;
                    } else {
                        // Start of quoted field
                        in_quotes = true;
                    }
                },
                c if c == delimiter && !in_quotes => {
                    // End of field
                    row.push(current_field.trim().to_string());
                    current_field.clear();
                },
                _ => {
                    current_field.push(ch);
                }
            }
        }
        
        // Add the last field
        row.push(current_field.trim().to_string());
        row
    }

    /// Infer the most likely delimiter from a header line
    fn infer_delimiter_from_header(header_line: &str) -> char {
        let delimiters = [',', '\t', ';', '|'];
        let mut best_delimiter = ',';
        let mut max_fields = 0;
        
        for &delimiter in &delimiters {
            let fields = Self::parse_csv_line(header_line, delimiter);
            if fields.len() > max_fields && fields.len() > 1 {
                max_fields = fields.len();
                best_delimiter = delimiter;
            }
        }
        
        best_delimiter
    }

    /// Get the display name for a delimiter
    fn delimiter_display_name(delimiter: char) -> &'static str {
        match delimiter {
            ',' => "Comma",
            '\t' => "Tab", 
            ';' => "Semicolon",
            '|' => "Pipe",
            _ => "Unknown"
        }
    }
    
    pub fn new() -> Self {
        Self {
            show: false,
            database_path: None,
            files: Vec::new(),
            current_file_index: 0,
            create_database: false,
            null_value_input: String::new(),
            error: None,
            processing_state: Arc::new(Mutex::new(ProcessingState::Idle)),
            needs_resampling: false,
        }
    }
    
    pub fn open(&mut self, path: PathBuf) {
        self.database_path = Some(path);
        self.show = true;
        self.create_database = false;
        self.files.clear();
        self.current_file_index = 0;
    }
    
    pub fn open_with_csv_selection(&mut self) {
        // First, let user select CSV files
        if let Some(csv_files) = rfd::FileDialog::new()
            .add_filter("CSV files", &["csv"])
            .set_title("Select CSV files to import")
            .pick_files()
        {
            if !csv_files.is_empty() {
                // Reset dialog state
                self.reset();
                
                // Set default database path in Documents folder
                let default_db_path = if let Some(docs_dir) = dirs::document_dir() {
                    docs_dir.join("fresh_project")
                } else {
                    PathBuf::from("fresh_project")
                };
                
                // Create the project folder structure immediately
                if let Err(e) = std::fs::create_dir_all(&default_db_path) {
                    eprintln!("Warning: Could not create project folder: {}", e);
                }
                
                self.database_path = Some(default_db_path);
                self.show = true;
                self.create_database = false;
                self.files.clear();
                self.current_file_index = 0;
                
                // Add all selected CSV files
                for csv_path in csv_files {
                    self.add_file(csv_path);
                }
            }
        }
    }
    
    fn reset(&mut self) {
        self.files.clear();
        self.current_file_index = 0;
        self.error = None;
        self.null_value_input.clear();
        self.needs_resampling = false;
        if let Ok(mut state) = self.processing_state.lock() {
            *state = ProcessingState::Idle;
        }
    }
    
    pub fn add_file(&mut self, path: PathBuf) {
        let config = FileConfig::new(path);
        self.files.push(config);
        self.current_file_index = self.files.len() - 1;
        self.load_preview_for_current_file();
    }
    
    pub fn show(&mut self, ctx: &Context) -> Option<PathBuf> {
        if !self.show {
            return None;
        }
        
        let mut created_db_path = None;
        
        // Check processing state first
        let current_state = if let Ok(state) = self.processing_state.lock() {
            match &*state {
                ProcessingState::Idle => ProcessingState::Idle,
                ProcessingState::Processing(msg, progress) => {
                    ctx.request_repaint();
                    ProcessingState::Processing(msg.clone(), *progress)
                }
                ProcessingState::Complete => {
                    created_db_path = self.database_path.clone();
                    self.show = false;
                    ProcessingState::Complete
                }
                ProcessingState::Error(error_msg) => {
                    self.error = Some(error_msg.clone());
                    ProcessingState::Error(error_msg.clone())
                }
                ProcessingState::Loading(progress, msg) => {
                    ProcessingState::Loading(*progress, msg.clone())
                }
            }
        } else {
            ProcessingState::Idle
        };
        
        // Reset state after Complete or Error
        match current_state {
            ProcessingState::Complete | ProcessingState::Error(_) => {
                if let Ok(mut state) = self.processing_state.lock() {
                    *state = ProcessingState::Idle;
                }
                if matches!(current_state, ProcessingState::Complete) {
                    return created_db_path;
                }
            }
            _ => {}
        }
        
        // Show progress overlay if processing
        if let ProcessingState::Processing(message, progress) = &current_state {
            egui::Area::new(egui::Id::new("progress_overlay"))
                .fixed_pos(egui::pos2(0.0, 0.0))
                .show(ctx, |ui| {
                    let screen_rect = ctx.screen_rect();
                    ui.painter().rect_filled(
                        screen_rect,
                        0.0,
                        egui::Color32::from_black_alpha(120)
                    );
                });
            egui::Window::new("Processing")
                .collapsible(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .fixed_size([400.0, 200.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(20.0);
                        ui.heading("Creating Database");
                        ui.add_space(20.0);
                        let progress_bar = egui::ProgressBar::new(*progress)
                            .text(format!("{:.0}%", progress * 100.0))
                            .desired_width(350.0);
                        ui.add(progress_bar);
                        ui.add_space(10.0);
                        ui.label(egui::RichText::new(message).size(14.0));
                        ui.add_space(20.0);
                        ui.label(egui::RichText::new("Please wait...").size(12.0).color(egui::Color32::from_gray(150)));
                    });
                });
            return None;
        }
        
        // Show main dialog only if not processing
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Create Project from CSVs");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("✖").clicked() {
                        self.show = false;
                    }
                });
            });
            ui.separator();
            self.render_content(ui);
        });
        
        // Handle database creation after UI rendering
        if self.create_database {
            if let Some(path) = self.start_database_creation() {
                self.show = false;
                return Some(path);
            }
            self.create_database = false;
        }
        created_db_path
    }
    
    fn render_content(&mut self, ui: &mut egui::Ui) {
        // Use vertical layout with bottom panel for buttons
        egui::TopBottomPanel::bottom("bottom_buttons")
            .show_inside(ui, |ui| {
                ui.add_space(10.0);
                ui.separator();
                ui.horizontal(|ui| {
                    if ui.button("Cancel").clicked() {
                        self.show = false;
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let can_create = self.database_path.is_some() && 
                            !self.files.is_empty() && 
                            self.files.iter().all(|f| !f.table_name.is_empty() && 
                                f.columns.iter().any(|c| c.included));
                        let create_button = egui::Button::new(
                            egui::RichText::new(format!("✅ Create Project"))
                                .size(16.0)
                                .color(egui::Color32::WHITE)
                        )
                        .fill(egui::Color32::from_rgb(76, 175, 80))
                        .rounding(egui::Rounding::same(6.0));
                        if ui.add_enabled(can_create, create_button).clicked() {
                            if let Some(error) = self.validate_constraints() {
                                self.error = Some(error);
                            } else {
                                self.create_database = true;
                            }
                        }
                        ui.add_space(10.0);
                        ui.label(
                            egui::RichText::new("💡 Configure each file's import settings before creating the project")
                                .size(12.0)
                                .color(egui::Color32::from_gray(150))
                        );
                    });
                });
                ui.add_space(5.0);
            });
        // Main content in central panel
        egui::CentralPanel::default()
            .show_inside(ui, |ui| {
                // Project folder section
                ui.group(|ui| {
                    ui.horizontal(|ui| {
                        ui.label("Project Folder:");
                        if let Some(path) = &self.database_path {
                            ui.label(path.display().to_string());
                        } else {
                            ui.label("No project folder selected");
                        }
                        if ui.button("Browse...").clicked() {
                            if let Some(path) = rfd::FileDialog::new()
                                .add_filter("Project Folder", &["*"])
                                .set_title("Select project folder...")
                                .pick_folder()
                            {
                                if let Err(e) = std::fs::create_dir_all(&path) {
                                    eprintln!("Warning: Could not create project folder: {}", e);
                                }
                                self.database_path = Some(path);
                            }
                        }
                    });
                });
                if let Some(error) = &self.error.clone() {
                    ui.horizontal(|ui| {
                        ui.colored_label(egui::Color32::from_rgb(255, 100, 100), format!("❌ {}", error));
                        if ui.small_button("✖").clicked() {
                            self.error = None;
                        }
                    });
                }
                ui.separator();
                let available_height = ui.available_height();
                ui.horizontal_top(|ui| {
                    // Left side - file configuration
                    ui.vertical(|ui| {
                        ui.set_width(500.0);
                        ui.set_height(available_height);
                        
                        // File selector dropdown
                        ui.horizontal(|ui| {
                            ui.label("CSV File:");
                            
                            let file_names: Vec<String> = self.files.iter()
                                .enumerate()
                                .map(|(_idx, config)| {
                                    let configured = !config.columns.is_empty();
                                    format!("{}{}", 
                                        config.file_name(),
                                        if configured { " ✓" } else { "" }
                                    )
                                })
                                .collect();
                            
                            if !file_names.is_empty() {
                                let current_text = file_names.get(self.current_file_index)
                                    .cloned()
                                    .unwrap_or_else(|| "No file selected".to_string());
                                
                                egui::ComboBox::new("file_selector_combo", "CSV File")
                                    .selected_text(&current_text)
                                    .show_ui(ui, |ui| {
                                        for (idx, name) in file_names.iter().enumerate() {
                                            if ui.selectable_value(&mut self.current_file_index, idx, name).clicked() {
                                                // Load preview for newly selected file
                                                self.load_preview_for_current_file();
                                            }
                                        }
                                    });
                            } else {
                                ui.label("No files added");
                            }
                            
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                if ui.button("Add Files...").clicked() {
                                    if let Some(paths) = rfd::FileDialog::new()
                                        .add_filter("CSV files", &["csv"])
                                        .set_title("Select CSV files")
                                        .pick_files()
                                    {
                                        for path in paths {
                                            self.add_file(path);
                                        }
                                    }
                                }
                            });
                        });
                        
                        ui.label(format!("Files to import: {} total, {} configured", 
                            self.files.len(),
                            self.files.iter().filter(|f| !f.columns.is_empty()).count()
                        ));
                        
                        ui.separator();
                        ui.add_space(5.0);
                        
                        if let Some(config) = self.files.get_mut(self.current_file_index) {
                            // Table name
                            ui.horizontal(|ui| {
                                ui.label("Table Name:");
                                ui.text_edit_singleline(&mut config.table_name);
                            });
                            
                            ui.add_space(10.0);
                            
                            // Header configuration
                            ui.group(|ui| {
                                ui.set_width(ui.available_width());
                                ui.label(egui::RichText::new("Header Configuration").size(16.0).strong());
                                ui.add_space(5.0);
                                
                                ui.horizontal(|ui| {
                                    ui.label("Header Row:");
                                    
                                    // Convert to 1-indexed for display
                                    let mut header_row_display = config.header_row; // 1-indexed for display
                                    
                                    // Always show 1-50 range for header row selection
                                    let max_rows = 50;
                                    
                                    let response = ui.add(
                                        egui::DragValue::new(&mut header_row_display)
                                            .range(1..=max_rows)
                                            .speed(1)
                                    );
                                    
                                    if response.changed() {
                                        config.header_row = header_row_display; // 1-indexed
                                        // Trigger resampling
                                        self.needs_resampling = true;
                                    }
                                    
                                    ui.label("(1-50)");
                                });
                                
                                ui.add_space(5.0);
                                ui.label(
                                    egui::RichText::new("The green highlighted row in the preview is your header")
                                        .size(12.0)
                                        .color(egui::Color32::from_gray(150))
                                );
                            });
                            
                            ui.add_space(10.0);
                            
                            // Sample size
                            ui.horizontal(|ui| {
                                ui.label("Sample Size:");
                                let response = ui.add(
                                    egui::DragValue::new(&mut config.sample_size)
                                        .range(100..=10000)
                                        .speed(10)
                                );
                                if response.changed() {
                                    self.needs_resampling = true;
                                }
                                ui.label("rows");
                                
                                if ui.button("🔄 Resample").clicked() {
                                    self.needs_resampling = true;
                                }
                            });
                            
                            ui.add_space(10.0);
                            
                            // Delimiter
                            ui.horizontal(|ui| {
                                ui.label("Delimiter:");
                                
                                // Try to infer delimiter from header if we have preview data
                                let inferred_delimiter = if let Some(ref preview) = config.preview_data {
                                    if let Some(header_line) = preview.rows.get(config.header_row.saturating_sub(1)) {
                                        if let Some(header_str) = header_line.get(0) {
                                            Self::infer_delimiter_from_header(header_str)
                                        } else {
                                            ','
                                        }
                                    } else {
                                        ','
                                    }
                                } else {
                                    ','
                                };
                                
                                // Show inferred delimiter if it's different from current selection
                                if inferred_delimiter != config.delimiter {
                                    ui.label(format!("Inferred: {}", Self::delimiter_display_name(inferred_delimiter)));
                                    if ui.button("Use Inferred").clicked() {
                                        config.delimiter = inferred_delimiter;
                                    }
                                }
                                
                                ui.radio_value(&mut config.delimiter, ',', "Comma");
                                ui.radio_value(&mut config.delimiter, '\t', "Tab");
                                ui.radio_value(&mut config.delimiter, ';', "Semicolon");
                                ui.radio_value(&mut config.delimiter, '|', "Pipe");
                            });
                            
                            ui.add_space(10.0);
                            
                            // Null values
                            ui.group(|ui| {
                                ui.set_width(ui.available_width());
                                ui.label(egui::RichText::new("Null Values").size(14.0));
                                ui.label(egui::RichText::new("Values to treat as NULL:").size(12.0));
                                
                                egui::ScrollArea::vertical()
                                    .id_salt(format!("null_scroll_{}", self.current_file_index))
                                    .max_height(100.0)
                                    .show(ui, |ui| {
                                        let mut to_remove = None;
                                        for (idx, pattern) in config.null_values.iter().enumerate() {
                                            ui.horizontal(|ui| {
                                                if ui.small_button("×").clicked() {
                                                    to_remove = Some(idx);
                                                }
                                                ui.label(if pattern.is_empty() { "[empty string]" } else { pattern });
                                            });
                                        }
                                        if let Some(idx) = to_remove {
                                            config.null_values.remove(idx);
                                        }
                                    });
                                
                                ui.horizontal(|ui| {
                                    ui.text_edit_singleline(&mut self.null_value_input);
                                    if ui.button("Add").clicked() && !self.null_value_input.trim().is_empty() {
                                        config.null_values.push(self.null_value_input.clone());
                                        self.null_value_input.clear();
                                    }
                                });
                            });
                            
                            ui.add_space(10.0);
                            
                            // Column selection
                            ui.group(|ui| {
                                ui.set_width(ui.available_width());
                                ui.label(egui::RichText::new("Column Selection").size(14.0));
                                
                                if !config.columns.is_empty() {
                                    let selected_count = config.columns.iter()
                                        .filter(|c| c.included)
                                        .count();
                                    
                                    ui.horizontal(|ui| {
                                        if ui.button("Select All").clicked() {
                                            for col in &mut config.columns {
                                                col.included = true;
                                            }
                                        }
                                        if ui.button("Deselect All").clicked() {
                                            for col in &mut config.columns {
                                                col.included = false;
                                            }
                                        }
                                        ui.label(format!("{} / {} selected", selected_count, config.columns.len()));
                                    });
                                    
                                    ui.separator();
                                    
                                    let available_height = ui.available_height();
                                    egui::ScrollArea::vertical()
                                        .id_salt(format!("column_scroll_{}", self.current_file_index))
                                        .max_height(available_height)
                                        .show(ui, |ui| {
                                            use egui_extras::{TableBuilder, Column};
                                            
                                            TableBuilder::new(ui)
                                                .striped(true)
                                                .resizable(true)
                                                .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                                                .column(Column::auto().at_least(60.0)) // Include
                                                .column(Column::auto().at_least(100.0).resizable(true)) // Column
                                                .column(Column::auto().at_least(100.0)) // Type
                                                .header(20.0, |mut header| {
                                                    header.col(|ui| {
                                                        ui.label(egui::RichText::new("Include").strong());
                                                    });
                                                    header.col(|ui| {
                                                        ui.label(egui::RichText::new("Column").strong());
                                                    });
                                                    header.col(|ui| {
                                                        ui.label(egui::RichText::new("Type").strong());
                                                    });
                                                })
                                                .body(|mut body| {
                                                    for (col_idx, col) in config.columns.iter_mut().enumerate() {
                                                        body.row(25.0, |mut row| {
                                                            row.col(|ui| {
                                                                ui.checkbox(&mut col.included, "");
                                                            });
                                                            row.col(|ui| {
                                                                ui.label(&col.name);
                                                            });
                                                            row.col(|ui| {
                                                                // Type dropdown
                                                                let old_type = col.data_type.clone();
                                                                let mut new_type = col.data_type.clone();
                                                                egui::ComboBox::new(format!("type_{}_{}", self.current_file_index, col_idx), "")
                                                                    .selected_text(col.data_type.display_name())
                                                                    .width(120.0)
                                                                    .show_ui(ui, |ui| {
                                                                        ui.set_max_height(200.0); // Force dropdown to open downward
                                                                        ui.selectable_value(&mut new_type, ColumnType::Text, "Text");
                                                                        ui.selectable_value(&mut new_type, ColumnType::Integer, "Integer (64-bit)");
                                                                        ui.selectable_value(&mut new_type, ColumnType::Real, "Float (64-bit)");
                                                                        ui.selectable_value(&mut new_type, ColumnType::Boolean, "Boolean");
                                                                        ui.selectable_value(&mut new_type, ColumnType::Date, "Date");
                                                                        ui.selectable_value(&mut new_type, ColumnType::DateTime, "Time");
                                                                        ui.selectable_value(&mut new_type, ColumnType::TimeSeconds, "Time (seconds)");
                                                                        ui.selectable_value(&mut new_type, ColumnType::TimeMilliseconds, "Time (milliseconds)");
                                                                        ui.selectable_value(&mut new_type, ColumnType::TimeMicroseconds, "Time (microseconds)");
                                                                        ui.selectable_value(&mut new_type, ColumnType::TimeNanoseconds, "Time (nanoseconds)");
                                                                    });
                                                                // Only validate and update if the type actually changed
                                                                if new_type != old_type {
                                                                    let preview_data = config.preview_data.clone();
                                                                    let header_row = config.header_row;
                                                                    let sample_size = config.sample_size;
                                                                    if let Some(preview) = &preview_data {
                                                                        let sample_data: Vec<Vec<String>> = preview.rows.iter()
                                                                            .skip(header_row + 1)
                                                                            .take(sample_size)
                                                                            .cloned()
                                                                            .collect();
                                                                        if let Err(validation_error) = TypeInferrer::validate_column_type_change(
                                                                            &sample_data,
                                                                            col_idx,
                                                                            &new_type
                                                                        ) {
                                                                            // Show error, do not update col.data_type
                                                                            self.error = Some(validation_error);
                                                                        } else {
                                                                            col.data_type = new_type;
                                                                        }
                                                                    } else {
                                                                        // If no preview, just update
                                                                        col.data_type = new_type;
                                                                    }
                                                                }
                                                            });
                                                        });
                                                    }
                                                });
                                        });
                                }
                            });
                            
                            // Note: Primary key handling removed since it's not relevant for Arrow/DataFusion
                        }
                    }); // End left column
                    // Right side - data preview
                    ui.vertical(|ui| {
                        ui.set_height(available_height);
                        ui.label(egui::RichText::new("Data Preview").size(16.0).strong());
                        ui.add_space(8.0);
                        
                        let preview_height = ui.available_height();
                        
                        if let Some(config) = self.files.get(self.current_file_index) {
                            if let Some(preview) = &config.preview_data {
                                egui::ScrollArea::both()
                                    .id_salt(format!("preview_scroll_{}", self.current_file_index))
                                    .max_height(preview_height)
                                    .show(ui, |ui| {
                                        // Use TableBuilder for proper vertical separators
                                        use egui_extras::{TableBuilder, Column};
                                        
                                        // Calculate number of columns (row number + data columns)
                                        let max_columns = preview.rows.iter()
                                            .map(|row| row.len())
                                            .max()
                                            .unwrap_or(0);
                                        let num_columns = max_columns + 1; // +1 for row number column
                                        

                                        
                                        TableBuilder::new(ui)
                                            .striped(true)
                                            .resizable(true)
                                            .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                                            .column(Column::auto().at_least(40.0)) // Row number column
                                            .columns(Column::auto().at_least(100.0).resizable(true), num_columns - 1) // Data columns
                                            .vscroll(false) // We're already in a scroll area
                                            .body(|mut body| {
                                                for (row_idx, row) in preview.rows.iter().enumerate() {
                                                    let is_header = row_idx == config.header_row.saturating_sub(1); // Convert 1-indexed to 0-indexed
                                                    let color = if is_header {
                                                        egui::Color32::from_rgb(100, 200, 100)
                                                    } else {
                                                        egui::Color32::from_gray(200)
                                                    };
                                                    
                                                    // Show actual file row numbers (1-indexed)
                                                    let file_row_number = row_idx + 1; // 1-indexed file row numbers
                                                    
                                                    body.row(20.0, |mut table_row| {
                                                        // Row number
                                                        table_row.col(|ui| {
                                                            let row_text = egui::RichText::new(file_row_number.to_string())
                                                                .color(if is_header { color } else { egui::Color32::from_gray(150) });
                                                            ui.label(if is_header { row_text.strong() } else { row_text });
                                                        });
                                                        
                                                        // Row data
                                                        for (col_idx, cell) in row.iter().enumerate() {
                                                            table_row.col(|ui| {
                                                                let cell_text = egui::RichText::new(cell)
                                                                    .color(if is_header { color } else { egui::Color32::from_gray(200) });
                                                                ui.label(if is_header { cell_text.strong() } else { cell_text });
                                                            });
                                                        }
                                                        
                                                        // Fill remaining columns with empty cells if this row has fewer columns
                                                        for _ in row.len()..max_columns {
                                                            table_row.col(|ui| {
                                                                ui.label("");
                                                            });
                                                        }
                                                    });
                                                }
                                            });
                                    });
                            } else {
                                ui.centered_and_justified(|ui| {
                                    ui.label("Loading preview...");
                                });
                            }
                        } else {
                            ui.centered_and_justified(|ui| {
                                ui.label("No file selected");
                            });
                        }
                    }); // End right column
                }); // End horizontal_top
            }); // End CentralPanel
        if self.needs_resampling {
            self.needs_resampling = false;
            self.load_preview_for_current_file();
        }
    }
    
    pub fn load_preview_for_current_file(&mut self) {
        if let Some(config) = self.files.get_mut(self.current_file_index) {
            let path = config.path.clone();
            let delimiter = config.delimiter;
            let sample_size = config.sample_size;
            let header_row = config.header_row;
            
                                // Load preview data from the beginning of the file (first 50 rows)
                    match std::fs::read_to_string(&path) {
                        Ok(content) => {
                            let lines: Vec<&str> = content.lines().collect();
                            
                            // Take more lines to ensure we get 50 valid rows for preview
                            let data_lines = lines.into_iter().take(200).collect::<Vec<&str>>();
                    
                    if data_lines.is_empty() {
                        self.error = Some("No data found in file".to_string());
                        return;
                    }
                    
                    // Show raw file content for preview (first 50 lines)
                    let mut preview_rows: Vec<Vec<String>> = Vec::new();
                    let mut valid_data_rows = Vec::new();
                    
                    // Collect raw lines for preview (up to 50 lines)
                    for (line_idx, line) in data_lines.iter().enumerate() {
                        // Parse the line to show as individual cells
                        let row = Self::parse_csv_line(line, delimiter);
                        preview_rows.push(row.clone());
                        
                        // Also collect valid data rows for type inference (filtered)
                        if !line.trim().starts_with('#') {
                            let is_garbage = row.iter().all(|cell| cell.is_empty()) ||
                                           row.iter().any(|cell| {
                                               let cell_lower = cell.to_lowercase();
                                               cell_lower.contains("fake line") ||
                                               cell_lower.contains("ignore this") ||
                                               cell_lower.contains("data starts here") ||
                                               cell_lower.contains("realistic patterns")
                                           });
                            
                            if !is_garbage && valid_data_rows.len() < sample_size {
                                valid_data_rows.push(row);
                            }
                        }
                        
                        // Stop after 50 lines for preview
                        if preview_rows.len() >= 50 {
                            break;
                        }
                    }
                    
                    // Get the header row from the selected header row position (1-indexed to 0-indexed)
                    let header_idx = header_row.saturating_sub(1);
                    if header_idx < preview_rows.len() {
                        let headers = preview_rows[header_idx].clone();
                        
                        // Use data rows after the header row for type inference (not filtered)
                        let mut sample_data = Vec::new();
                        for (idx, row) in preview_rows.iter().enumerate() {
                            if idx > header_idx && sample_data.len() < sample_size {
                                // Skip comment lines (lines starting with #)
                                if !row.is_empty() && !row[0].starts_with('#') {
                                    sample_data.push(row.clone());
                                }
                            }
                        }
                        
                        // Infer types using the sample data with null value awareness
                        let inferred_types = TypeInferrer::infer_column_types_with_nulls(&headers, &sample_data, &config.null_values);
                        
                        // Update columns
                        config.columns.clear();
                        for (header, (_name, data_type)) in headers.iter().zip(inferred_types.iter()) {
                            config.columns.push(ColumnConfig {
                                name: header.clone(),
                                data_type: data_type.clone(),
                                included: true,
                            });
                        }
                    }
                    
                    config.preview_data = Some(PreviewData { 
                        rows: preview_rows,
                        original_row_numbers: Vec::new(), // Not used anymore
                    });
                }
                Err(e) => {
                    self.error = Some(format!("Failed to load preview: {}", e));
                }
            }
        }
    }
    
    fn start_database_creation(&mut self) -> Option<PathBuf> {
        let db_path = self.database_path.clone()?;
        let mut files = self.files.clone();
        
        // For DataFusion, we need to create the database synchronously
        // since it's in-memory and we need to return the actual database instance
        match Database::open_writable(&db_path) {
            Ok(mut db) => {
                let total_files = files.len();
                
                // Create all tables first
                // Skip pre-creating tables - let stream_insert_csv handle schema creation from CSV headers
                
                // Import data for each file
                for (file_idx, config) in files.iter_mut().enumerate() {
                    // Use enhanced streaming import with custom header row
                    match db.stream_insert_csv_with_header_row(&config.table_name, &config.path, config.delimiter, config.header_row.saturating_sub(1)) {
                        Ok(inferred_delimiter) => {
                            // Update the config with the inferred delimiter if it was auto-detected
                            if config.delimiter == ',' {
                                config.delimiter = inferred_delimiter;
                            }
                        }
                        Err(e) => {
                            self.error = Some(format!("Failed to import {}: {}", config.file_name(), e));
                            return None;
                        }
                    }
                }
                
                // Save all tables to persistence (Arrow IPC format) directly in the project folder
                if let Err(e) = db.save_all_tables(&db_path) {
                    self.error = Some(format!("Failed to save tables to persistence: {}", e));
                    return None;
                }
                
                // Return the path for the app to load
                Some(db_path)
            }
            Err(e) => {
                self.error = Some(format!("Failed to create database: {}", e));
                None
            }
        }
    }
    
    fn create_database_in_thread(
        db_path: PathBuf,
        mut files: Vec<FileConfig>,
        processing_state: Arc<Mutex<ProcessingState>>
    ) {
        // Update state to processing
        if let Ok(mut state) = processing_state.lock() {
            *state = ProcessingState::Processing("Initializing DataFusion context...".to_string(), 0.0);
        }
        
        match Database::open_writable(&db_path) {
            Ok(mut db) => {
                let total_files = files.len();
                
                // DataFusion is in-memory, so no need for pragmas or transactions
                // Just proceed with table creation and data import
                
                // Skip pre-creating tables - let stream_insert_csv handle schema creation from CSV headers
                
                // Now import data for each file
                for (file_idx, config) in files.iter_mut().enumerate() {
                    // Update initial progress for this file
                    if let Ok(mut state) = processing_state.lock() {
                        *state = ProcessingState::Processing(
                            format!("Importing {} ({}/{})", config.file_name(), file_idx + 1, total_files),
                            0.2 + (0.7 * file_idx as f32 / total_files as f32)
                        );
                    }
                    
                    // Use enhanced streaming import with custom header row
                    match db.stream_insert_csv_with_header_row(&config.table_name, &config.path, config.delimiter, config.header_row.saturating_sub(1)) {
                        Ok(inferred_delimiter) => {
                            // Update the config with the inferred delimiter if it was auto-detected
                            if config.delimiter == ',' {
                                config.delimiter = inferred_delimiter;
                            }
                        }
                        Err(e) => {
                            if let Ok(mut state) = processing_state.lock() {
                                *state = ProcessingState::Error(format!("Failed to import {}: {}", config.file_name(), e));
                            }
                            let _ = db.rollback_transaction();
                            return;
                        }
                    }
                    
                    // Final progress update for this file
                    if let Ok(mut state) = processing_state.lock() {
                        let overall_progress = 0.2 + (0.7 * (file_idx + 1) as f32 / total_files as f32);
                        *state = ProcessingState::Processing(
                            format!("Completed {}", config.file_name()),
                            overall_progress
                        );
                    }
                }
                
                // eprintln!("All files imported, creating indexes...");
                
                // Note: DataFusion is in-memory, so no indexes or transactions needed
                if let Ok(mut state) = processing_state.lock() {
                    *state = ProcessingState::Processing("Saving tables to persistence...".to_string(), 0.9);
                }
                
                // Save all tables to persistence (Arrow IPC format) directly in the project folder
                if let Err(e) = db.save_all_tables(&db_path) {
                    eprintln!("[FileConfig] Warning: Failed to save tables to persistence: {}", e);
                    // Don't fail the entire operation, just log the warning
                } else {
                    eprintln!("[FileConfig] Successfully saved {} tables to persistence", 
                             std::fs::read_dir(&db_path).map(|entries| entries.count()).unwrap_or(0));
                }
                
                // eprintln!("DataFusion context finalized successfully");
                
                // Explicitly drop the database connection to ensure it's closed
                drop(db);
                
                if let Ok(mut state) = processing_state.lock() {
                    *state = ProcessingState::Complete;
                    // eprintln!("State set to Complete");
                }
            }
            Err(e) => {
                // eprintln!("Failed to create database: {}", e);
                if let Ok(mut state) = processing_state.lock() {
                    *state = ProcessingState::Error(format!("Failed to create database: {}", e));
                }
            }
        }
        
        // eprintln!("create_database_in_thread finished");
    }

    fn validate_constraints(&self) -> Option<String> {
        for config in &self.files {
            // Validate table name
            if config.table_name.is_empty() {
                return Some(format!("File '{}' has an empty table name", config.file_name()));
            }
            
            // Check for invalid characters in table name
            if !config.table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Some(format!(
                    "Table name '{}' contains invalid characters. Use only letters, numbers, and underscores.",
                    config.table_name
                ));
            }
            
            // Check that at least one column is included
            if !config.columns.iter().any(|c| c.included) {
                return Some(format!(
                    "Table '{}' has no columns selected. Select at least one column to include.",
                    config.table_name
                ));
            }
        }
        
        // Check for duplicate table names
        let mut table_names = std::collections::HashSet::new();
        for config in &self.files {
            if !table_names.insert(&config.table_name) {
                return Some(format!(
                    "Duplicate table name '{}'. Each table must have a unique name.",
                    config.table_name
                ));
            }
        }
        
        None
    }
} 