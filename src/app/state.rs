use egui::{Context, Id};
use crate::core::{Database, TableInfo, ComputedColumnsProcessor, EnhancedGroupingProcessor};
use crate::ui::{Sidebar, SidebarAction, QueryWindow, CsvImportDialog, FileConfigDialog, HomeScreen, DuplicateDetectionDialog, DuplicateResultsViewer, TransformationDialog, TransformationManager, TimeBinDialog, ComputedColumnsDialog, EnhancedGroupingDialog};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AppMode {
    Viewer,
    Builder,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HomeAction {
    OpenProject,
    CreateProject,
}

pub struct LeafApp {
    mode: AppMode,
    database: Option<Arc<Database>>,
    database_path: Option<std::path::PathBuf>,
    tables: Vec<TableInfo>,
    views: Vec<String>,
    sidebar: Sidebar,
    home_screen: HomeScreen,
    query_windows: Vec<QueryWindow>,
    csv_import_dialog: Option<CsvImportDialog>,
    file_config_dialog: FileConfigDialog,
    duplicate_detection_dialog: DuplicateDetectionDialog,
    duplicate_results_viewer: DuplicateResultsViewer,
    transformation_dialog: TransformationDialog,
    transformation_manager: TransformationManager,
    computed_columns_dialog: ComputedColumnsDialog,
    computed_columns_processor: ComputedColumnsProcessor,
    enhanced_grouping_dialog: EnhancedGroupingDialog,
    enhanced_grouping_processor: EnhancedGroupingProcessor,
    time_bin_dialog: TimeBinDialog,
    next_window_id: usize,
    error: Option<String>,
}

impl LeafApp {
    pub fn new() -> Self {
        Self {
            mode: AppMode::Viewer,
            database: None,
            database_path: None,
            tables: Vec::new(),
            views: Vec::new(),
            sidebar: Sidebar::new(),
            home_screen: HomeScreen::new(),
            query_windows: Vec::new(),
            csv_import_dialog: None,
            file_config_dialog: FileConfigDialog::new(),
            duplicate_detection_dialog: DuplicateDetectionDialog::default(),
            duplicate_results_viewer: DuplicateResultsViewer::default(),
            transformation_dialog: TransformationDialog::new(),
            transformation_manager: TransformationManager::new(),
            computed_columns_dialog: ComputedColumnsDialog::new(),
            computed_columns_processor: ComputedColumnsProcessor::new(),
            enhanced_grouping_dialog: EnhancedGroupingDialog::new(),
            enhanced_grouping_processor: EnhancedGroupingProcessor::new(),
            time_bin_dialog: TimeBinDialog::default(),
            next_window_id: 0,
            error: None,
        }
    }
    
    pub fn update(&mut self, ctx: &Context) {
        // Apply dark theme
        ctx.set_visuals(egui::Visuals::dark());
        
        // Top panel with menu
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.label(egui::RichText::new("Leaf").size(16.0).strong());
                ui.separator();
                
            });
        });
        
        // Error display
        if let Some(error) = self.error.clone() {
            egui::TopBottomPanel::top("error_panel").show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.colored_label(egui::Color32::from_rgb(255, 100, 100), format!("✗ {}", error));
                    if ui.button("×").clicked() {
                        self.error = None;
                    }
                });
            });
        }
        
        // Sidebar
        if self.database.is_some() {
            egui::SidePanel::left("sidebar")
                .default_width(200.0)
                .min_width(150.0)
                .max_width(300.0)
                .resizable(true)
                .show(ctx, |ui| {
                    // Set darker background for the sidebar panel with green tint
                    ui.visuals_mut().widgets.noninteractive.bg_fill = egui::Color32::from_rgb(26, 32, 28);
                    
                    match self.sidebar.show(ctx, ui, &self.tables, &self.views) {
                        SidebarAction::OpenTable(table_name) => {
                            self.open_query_window(&table_name);
                        }
                        SidebarAction::OpenDuplicateDetection => {
                            self.duplicate_detection_dialog.visible = true;
                            if let Some(db) = &self.database {
                                self.duplicate_detection_dialog.update_available_tables_and_columns(db);
                            }
                        }
                        SidebarAction::RefreshDatabase => {
                            self.refresh_database();
                        }
                        SidebarAction::AddComputedColumns => {
                            self.computed_columns_dialog.visible = true;
                            if let Some(db) = &self.database {
                                self.computed_columns_dialog.update_available_tables(db);
                            }
                        }
                        SidebarAction::AddTimeBinColumn => {
                            self.time_bin_dialog.visible = true;
                            if let Some(db) = &self.database {
                                self.time_bin_dialog.update_available_tables(db);
                            }
                        }
                        SidebarAction::AddGroupIdColumns => {
                            self.enhanced_grouping_dialog.visible = true;
                            if let Some(db) = &self.database {
                                self.enhanced_grouping_dialog.update_available_tables(db);
                            }
                        }
                        SidebarAction::None => {}
                    }
                });
        }
        
        // Main content area
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.database.is_none() && !self.file_config_dialog.show {
                let action = self.home_screen.show(ctx, ui);
                if let Some(action) = action {
                    match action {
                        HomeAction::OpenProject => self.open_database(),
                        HomeAction::CreateProject => self.file_config_dialog.open_with_csv_selection(),
                    }
                }
            } else if self.database.is_some() {
                // Main interface when database is loaded
                ui.centered_and_justified(|ui| {
                    ui.vertical_centered(|ui| {
                        ui.heading(egui::RichText::new("Project Loaded").size(24.0));
                        ui.add_space(10.0);
                        ui.label(egui::RichText::new("Use the sidebar to explore tables and run queries").size(14.0).color(egui::Color32::from_gray(180)));
                        
                        if self.tables.is_empty() {
                            ui.add_space(20.0);
                            ui.label(egui::RichText::new("No tables found").size(16.0).color(egui::Color32::from_gray(150)));
                            ui.label(egui::RichText::new("This project is empty or the tables were not persisted").size(12.0).color(egui::Color32::from_gray(120)));
                        } else {
                            ui.add_space(20.0);
                            ui.label(egui::RichText::new(format!("Found {} table(s)", self.tables.len())).size(16.0).color(egui::Color32::from_gray(150)));
                        }
                    });
                });
            }
        });
        
        // Query windows
        if let Some(db) = &self.database {
            // Show query windows
            self.query_windows.retain_mut(|window| {
                window.show(ctx, db.clone())
            });
        }
        
        // Show CSV import dialog if active
        if let Some(dialog) = &mut self.csv_import_dialog {
            if !dialog.show(ctx) {
                self.csv_import_dialog = None;
                self.load_tables(); // Refresh after import
            }
        }
        
        // Show duplicate detection dialog if active
        if let Some(db) = &self.database {
            self.duplicate_detection_dialog.show(ctx, db);
        }
        
        // Show transformation dialog if active
        if let Some(db) = &self.database {
            if let Some(request) = self.transformation_dialog.show(ctx, db) {
                let default_path = std::path::PathBuf::from(".");
                let output_dir = self.database_path.as_ref().unwrap_or(&default_path);
                match self.transformation_manager.apply_transformation(&request, db, output_dir) {
                    Ok(output_path) => {
                        self.error = Some(format!("Transformation completed successfully! Output saved to: {}", output_path));
                    }
                    Err(e) => {
                        self.error = Some(format!("Transformation failed: {}", e));
                    }
                }
            }
        }
        
        // Show computed columns dialog if active
        if let Some(db) = &self.database {
            if let Some(request) = self.computed_columns_dialog.show(ctx, db) {
                let default_path = std::path::PathBuf::from(".");
                let output_dir = self.database_path.as_ref().unwrap_or(&default_path);
                
                match self.computed_columns_processor.process_request(&request, db, output_dir) {
                    Ok(output_filename) => {
                        self.error = Some(format!("Computed columns created successfully! Output saved to: {}", output_filename));
                        // Refresh the database to show the new file
                        self.refresh_database();
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to create computed columns: {}", e));
                    }
                }
            }
        }
        
        // Show enhanced grouping dialog if active
        if let Some(db) = &self.database {
            if let Some(request) = self.enhanced_grouping_dialog.show(ctx, db) {
                let default_path = std::path::PathBuf::from(".");
                let output_dir = self.database_path.as_ref().unwrap_or(&default_path);
                
                match self.enhanced_grouping_processor.process_request(&request, db, output_dir) {
                    Ok(output_filename) => {
                        self.error = Some(format!("Group ID columns created successfully! Output saved to: {}", output_filename));
                        self.refresh_database();
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to create group ID columns: {}", e));
                    }
                }
            }
        }
        
        // Show time bin dialog if active
        if let Some(db) = &self.database {
            let default_path = std::path::PathBuf::from(".");
            let output_dir = self.database_path.as_ref().unwrap_or(&default_path);
            self.time_bin_dialog.show(ctx, db.clone(), output_dir);
        }
        
        // File config dialog
        if let Some(path) = self.file_config_dialog.show(ctx) {
            self.mode = AppMode::Builder;
            // For DataFusion, we need to create a new database context
            // since the file config dialog creates its own context
            match Database::open_writable(&path) {
                Ok(db) => {
                    self.database = Some(Arc::new(db));
                    self.database_path = Some(path.clone());
                    
                    // Try to load tables from persistence
                    match self.load_all_tables_from_persistence() {
                        Ok(loaded_tables) => {
                            if !loaded_tables.is_empty() {
                                // Show success notification instead of error
                                println!("[App] Successfully created database with {} tables", loaded_tables.len());
                            } else {
                                println!("[App] Database created but no tables were found");
                            }
                        }
                        Err(e) => {
                            println!("[App] No persisted tables found: {}", e);
                        }
                    }
                    
                    self.load_tables();
                    self.error = None;
                    
                    // Force a repaint to ensure the UI updates
                    ctx.request_repaint();
                }
                Err(e) => {
                    self.error = Some(format!("Failed to open database: {}", e));
                }
            }
        }
    }
    
    fn open_database(&mut self) {
        if let Some(path) = rfd::FileDialog::new()
                            .add_filter("Project Folders", &["*"])
                            .set_title("Select project folder")
                            .pick_folder()
        {
            match if self.mode == AppMode::Builder {
                Database::open_writable(&path)
            } else {
                Database::open_readonly(&path)
            } {
                Ok(db) => {
                    self.database = Some(Arc::new(db));
                    self.database_path = Some(path.clone());
                    
                    // Try to load tables from persistence
                    match self.load_all_tables_from_persistence() {
                        Ok(loaded_tables) => {
                            if !loaded_tables.is_empty() {
                                // self.error = Some(format!("Loaded {} tables from project", loaded_tables.len()));
                            } else {
                                self.error = Some("No tables found in project - this is a new project".to_string());
                            }
                        }
                        Err(e) => {
                            println!("[App] No persisted tables found: {}", e);
                            self.error = Some("No tables found in project - this is a new project".to_string());
                        }
                    }
                    
                    self.load_tables();
                }
                Err(e) => {
                    self.error = Some(format!("Failed to open project: {}", e));
                }
            }
        }
    }
    
    fn new_database_from_csv(&mut self) {
        // This method is no longer needed as we handle it directly in the menu
    }
    
    fn load_database(&mut self, path: std::path::PathBuf) {
        match self.mode {
            AppMode::Viewer => {
                            match Database::open_readonly(&path) {
                Ok(db) => {
                    self.database = Some(Arc::new(db));
                    self.database_path = Some(path.clone());
                    
                    // Try to load tables from persistence
                    if let Err(e) = self.load_all_tables_from_persistence() {
                        println!("[App] No persisted tables found: {}", e);
                    }
                    
                    self.load_tables();
                    self.error = None;
                }
                    Err(e) => {
                        self.error = Some(format!("Failed to open database: {}", e));
                    }
                }
            }
            AppMode::Builder => {
                match Database::open_writable(&path) {
                    Ok(db) => {
                        // Use the same writable connection for both operations
                        self.database = Some(Arc::new(db));
                        self.database_path = Some(path.clone());
                        
                        // Try to load tables from persistence
                        if let Err(e) = self.load_all_tables_from_persistence() {
                            println!("[App] No persisted tables found: {}", e);
                        }
                        
                        self.load_tables();
                        self.error = None;
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to open database: {}", e));
                    }
                }
            }
        }
    }
    
    fn open_query_window(&mut self, table_name: &str) {
        if let Some(_db) = &self.database {
            let window = QueryWindow::new(
                self.next_window_id,
                table_name.to_string(),
                format!("SELECT * FROM \"{}\"", table_name),
            );
            self.query_windows.push(window);
            self.next_window_id += 1;
        }
    }
    
    fn show_csv_import(&mut self) {
        if self.database_path.is_some() && self.mode == AppMode::Builder {
            self.csv_import_dialog = Some(CsvImportDialog::new(Id::new("csv_import_dialog")));
        } else {
            self.error = Some("Open a database in Builder mode to import CSV files".to_string());
        }
    }

    // === HYBRID PERSISTENCE METHODS ===

    /// Save all current tables in Arrow IPC format
    pub fn save_all_tables(&mut self) -> Result<Vec<String>, String> {
        if let Some(db) = &self.database {
            if let Some(path) = &self.database_path {
                // Use the project folder directly for data storage
                let data_dir = path;
                
                // Clone the database for mutable operations
                let mut db_clone = (**db).clone();
                match db_clone.save_all_tables(&data_dir) {
                    Ok(saved_tables) => {
                        println!("[App] Saved {} tables to {:?}", saved_tables.len(), data_dir);
                        // Update the stored database with the modified version
                        self.database = Some(Arc::new(db_clone));
                        Ok(saved_tables)
                    }
                    Err(e) => Err(format!("Failed to save tables: {}", e))
                }
            } else {
                Err("No database path available".to_string())
            }
        } else {
            Err("No database loaded".to_string())
        }
    }

    /// Load all tables from the hybrid persistence directory
    pub fn load_all_tables_from_persistence(&mut self) -> Result<Vec<String>, String> {
        if let Some(db) = &self.database {
            if let Some(path) = &self.database_path {
                // Look for data files directly in the project folder
                let data_dir = path;
                
                // Clone the database for mutable operations
                let mut db_clone = (**db).clone();
                match db_clone.load_all_tables_from_directory(&data_dir) {
                    Ok(loaded_tables) => {
                        println!("[App] Loaded {} tables from {:?}", loaded_tables.len(), data_dir);
                        // Update the stored database with the modified version
                        self.database = Some(Arc::new(db_clone));
                        // Refresh the tables list
                        self.load_tables();
                        Ok(loaded_tables)
                    }
                    Err(e) => Err(format!("Failed to load tables: {}", e))
                }
            } else {
                Err("No database path available".to_string())
            }
        } else {
            Err("No database loaded".to_string())
        }
    }

    /// Save a specific table in both formats
    pub fn save_table(&mut self, table_name: &str) -> Result<(), String> {
        if let Some(db) = &self.database {
            if let Some(path) = &self.database_path {
                let data_dir = path;
                
                // Clone the database for mutable operations
                let mut db_clone = (**db).clone();
                match db_clone.save_table_dual(table_name, &data_dir) {
                    Ok(_) => {
                        println!("[App] Saved table '{}' in both formats", table_name);
                        // Update the stored database with the modified version
                        self.database = Some(Arc::new(db_clone));
                        Ok(())
                    }
                    Err(e) => Err(format!("Failed to save table '{}': {}", table_name, e))
                }
            } else {
                Err("No database path available".to_string())
            }
        } else {
            Err("No database loaded".to_string())
        }
    }

    /// Import CSV and automatically save in both formats
    pub fn import_csv_with_persistence(&mut self, csv_path: &std::path::Path, table_name: &str, 
                                     delimiter: char, has_header: bool) -> Result<(), String> {
        if let Some(db) = &self.database {
            // Clone the database for mutable operations
            let mut db_clone = (**db).clone();
            // Import CSV into DataFusion
            match db_clone.stream_insert_csv(table_name, csv_path, delimiter, has_header) {
                Ok(_) => {
                    // Save in both formats immediately
                    match db_clone.save_table_dual(table_name, &self.database_path.as_ref().unwrap()) {
                        Ok(_) => {
                            println!("[App] Imported and saved table '{}'", table_name);
                            // Update the stored database with the modified version
                            self.database = Some(Arc::new(db_clone));
                            // Refresh tables list
                            self.load_tables();
                            Ok(())
                        }
                        Err(e) => Err(format!("Failed to save imported table: {}", e))
                    }
                }
                Err(e) => Err(format!("Failed to import CSV: {}", e))
            }
        } else {
            Err("No database loaded".to_string())
        }
    }

    fn load_tables(&mut self) {
        if let Some(db) = &self.database {
            match db.get_tables() {
                Ok(tables) => {
                    self.tables = tables;
                    // If no tables found but we have a database path, this might be a DataFusion database
                    // that needs to be re-created. For now, we'll just show the empty state.
                    if self.tables.is_empty() && self.database_path.is_some() {
                        println!("[DEBUG] No tables found in DataFusion context. This is normal for in-memory databases.");
                    }
                },
                Err(e) => self.error = Some(format!("Failed to load tables: {}", e)),
            }
            
            match db.get_views() {
                Ok(views) => {
                    self.views = views.into_iter().map(|v| v.name).collect();
                },
                Err(e) => self.error = Some(format!("Failed to load views: {}", e)),
            }
        }
    }

    fn refresh_database(&mut self) {
        if let Some(db_path) = &self.database_path {
            if let Some(db) = &mut self.database {
                // Clone the database for mutable operations
                let mut db_clone = (**db).clone();
                match db_clone.load_all_tables_from_directory(db_path) {
                    Ok(loaded_tables) => {
                        if !loaded_tables.is_empty() {
                            // Update the stored database with the modified version
                            self.database = Some(Arc::new(db_clone));
                            self.load_tables();
                            self.error = Some(format!("Refreshed database: loaded {} new tables", loaded_tables.len()));
                        } else {
                            self.error = Some("No new Arrow files found in database directory".to_string());
                        }
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to refresh database: {}", e));
                    }
                }
            }
        } else {
            self.error = Some("No database path available for refresh".to_string());
        }
    }
} 