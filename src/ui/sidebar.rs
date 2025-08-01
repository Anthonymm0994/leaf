use egui;
use crate::core::database::TableInfo;

#[derive(Debug, PartialEq)]
pub enum SidebarAction {
    None,
    OpenTable(String),
    OpenDuplicateDetection,
    RefreshDatabase,
    AddDerivedField,
    AddTimeBinColumn,
    AddRowIdColumns,
    TimeBasedGrouping,
}

pub struct Sidebar {
    selected_table: Option<String>,
    selected_view: Option<String>,
    duplicate_detection_clicked: bool,
}

impl Sidebar {
    pub fn new() -> Self {
        Self {
            selected_table: None,
            selected_view: None,
            duplicate_detection_clicked: false,
        }
    }
    
    pub fn show(&mut self, _ctx: &egui::Context, ui: &mut egui::Ui, tables: &[TableInfo], views: &[String]) -> SidebarAction {
        let mut table_to_open = None;
        let mut action = SidebarAction::None;
        self.duplicate_detection_clicked = false;
        
        // Darker background for the sidebar
        ui.visuals_mut().widgets.noninteractive.bg_fill = egui::Color32::from_gray(25);
        
        ui.vertical(|ui| {
            ui.heading("Data Sources");
            ui.separator();
            
            // Refresh button
            if ui.button("🔄 Refresh Database").clicked() {
                action = SidebarAction::RefreshDatabase;
            }
            
            ui.add_space(10.0);
            
            // Tools section
            ui.heading("Tools");
            ui.separator();
            
            if ui.button("🔍 Detect Duplicate Blocks").clicked() {
                self.duplicate_detection_clicked = true;
            }
            
            if ui.button("➕ Add Derived Field").clicked() {
                action = SidebarAction::AddDerivedField;
            }
            
            if ui.button("⏰ Add Time Bin Column").clicked() {
                action = SidebarAction::AddTimeBinColumn;
            }
            
            if ui.button("🆔 Add Row ID Columns").clicked() {
                action = SidebarAction::AddRowIdColumns;
            }
            
            if ui.button("⏱️ Time-Based Grouping").clicked() {
                action = SidebarAction::TimeBasedGrouping;
            }
            
            ui.add_space(10.0);
            
            // Tables section
            egui::CollapsingHeader::new(format!("Tables ({})", tables.len()))
                .default_open(true)
                .show(ui, |ui| {
                    // Even darker background for table list
                    ui.visuals_mut().widgets.noninteractive.bg_fill = egui::Color32::from_gray(20);
                    
                    egui::ScrollArea::vertical()
                        .max_height(300.0)
                        .show(ui, |ui| {
                            for table in tables {
                                let response = ui.allocate_response(
                                    egui::vec2(ui.available_width(), 50.0),
                                    egui::Sense::click(),
                                );
                                
                                if response.clicked() {
                                    table_to_open = Some(table.name.clone());
                                }
                                
                                let visuals = if response.hovered() {
                                    ui.visuals().widgets.hovered
                                } else {
                                    ui.visuals().widgets.inactive
                                };
                                
                                ui.painter().rect(
                                    response.rect,
                                    visuals.rounding,
                                    visuals.bg_fill,
                                    visuals.bg_stroke,
                                );
                                
                                let text_pos = response.rect.min + egui::vec2(8.0, 8.0);
                                ui.painter().text(
                                    text_pos,
                                    egui::Align2::LEFT_TOP,
                                    &table.name,
                                    egui::FontId::proportional(14.0),
                                    ui.visuals().text_color(),
                                );
                                
                                let info_text = format!("{} rows, {} columns", table.row_count, table.columns.len());
                                let info_pos = text_pos + egui::vec2(0.0, 20.0);
                                ui.painter().text(
                                    info_pos,
                                    egui::Align2::LEFT_TOP,
                                    info_text,
                                    egui::FontId::proportional(12.0),
                                    egui::Color32::from_gray(140),
                                );
                            }
                        });
                });
            
            ui.add_space(10.0);
            
            // Views section
            egui::CollapsingHeader::new(format!("Views ({})", views.len()))
                .default_open(false)
                .show(ui, |ui| {
                    // Even darker background for view list
                    ui.visuals_mut().widgets.noninteractive.bg_fill = egui::Color32::from_gray(20);
                    
                    egui::ScrollArea::vertical()
                        .max_height(200.0)
                        .show(ui, |ui| {
                            for view in views {
                                let response = ui.allocate_response(
                                    egui::vec2(ui.available_width(), 30.0),
                                    egui::Sense::click(),
                                );
                                
                                if response.clicked() {
                                    table_to_open = Some(view.clone());
                                }
                                
                                let visuals = if response.hovered() {
                                    ui.visuals().widgets.hovered
                                } else {
                                    ui.visuals().widgets.inactive
                                };
                                
                                ui.painter().rect(
                                    response.rect,
                                    visuals.rounding,
                                    visuals.bg_fill,
                                    visuals.bg_stroke,
                                );
                                
                                let text_pos = response.rect.min + egui::vec2(8.0, 8.0);
                                ui.painter().text(
                                    text_pos,
                                    egui::Align2::LEFT_CENTER,
                                    view,
                                    egui::FontId::proportional(14.0),
                                    ui.visuals().text_color(),
                                );
                            }
                        });
                });
        });
        
        if action != SidebarAction::None {
            action
        } else if self.duplicate_detection_clicked {
            SidebarAction::OpenDuplicateDetection
        } else if let Some(table) = table_to_open {
            SidebarAction::OpenTable(table)
        } else {
            SidebarAction::None
        }
    }
} 