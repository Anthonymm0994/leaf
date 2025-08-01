use egui::{Context, Id};
use rfd::FileDialog;
use std::fs::File;
use std::io::Write;
use crate::core::{QueryResult, CsvWriter};

pub struct ExportDialog {
    id: Id,
    result: QueryResult,
    export_format: ExportFormat,
    show: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExportFormat {
    Csv,
    Json,
}

impl ExportDialog {
    pub fn new(id: Id, result: QueryResult) -> Self {
        Self {
            id,
            result,
            export_format: ExportFormat::Csv,
            show: true,
        }
    }
    
    pub fn show(&mut self, ctx: &Context) -> bool {
        if !self.show {
            return false;
        }
        
        let mut keep_open = true;
        
        egui::Window::new("Export Results")
            .id(self.id)
            .default_size([300.0, 150.0])
            .resizable(false)
            .collapsible(false)
            .open(&mut keep_open)
            .show(ctx, |ui| {
                ui.vertical(|ui| {
                    ui.label("Select export format:");
                    
                    ui.radio_value(&mut self.export_format, ExportFormat::Csv, "CSV");
                    ui.radio_value(&mut self.export_format, ExportFormat::Json, "JSON");
                    
                    ui.separator();
                    
                    ui.horizontal(|ui| {
                        if ui.button("Export").clicked() {
                            self.export();
                            self.show = false;
                        }
                        
                        if ui.button("Cancel").clicked() {
                            self.show = false;
                        }
                    });
                });
            });
        
        keep_open && self.show
    }
    
    fn export(&self) {
        let extension = match self.export_format {
            ExportFormat::Csv => "csv",
            ExportFormat::Json => "json",
        };
        
        if let Some(path) = FileDialog::new()
            .add_filter(extension, &[extension])
            .save_file()
        {
            match self.export_format {
                ExportFormat::Csv => self.export_csv(&path),
                ExportFormat::Json => self.export_json(&path),
            }
        }
    }
    
    fn export_csv(&self, path: &std::path::Path) {
        if let Ok(mut writer) = CsvWriter::from_path(path) {
            // Write headers
            let _ = writer.write_headers(&self.result.columns);
            
            // Write rows
            for row in &self.result.rows {
                let _ = writer.write_record(row);
            }
            
            let _ = writer.flush();
        }
    }
    
    fn export_json(&self, path: &std::path::Path) {
        let json_data: Vec<serde_json::Map<String, serde_json::Value>> = self.result.rows
            .iter()
            .map(|row| {
                let mut map = serde_json::Map::new();
                for (i, value) in row.iter().enumerate() {
                    if let Some(column) = self.result.columns.get(i) {
                        map.insert(column.clone(), serde_json::Value::String(value.clone()));
                    }
                }
                map
            })
            .collect();
        
        if let Ok(json_string) = serde_json::to_string_pretty(&json_data) {
            let _ = File::create(path).and_then(|mut file| file.write_all(json_string.as_bytes()));
        }
    }
} 