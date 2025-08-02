use egui;
use crate::app::state::HomeAction;

pub struct HomeScreen {
    leaf_texture: Option<egui::TextureHandle>,
}

impl HomeScreen {
    pub fn new() -> Self {
        Self {
            leaf_texture: None,
        }
    }
    
    pub fn show(&mut self, ctx: &egui::Context, ui: &mut egui::Ui) -> Option<HomeAction> {
        // Load leaf texture on first frame
        if self.leaf_texture.is_none() {
            if let Ok(image_data) = std::fs::read("media/leaf.png") {
                if let Ok(image) = image::load_from_memory(&image_data) {
                    let size = [image.width() as _, image.height() as _];
                    let image_buffer = image.to_rgba8();
                    let pixels = image_buffer.as_flat_samples();
                    let color_image = egui::ColorImage::from_rgba_unmultiplied(
                        size,
                        pixels.as_slice(),
                    );
                                            self.leaf_texture = Some(ctx.load_texture(
                            "leaf_logo",
                        color_image,
                        egui::TextureOptions::default(),
                    ));
                }
            }
        }
        let mut action = None;
        ui.centered_and_justified(|ui| {
            ui.vertical_centered(|ui| {
                            // Display leaf image if loaded
            if let Some(texture) = &self.leaf_texture {
                    let desired_size = egui::vec2(120.0, 120.0);
                    let image_size = texture.size_vec2();
                    let scale = (desired_size.x / image_size.x).min(desired_size.y / image_size.y);
                    let scaled_size = image_size * scale;
                    
                    ui.add(egui::Image::new(texture).fit_to_exact_size(scaled_size));
                    ui.add_space(20.0);
                }
                
                ui.heading(egui::RichText::new("Leaf").size(32.0));
                ui.add_space(10.0);
                ui.label(egui::RichText::new("In-memory data exploration with DataFusion + Arrow").size(16.0).color(egui::Color32::from_gray(180)));
                ui.add_space(10.0);
                ui.label(egui::RichText::new("(Fast, modern, and visual analytics for CSV-based datasets)").size(14.0).italics().color(egui::Color32::from_gray(150)));
                ui.add_space(30.0);
                
                ui.group(|ui| {
                    ui.set_width(300.0);
                    if ui.button(egui::RichText::new("Open Project").size(16.0))
                        .on_hover_text("Open an existing data project")
                        .clicked() {
                        action = Some(HomeAction::OpenProject);
                    }
                    
                    ui.add_space(10.0);
                    
                    if ui.button(egui::RichText::new("Create Project from CSVs").size(16.0))
                        .on_hover_text("Import CSV files to create a new data project")
                        .clicked() {
                        action = Some(HomeAction::CreateProject);
                    }
                });
                
                ui.add_space(30.0);
                
                ui.label(egui::RichText::new("Tips:").size(14.0).strong().color(egui::Color32::from_gray(200)));
                ui.label(egui::RichText::new("• Create data projects from CSVs").color(egui::Color32::from_gray(160)));
                ui.label(egui::RichText::new("• Automatically infer types and headers").color(egui::Color32::from_gray(160)));
                ui.label(egui::RichText::new("• Fast querying with DataFusion and Arrow").color(egui::Color32::from_gray(160)));
                ui.label(egui::RichText::new("• Export query results as CSV or JSON").color(egui::Color32::from_gray(160)));
                ui.label(egui::RichText::new("• Press Ctrl+Enter to run your queries").color(egui::Color32::from_gray(160)));
            });
        });
        action
    }
} 