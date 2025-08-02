mod app;
mod core;
mod infer;
mod ui;

use eframe::egui;
use app::LeafApp;
use ui::apply_theme;

fn test_delta_null_handling() {
    use core::transformations::DataTransformer;
    
    let transformer = DataTransformer::new();
    
    println!("Testing delta null handling...");
    match transformer.test_delta_null_handling() {
        Ok(()) => println!("✅ Test passed! Null values are working correctly."),
        Err(e) => println!("❌ Test failed: {}", e),
    }
}

fn main() -> Result<(), eframe::Error> {
    // Check if we should run tests
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "--test-delta" {
        test_delta_null_handling();
        return Ok(());
    }
    
    // Load icon from leaf.png
    let icon_data = load_icon_from_png();
    
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 800.0])
            .with_min_inner_size([800.0, 600.0])
            .with_title("Leaf - Arrow File Ingestion Tool")
            .with_icon(icon_data),
        ..Default::default()
    };
    
    eframe::run_native(
        "Leaf",
        options,
        Box::new(|cc| {
            // Apply the dark theme
            apply_theme(&cc.egui_ctx);
            Ok(Box::new(LeafApp::new()))
        }),
    )
}

fn load_icon_from_png() -> egui::IconData {
    // Try to load the leaf.png file
    if let Ok(image_data) = std::fs::read("media/leaf.png") {
        if let Ok(image) = image::load_from_memory(&image_data) {
            let image = image.resize_exact(32, 32, image::imageops::FilterType::Lanczos3);
            let image_buffer = image.to_rgba8();
            return egui::IconData {
                rgba: image_buffer.into_raw(),
                width: 32,
                height: 32,
            };
        }
    }
    
    // Fallback to generated icon if loading fails
    create_leaf_icon()
}

fn create_leaf_icon() -> egui::IconData {
    // Create a leaf-themed icon
    let size = 32u32;
    let mut pixels = vec![0u8; (size * size * 4) as usize];
    
    // Draw a modern, clean icon
    for y in 0..size {
        for x in 0..size {
            let idx = ((y * size + x) * 4) as usize;
            let cx = size as f32 / 2.0;
            let cy = size as f32 / 2.0;
            let dx = x as f32 - cx;
            let dy = y as f32 - cy;
            
            // Create a clean, modern shape
            let dist_x = dx.abs() / 14.0;
            let dist_y = dy.abs() / 12.0;
            let combined = (dist_x.powf(2.0) + dist_y.powf(2.0)).powf(0.5);
            
            if combined < 1.0 {
                // Modern blue color scheme
                let base_color = 100u8;
                let edge_factor = combined;
                let color = (base_color as f32 * (1.0 - edge_factor * 0.3)) as u8;
                
                pixels[idx] = color;       // R
                pixels[idx + 1] = color + 50;   // G
                pixels[idx + 2] = color + 100;   // B
                pixels[idx + 3] = 255;     // A
                
                // Add modern accent elements
                let fx = x as i32;
                let fy = y as i32;
                
                // Center accent
                if ((fx - 16).pow(2) + (fy - 16).pow(2) <= 16) {
                    pixels[idx] = (pixels[idx] as u16 * 12 / 10).min(255) as u8;
                    pixels[idx + 1] = (pixels[idx + 1] as u16 * 12 / 10).min(255) as u8;
                    pixels[idx + 2] = (pixels[idx + 2] as u16 * 12 / 10).min(255) as u8;
                }
            }
        }
    }
    
    egui::IconData {
        rgba: pixels,
        width: size,
        height: size,
    }
}

impl eframe::App for LeafApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.update(ctx);
    }
} 