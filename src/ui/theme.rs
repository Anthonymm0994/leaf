use egui::{Context, Visuals};

pub fn apply_theme(ctx: &Context) {
    let mut visuals = Visuals::dark();
    
    // Main background - very dark
    visuals.panel_fill = egui::Color32::from_gray(20);
    visuals.window_fill = egui::Color32::from_gray(25);
    
    // Create more depth with varied grays
    visuals.extreme_bg_color = egui::Color32::from_gray(15);  // Darkest elements
    visuals.faint_bg_color = egui::Color32::from_gray(30);    // Subtle backgrounds
    
    // Selection and hover colors
    visuals.selection.bg_fill = egui::Color32::from_rgb(60, 60, 80);
    visuals.selection.stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(100, 100, 120));
    
    // Widget colors with more depth
    visuals.widgets.noninteractive.bg_fill = egui::Color32::from_gray(35);
    visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(50));
    visuals.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(180));
    
    visuals.widgets.inactive.bg_fill = egui::Color32::from_gray(40);
    visuals.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(55));
    visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(160));
    
    visuals.widgets.hovered.bg_fill = egui::Color32::from_gray(45);
    visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(70));
    visuals.widgets.hovered.fg_stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(200));
    
    visuals.widgets.active.bg_fill = egui::Color32::from_gray(50);
    visuals.widgets.active.bg_stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(80));
    visuals.widgets.active.fg_stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(220));
    
    // Window shadows for depth
    visuals.window_shadow = egui::epaint::Shadow {
        offset: egui::vec2(0.0, 8.0),
        blur: 16.0,
        spread: 0.0,
        color: egui::Color32::from_black_alpha(96),
    };
    
    // Popup shadows
    visuals.popup_shadow = egui::epaint::Shadow {
        offset: egui::vec2(0.0, 4.0),
        blur: 8.0,
        spread: 0.0,
        color: egui::Color32::from_black_alpha(64),
    };
    
    // Hyperlink color
    visuals.hyperlink_color = egui::Color32::from_rgb(100, 150, 200);
    
    // Window rounding
    visuals.window_rounding = egui::Rounding::same(6.0);
    
    ctx.set_visuals(visuals);
}

pub struct Theme;

impl Theme {
    pub fn button_size() -> egui::Vec2 {
        egui::Vec2::new(80.0, 24.0)
    }
    
    pub fn small_button_size() -> egui::Vec2 {
        egui::Vec2::new(60.0, 20.0)
    }
} 