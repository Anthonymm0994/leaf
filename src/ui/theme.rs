use egui::{Context, Visuals};

pub fn apply_theme(ctx: &Context) {
    let mut visuals = Visuals::dark();
    
    // Main background - dark with subtle green tint
    visuals.panel_fill = egui::Color32::from_rgb(18, 22, 20);
    visuals.window_fill = egui::Color32::from_rgb(22, 26, 24);
    
    // Create more depth with green-tinted grays
    visuals.extreme_bg_color = egui::Color32::from_rgb(14, 18, 16);  // Darkest elements
    visuals.faint_bg_color = egui::Color32::from_rgb(26, 32, 28);    // Subtle backgrounds
    
    // Selection and hover colors - leaf green inspired
    visuals.selection.bg_fill = egui::Color32::from_rgb(40, 70, 50);
    visuals.selection.stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(80, 140, 90));
    
    // Widget colors with green accents
    visuals.widgets.noninteractive.bg_fill = egui::Color32::from_rgb(30, 36, 32);
    visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(45, 55, 48));
    visuals.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(180, 190, 180));
    
    visuals.widgets.inactive.bg_fill = egui::Color32::from_rgb(35, 42, 38);
    visuals.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(50, 60, 52));
    visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(160, 170, 160));
    
    visuals.widgets.hovered.bg_fill = egui::Color32::from_rgb(40, 48, 42);
    visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(70, 120, 80));
    visuals.widgets.hovered.fg_stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(200, 220, 200));
    
    visuals.widgets.active.bg_fill = egui::Color32::from_rgb(45, 65, 50);
    visuals.widgets.active.bg_stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(90, 160, 100));
    visuals.widgets.active.fg_stroke = egui::Stroke::new(1.0, egui::Color32::from_rgb(220, 240, 220));
    
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
    
    // Hyperlink color - soft green
    visuals.hyperlink_color = egui::Color32::from_rgb(120, 200, 140);
    
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