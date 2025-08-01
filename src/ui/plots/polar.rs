use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi, Points};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;
use std::f64::consts::PI;

pub struct PolarPlot;

impl PlotTrait for PolarPlot {
    fn name(&self) -> &'static str {
        "Polar Plot"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![DataType::Float64, DataType::Int64])
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Float64, DataType::Int64]
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("Color", vec![DataType::Float64, DataType::Int64, DataType::Utf8]),
            ("Size", vec![DataType::Float64, DataType::Int64]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    
    fn supports_size_mapping(&self) -> bool { true }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for polar plots".to_string());
        }
        
        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column)
            .ok_or("X column not found")?;
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or("Y column not found")?;
        
        // Find color column
        let color_idx = if let Some(color_col) = &config.color_column {
            if !color_col.is_empty() {
                query_result.columns.iter().position(|c| c == color_col)
            } else {
                None
            }
        } else {
            None
        };
        
        // Find size column
        let size_idx = if let Some(size_col) = &config.size_column {
            if !size_col.is_empty() {
                query_result.columns.iter().position(|c| c == size_col)
            } else {
                None
            }
        } else {
            None
        };
        
        let mut points = Vec::new();
        let mut polar_data = Vec::new();
        
        for (row_idx, row) in query_result.rows.iter().enumerate() {
            if row.len() > x_idx && row.len() > y_idx {
                // Parse angle (theta) value - convert to radians if needed
                let angle_val = row[x_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse angle value '{}' as number", row[x_idx]))?;
                
                // Normalize angle to 0-2π range
                let normalized_angle = if angle_val < 0.0 {
                    angle_val + 2.0 * PI
                } else if angle_val > 2.0 * PI {
                    angle_val - 2.0 * PI
                } else {
                    angle_val
                };
                
                // Parse radius value
                let radius_val = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse radius value '{}' as number", row[y_idx]))?;
                
                // Ensure radius is positive
                let radius_val = radius_val.abs();
                
                // Create color mapping
                let color = if let Some(color_idx) = color_idx {
                    if row.len() > color_idx {
                        let color_value = &row[color_idx];
                        if let Ok(num_val) = color_value.parse::<f64>() {
                            // Use a color gradient based on the value
                            let normalized = (num_val - 0.0).max(0.0).min(1.0);
                            Color32::from_rgb(
                                (normalized * 255.0) as u8,
                                ((1.0 - normalized) * 255.0) as u8,
                                128
                            )
                        } else {
                            // Categorical color
                            let hash = color_value.chars().map(|c| c as u32).sum::<u32>();
                            Color32::from_rgb(
                                (hash % 256) as u8,
                                ((hash >> 8) % 256) as u8,
                                ((hash >> 16) % 256) as u8,
                            )
                        }
                    } else {
                        Color32::BLUE
                    }
                } else {
                    Color32::BLUE
                };
                
                // Get size mapping
                let size = if let Some(size_idx) = size_idx {
                    if row.len() > size_idx {
                        row[size_idx].parse::<f32>().unwrap_or(4.0)
                    } else {
                        4.0
                    }
                } else {
                    4.0
                };
                
                // Convert polar to Cartesian coordinates
                let x = radius_val * normalized_angle.cos();
                let y = radius_val * normalized_angle.sin();
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Angle (rad)".to_string(), normalized_angle.to_string());
                tooltip_data.insert("Angle (deg)".to_string(), (normalized_angle * 180.0 / PI).to_string());
                tooltip_data.insert("Radius".to_string(), radius_val.to_string());
                tooltip_data.insert("X".to_string(), x.to_string());
                tooltip_data.insert("Y".to_string(), y.to_string());
                tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
                
                points.push(PlotPoint {
                    x,
                    y,
                    z: None,
                    label: None,
                    color: Some(color),
                    size: Some(size),
                    series_id: None,
                    tooltip_data,
                });
                
                polar_data.push(PolarDataPoint {
                    angle: normalized_angle,
                    radius: radius_val,
                    color,
                    size,
                });
            }
        }
        
        // Calculate polar statistics
        let statistics = calculate_polar_statistics(&polar_data);
        
        Ok(PlotData {
            points,
            series: vec![],
            metadata: super::PlotMetadata {
                title: config.title.clone(),
                x_label: config.x_column.clone(),
                y_label: config.y_column.clone(),
                show_legend: config.show_legend,
                show_grid: config.show_grid,
                color_scheme: config.color_scheme.clone(),
                extra_data: None,
            },
            statistics: Some(statistics),
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("No data available for polar plot").color(Color32::GRAY));
            });
            return;
        }
        
        // Calculate plot bounds
        let max_radius = data.points.iter().map(|p| (p.x * p.x + p.y * p.y).sqrt()).fold(0.0, f64::max);
        let plot_size = ui.available_size();
        let center = Pos2::new(plot_size.x / 2.0, plot_size.y / 2.0);
        let max_radius_pixels = plot_size.x.min(plot_size.y) / 2.0 - 50.0; // Leave margin for labels
        
        // Create custom polar plot
        let plot_rect = Rect::from_min_size(
            ui.cursor().min,
            plot_size
        );
        
        // Draw polar grid and labels
        if config.show_grid {
            draw_polar_grid(ui, center, max_radius_pixels as f32);
        }
        
        // Draw polar labels
        draw_polar_labels(ui, center, max_radius_pixels as f32);
        
        // Draw data points
        let scale_factor = max_radius_pixels as f64 / max_radius;
        
        for point in &data.points {
            let pixel_x = center.x + (point.x * scale_factor) as f32;
            let pixel_y = center.y - (point.y * scale_factor) as f32; // Flip Y for screen coordinates
            
            let point_size = point.size.unwrap_or(4.0);
            let point_color = point.color.unwrap_or(Color32::BLUE);
            
            // Draw point
            ui.painter().circle_filled(
                Pos2::new(pixel_x, pixel_y),
                point_size,
                point_color
            );
            
            // Add hover effect
            if let Some(pointer_pos) = ui.input(|i| i.pointer.hover_pos()) {
                let distance = ((pointer_pos.x - pixel_x).powi(2) + (pointer_pos.y - pixel_y).powi(2)).sqrt();
                if distance < point_size + 5.0 {
                    // Draw highlight circle
                    ui.painter().circle_stroke(
                        Pos2::new(pixel_x, pixel_y),
                        point_size + 3.0,
                        Stroke::new(2.0, Color32::WHITE)
                    );
                    
                    // Show tooltip
                    if config.show_tooltips {
                        let mut tooltip_text = String::new();
                        tooltip_text.push_str(&format!("Angle: {:.2}°\n", 
                            point.tooltip_data.get("Angle (deg)").unwrap_or(&"0".to_string()).parse::<f64>().unwrap_or(0.0)));
                        tooltip_text.push_str(&format!("Radius: {:.2}\n", 
                            point.tooltip_data.get("Radius").unwrap_or(&"0".to_string()).parse::<f64>().unwrap_or(0.0)));
                        
                        egui::show_tooltip_at_pointer(ui.ctx(), 
                            egui::LayerId::new(egui::Order::Tooltip, egui::Id::new("polar_tooltip")), 
                            egui::Id::new("polar_tooltip"), 
                            |ui| {
                                ui.label(tooltip_text);
                            });
                    }
                }
            }
        }
        
        // Draw title
        if !config.title.is_empty() {
            ui.painter().text(
                Pos2::new(center.x, 20.0),
                egui::Align2::CENTER_TOP,
                &config.title,
                egui::FontId::proportional(16.0),
                Color32::WHITE
            );
        }
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if config.show_legend {
            ui.group(|ui| {
                ui.label(RichText::new("Polar Plot Info").strong());
                ui.separator();
                
                ui.horizontal(|ui| {
                    ui.label("Data Points:");
                    ui.label(format!("{}", data.points.len()));
                });
                
                if let Some(stats) = &data.statistics {
                    ui.horizontal(|ui| {
                        ui.label("Mean Radius:");
                        ui.label(format!("{:.3}", stats.mean_y));
                    });
                    ui.horizontal(|ui| {
                        ui.label("Max Radius:");
                        ui.label(format!("{:.3}", stats.std_y));
                    });
                    if let Some(corr) = stats.correlation {
                        ui.horizontal(|ui| {
                            ui.label("Distribution:");
                            ui.label(if corr.abs() > 0.5 { "Concentrated" } 
                                   else { "Scattered" });
                        });
                    }
                }
                
                ui.separator();
                ui.label(RichText::new("Coordinate System:").strong());
                ui.label("• X-axis: Angle (0° to 360°)");
                ui.label("• Y-axis: Radius (distance from center)");
                ui.label("• Points are plotted in polar coordinates");
            });
        }
    }
}

#[derive(Debug)]
struct PolarDataPoint {
    angle: f64,
    radius: f64,
    color: Color32,
    size: f32,
}

fn calculate_polar_statistics(polar_data: &[PolarDataPoint]) -> super::DataStatistics {
    if polar_data.is_empty() {
        return super::DataStatistics {
            mean_x: 0.0,
            mean_y: 0.0,
            std_x: 0.0,
            std_y: 0.0,
            correlation: None,
            count: 0,
        };
    }
    
    let angles: Vec<f64> = polar_data.iter().map(|p| p.angle).collect();
    let radii: Vec<f64> = polar_data.iter().map(|p| p.radius).collect();
    
    let mean_angle = angles.iter().sum::<f64>() / angles.len() as f64;
    let mean_radius = radii.iter().sum::<f64>() / radii.len() as f64;
    
    let angle_variance = angles.iter()
        .map(|&a| (a - mean_angle).powi(2))
        .sum::<f64>() / angles.len() as f64;
    let radius_variance = radii.iter()
        .map(|&r| (r - mean_radius).powi(2))
        .sum::<f64>() / radii.len() as f64;
    
    let std_angle = angle_variance.sqrt();
    let std_radius = radius_variance.sqrt();
    
    // Calculate correlation between angle and radius
    let correlation = if std_angle > 0.0 && std_radius > 0.0 {
        let covariance = polar_data.iter()
            .map(|p| (p.angle - mean_angle) * (p.radius - mean_radius))
            .sum::<f64>() / polar_data.len() as f64;
        Some(covariance / (std_angle * std_radius))
    } else {
        None
    };
    
    super::DataStatistics {
        mean_x: mean_angle,
        mean_y: mean_radius,
        std_x: std_angle,
        std_y: std_radius,
        correlation,
        count: polar_data.len(),
    }
}

fn draw_polar_grid(ui: &mut Ui, center: Pos2, max_radius: f32) {
    let painter = ui.painter();
    
    // Draw concentric circles
    for i in 1..=5 {
        let radius = (max_radius * i as f32) / 5.0;
        painter.circle_stroke(
            center,
            radius,
            Stroke::new(1.0, Color32::from_gray(80))
        );
    }
    
            // Draw radial lines (every 30 degrees)
        for i in 0..12 {
            let angle = (i as f64 * 30.0) * PI / 180.0;
            let end_x = center.x + max_radius * angle.cos() as f32;
            let end_y = center.y - max_radius * angle.sin() as f32; // Flip Y for screen coordinates
        
        painter.line_segment(
            [center, Pos2::new(end_x, end_y)],
            Stroke::new(1.0, Color32::from_gray(80))
        );
    }
}

fn draw_polar_labels(ui: &mut Ui, center: Pos2, max_radius: f32) {
    let painter = ui.painter();
    
    // Draw angle labels
    for i in 0..12 {
        let angle = (i as f64 * 30.0) * PI / 180.0;
        let label_radius = max_radius + 20.0;
        let label_x = center.x + label_radius * angle.cos() as f32;
        let label_y = center.y - label_radius * angle.sin() as f32; // Flip Y for screen coordinates
        
        let angle_deg = i * 30;
        painter.text(
            Pos2::new(label_x, label_y),
            egui::Align2::CENTER_CENTER,
            &format!("{}°", angle_deg),
            egui::FontId::proportional(12.0),
            Color32::from_gray(150)
        );
    }
    
    // Draw radius labels
    for i in 1..=5 {
        let radius = (max_radius * i as f32) / 5.0;
        let label_x = center.x + radius + 10.0;
        let label_y = center.y;
        
        painter.text(
            Pos2::new(label_x, label_y),
            egui::Align2::LEFT_CENTER,
            &format!("{:.1}", radius),
            egui::FontId::proportional(10.0),
            Color32::from_gray(150)
        );
    }
}
