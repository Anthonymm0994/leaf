use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, DataSeries, PlotMetadata, ColorScheme};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke, Sense, Key, FontId, Align2};
use egui_plot::{Plot, PlotPoints, PlotBounds, Points, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;
use glam::{Vec3, Mat4, Quat, Vec4Swizzles};
use std::f32::consts::PI;

pub struct Scatter3DPlot;

/// 3D camera for projection
#[derive(Debug, Clone)]
struct Camera3D {
    position: Vec3,
    target: Vec3,
    up: Vec3,
    fov: f32,
    yaw: f32,
    pitch: f32,
    distance: f32,
}

impl Default for Camera3D {
    fn default() -> Self {
        Self {
            position: Vec3::new(5.0, 5.0, 5.0),
            target: Vec3::ZERO,
            up: Vec3::Y,
            fov: 45.0_f32.to_radians(),
            yaw: -PI / 4.0,
            pitch: PI / 6.0,
            distance: 10.0,
        }
    }
}

impl Camera3D {
    fn update_from_angles(&mut self) {
        self.position = Vec3::new(
            self.distance * self.yaw.cos() * self.pitch.cos(),
            self.distance * self.pitch.sin(),
            self.distance * self.yaw.sin() * self.pitch.cos(),
        );
    }
    
    fn view_matrix(&self) -> Mat4 {
        Mat4::look_at_rh(self.position, self.target, self.up)
    }
    
    fn projection_matrix(&self, aspect_ratio: f32) -> Mat4 {
        Mat4::perspective_rh(self.fov, aspect_ratio, 0.1, 100.0)
    }
}

/// 3D point for rendering
#[derive(Clone)]
struct Point3D {
    position: Vec3,
    color: Color32,
    size: f32,
    label: Option<String>,
    index: usize,
}

impl Scatter3DPlot {
    /// Project 3D point to 2D screen coordinates
    fn project_point(&self, point: Vec3, camera: &Camera3D, rect: &Rect) -> Option<(Pos2, f32)> {
        let view = camera.view_matrix();
        let proj = camera.projection_matrix(rect.width() / rect.height());
        let mvp = proj * view;
        
        let clip_pos = mvp * glam::Vec4::new(point.x, point.y, point.z, 1.0);
        
        if clip_pos.w <= 0.0 {
            return None; // Behind camera
        }
        
        let ndc = clip_pos.xyz() / clip_pos.w;
        
        if ndc.x < -1.0 || ndc.x > 1.0 || ndc.y < -1.0 || ndc.y > 1.0 || ndc.z < -1.0 || ndc.z > 1.0 {
            return None; // Outside view frustum
        }
        
        let screen_x = rect.min.x + (ndc.x + 1.0) * 0.5 * rect.width();
        let screen_y = rect.min.y + (1.0 - ndc.y) * 0.5 * rect.height();
        
        Some((Pos2::new(screen_x, screen_y), ndc.z))
    }
    
    /// Draw 3D axes with improved styling
    fn draw_axes(&self, ui: &mut Ui, rect: Rect, camera: &Camera3D) {
        let painter = ui.painter();
        let axis_length = 1.5;
        
        // X axis (red)
        let x_start = self.project_point(Vec3::ZERO, camera, &rect);
        let x_end = self.project_point(Vec3::new(axis_length, 0.0, 0.0), camera, &rect);
        if let (Some(start), Some(end)) = (x_start, x_end) {
            // Draw axis line with gradient
            painter.line_segment([start.0, end.0], Stroke::new(3.0, Color32::RED));
            
            // Draw arrowhead
            let arrow_size = 8.0;
            let direction = (end.0 - start.0).normalized();
            let perpendicular = Vec2::new(-direction.y, direction.x);
            let arrow_tip = end.0;
            let arrow_base = arrow_tip - direction * arrow_size;
            let arrow_left = arrow_base + perpendicular * (arrow_size * 0.5);
            let arrow_right = arrow_base - perpendicular * (arrow_size * 0.5);
            
            painter.add(egui::Shape::convex_polygon(
                vec![arrow_tip, arrow_left, arrow_right],
                Color32::RED,
                Stroke::NONE,
            ));
            
            // Draw axis label with background
            let label_pos = end.0 + direction * 15.0;
            painter.text(
                label_pos,
                Align2::CENTER_CENTER,
                "X",
                FontId::proportional(14.0),
                Color32::WHITE,
            );
        }
        
        // Y axis (green)
        let y_end = self.project_point(Vec3::new(0.0, axis_length, 0.0), camera, &rect);
        if let (Some(start), Some(end)) = (x_start, y_end) {
            painter.line_segment([start.0, end.0], Stroke::new(3.0, Color32::GREEN));
            
            // Draw arrowhead
            let arrow_size = 8.0;
            let direction = (end.0 - start.0).normalized();
            let perpendicular = Vec2::new(-direction.y, direction.x);
            let arrow_tip = end.0;
            let arrow_base = arrow_tip - direction * arrow_size;
            let arrow_left = arrow_base + perpendicular * (arrow_size * 0.5);
            let arrow_right = arrow_base - perpendicular * (arrow_size * 0.5);
            
            painter.add(egui::Shape::convex_polygon(
                vec![arrow_tip, arrow_left, arrow_right],
                Color32::GREEN,
                Stroke::NONE,
            ));
            
            let label_pos = end.0 + direction * 15.0;
            painter.text(
                label_pos,
                Align2::CENTER_CENTER,
                "Y",
                FontId::proportional(14.0),
                Color32::WHITE,
            );
        }
        
        // Z axis (blue)
        let z_end = self.project_point(Vec3::new(0.0, 0.0, axis_length), camera, &rect);
        if let (Some(start), Some(end)) = (x_start, z_end) {
            painter.line_segment([start.0, end.0], Stroke::new(3.0, Color32::BLUE));
            
            // Draw arrowhead
            let arrow_size = 8.0;
            let direction = (end.0 - start.0).normalized();
            let perpendicular = Vec2::new(-direction.y, direction.x);
            let arrow_tip = end.0;
            let arrow_base = arrow_tip - direction * arrow_size;
            let arrow_left = arrow_base + perpendicular * (arrow_size * 0.5);
            let arrow_right = arrow_base - perpendicular * (arrow_size * 0.5);
            
            painter.add(egui::Shape::convex_polygon(
                vec![arrow_tip, arrow_left, arrow_right],
                Color32::BLUE,
                Stroke::NONE,
            ));
            
            let label_pos = end.0 + direction * 15.0;
            painter.text(
                label_pos,
                Align2::CENTER_CENTER,
                "Z",
                FontId::proportional(14.0),
                Color32::WHITE,
            );
        }
        
        // Draw origin point
        if let Some(origin) = x_start {
            painter.circle_filled(origin.0, 4.0, Color32::WHITE);
            painter.circle_stroke(origin.0, 4.0, Stroke::new(1.0, Color32::BLACK));
        }
    }
    
    /// Draw 3D points with improved rendering
    fn draw_points(&self, ui: &mut Ui, rect: Rect, points: &[Point3D], camera: &Camera3D) {
        let painter = ui.painter();
        
        // Sort points by depth for proper rendering order
        let mut visible_points: Vec<_> = points.iter()
            .filter_map(|point| {
                self.project_point(point.position, camera, &rect)
                    .map(|(screen_pos, depth)| (point, screen_pos, depth))
            })
            .collect();
        
        // Sort by depth (back to front)
        visible_points.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
        
        let visible_points_len = visible_points.len();
        for (point, screen_pos, depth) in visible_points {
            // Enhanced depth-based effects
            let depth_factor = (1.0 - depth).max(0.2);
            let alpha = (depth_factor * 255.0) as u8;
            let size_factor = depth_factor * 0.8 + 0.2; // Points closer to camera are larger
            
            // Create enhanced color with depth effects
            let base_color = point.color;
            let enhanced_color = Color32::from_rgba_unmultiplied(
                base_color.r(),
                base_color.g(),
                base_color.b(),
                alpha,
            );
            
            // Draw point with gradient effect
            let outer_size = point.size * size_factor;
            let inner_size = outer_size * 0.6;
            
            // Outer glow
            painter.circle_filled(screen_pos, outer_size, enhanced_color);
            
            // Inner core
            let core_color = Color32::from_rgba_unmultiplied(
                (base_color.r() as f32 * 1.2).min(255.0) as u8,
                (base_color.g() as f32 * 1.2).min(255.0) as u8,
                (base_color.b() as f32 * 1.2).min(255.0) as u8,
                alpha,
            );
            painter.circle_filled(screen_pos, inner_size, core_color);
            
            // Highlight for front points
            if depth_factor > 0.8 {
                painter.circle_stroke(screen_pos, outer_size + 1.0, Stroke::new(1.0, Color32::WHITE));
            }
            
            // Only show labels for points close to camera and not too many
            if depth_factor > 0.7 && visible_points_len <= 50 {
                if let Some(ref label) = point.label {
                    // Use shorter labels to reduce clutter
                    let short_label = if label.len() > 8 {
                        format!("{}", &label[..8])
                    } else {
                        label.clone()
                    };
                    
                    painter.text(
                        screen_pos + Vec2::new(outer_size + 3.0, 0.0),
                        Align2::LEFT_CENTER,
                        &short_label,
                        FontId::proportional(9.0),
                        Color32::from_rgba_unmultiplied(255, 255, 255, alpha),
                    );
                }
            }
        }
    }
    
    /// Convert HSV to RGB
    fn hsv_to_rgb(&self, h: f32, s: f32, v: f32) -> (u8, u8, u8) {
        let c = v * s;
        let x = c * (1.0 - ((h / 60.0) % 2.0 - 1.0).abs());
        let m = v - c;
        
        let (r, g, b) = if h < 60.0 {
            (c, x, 0.0)
        } else if h < 120.0 {
            (x, c, 0.0)
        } else if h < 180.0 {
            (0.0, c, x)
        } else if h < 240.0 {
            (0.0, x, c)
        } else if h < 300.0 {
            (x, 0.0, c)
        } else {
            (c, 0.0, x)
        };
        
        (
            ((r + m) * 255.0).clamp(0.0, 255.0) as u8,
            ((g + m) * 255.0).clamp(0.0, 255.0) as u8,
            ((b + m) * 255.0).clamp(0.0, 255.0) as u8,
        )
    }
    
    /// Handle 3D camera interaction
    fn handle_camera_interaction(&self, ui: &mut Ui, rect: Rect, camera: &mut Camera3D) -> Response {
        let response = ui.allocate_response(rect.size(), Sense::drag());
        
        if response.dragged_by(egui::PointerButton::Primary) {
            let delta = response.drag_delta();
            camera.yaw += delta.x * 0.01;
            camera.pitch += delta.y * 0.01;
            camera.pitch = camera.pitch.clamp(-PI/2.0 + 0.1, PI/2.0 - 0.1);
            camera.update_from_angles();
        }
        
        if response.hovered() {
            let scroll = ui.input(|i| i.raw_scroll_delta.y);
            if scroll != 0.0 {
                camera.distance = (camera.distance - scroll * 0.1).max(1.0).min(50.0);
                camera.update_from_angles();
            }
        }
        
        response
    }
}

impl PlotTrait for Scatter3DPlot {
    fn name(&self) -> &'static str { 
        "3D Scatter Plot" 
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> { 
        Some(vec![DataType::Float64, DataType::Int64]) 
    }
    
    fn required_y_types(&self) -> Vec<DataType> { 
        vec![DataType::Float64, DataType::Int64] 
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("Z", vec![DataType::Float64, DataType::Int64]),
            ("Color", vec![DataType::Float64, DataType::Int64, DataType::Utf8]),
            ("Size", vec![DataType::Float64, DataType::Int64]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    fn supports_size_mapping(&self) -> bool { true }
    fn supports_multiple_series(&self) -> bool { true }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for 3D scatter plot".to_string());
        }
        
        // Find column indices
        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column)
            .ok_or_else(|| format!("X column '{}' not found", config.x_column))?;
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or_else(|| format!("Y column '{}' not found", config.y_column))?;
        
        // Find Z column (use a default if not specified)
        let z_column = if let Some(z_col) = query_result.columns.iter().find(|c| *c == "z" || *c == "Z") {
            z_col.clone()
        } else {
            // Use Y column as Z if no Z column found
            config.y_column.clone()
        };
        let z_idx = query_result.columns.iter().position(|c| c == &z_column)
            .ok_or_else(|| format!("Z column '{}' not found", z_column))?;
        
        let mut points = Vec::new();
        let mut series = Vec::new();
        
        // Extract color and size columns
        let color_idx = config.color_column.as_ref()
            .and_then(|col| query_result.columns.iter().position(|c| c == col));
        let size_idx = config.size_column.as_ref()
            .and_then(|col| query_result.columns.iter().position(|c| c == col));
        
        // Find min/max for normalization
        let mut x_min = f64::INFINITY;
        let mut x_max = f64::NEG_INFINITY;
        let mut y_min = f64::INFINITY;
        let mut y_max = f64::NEG_INFINITY;
        let mut z_min = f64::INFINITY;
        let mut z_max = f64::NEG_INFINITY;
        
        for row in &query_result.rows {
            if row.len() > x_idx.max(y_idx).max(z_idx) {
                if let (Ok(x), Ok(y), Ok(z)) = (
                    row[x_idx].parse::<f64>(),
                    row[y_idx].parse::<f64>(),
                    row[z_idx].parse::<f64>()
                ) {
                    x_min = x_min.min(x);
                    x_max = x_max.max(x);
                    y_min = y_min.min(y);
                    y_max = y_max.max(y);
                    z_min = z_min.min(z);
                    z_max = z_max.max(z);
                }
            }
        }
        
        // Normalize ranges to -1 to 1
        let x_range = x_max - x_min;
        let y_range = y_max - y_min;
        let z_range = z_max - z_min;
        
        for (i, row) in query_result.rows.iter().enumerate() {
            if row.len() > x_idx.max(y_idx).max(z_idx) {
                if let (Ok(x), Ok(y), Ok(z)) = (
                    row[x_idx].parse::<f64>(),
                    row[y_idx].parse::<f64>(),
                    row[z_idx].parse::<f64>()
                ) {
                    // Normalize coordinates to -1 to 1
                    let norm_x = if x_range > 0.0 { (x - x_min) / x_range * 2.0 - 1.0 } else { 0.0 };
                    let norm_y = if y_range > 0.0 { (y - y_min) / y_range * 2.0 - 1.0 } else { 0.0 };
                    let norm_z = if z_range > 0.0 { (z - z_min) / z_range * 2.0 - 1.0 } else { 0.0 };
                    
                    // Determine color with improved color mapping
                    let color = if let Some(color_idx) = color_idx {
                        if row.len() > color_idx {
                            if let Ok(color_val) = row[color_idx].parse::<f64>() {
                                // Use a more sophisticated color scheme
                                let normalized = (color_val - x_min) / x_range.max(1.0);
                                let normalized = normalized.clamp(0.0, 1.0);
                                
                                // Viridis-like color scheme
                                if normalized < 0.25 {
                                    let t = normalized / 0.25;
                                    Color32::from_rgb(
                                        (68.0 + (1.0 - t) * 187.0) as u8,
                                        (1.0 + t * 198.0) as u8,
                                        (84.0 + t * 112.0) as u8,
                                    )
                                } else if normalized < 0.5 {
                                    let t = (normalized - 0.25) / 0.25;
                                    Color32::from_rgb(
                                        (255.0 - t * 187.0) as u8,
                                        (199.0 + t * 56.0) as u8,
                                        (196.0 - t * 112.0) as u8,
                                    )
                                } else if normalized < 0.75 {
                                    let t = (normalized - 0.5) / 0.25;
                                    Color32::from_rgb(
                                        (68.0 + t * 187.0) as u8,
                                        (255.0 - t * 56.0) as u8,
                                        (84.0 + t * 112.0) as u8,
                                    )
                                } else {
                                    let t = (normalized - 0.75) / 0.25;
                                    Color32::from_rgb(
                                        (255.0 - t * 187.0) as u8,
                                        (199.0 + t * 56.0) as u8,
                                        (196.0 + t * 59.0) as u8,
                                    )
                                }
                            } else {
                                Color32::from_rgb(100, 150, 255) // Default blue
                            }
                        } else {
                            Color32::from_rgb(100, 150, 255) // Default blue
                        }
                    } else {
                        // Use a gradient based on point index for better visual appeal
                        let hue = (i as f32 * 137.5) % 360.0; // Golden angle for good distribution
                        let (r, g, b) = self.hsv_to_rgb(hue, 0.8, 0.9);
                        Color32::from_rgb(r, g, b)
                    };
                    
                    // Determine size
                    let size = if let Some(size_idx) = size_idx {
                        if row.len() > size_idx {
                            if let Ok(size_val) = row[size_idx].parse::<f64>() {
                                (size_val / x_max.max(1.0) * 10.0).clamp(2.0, 15.0) as f32
                            } else {
                                5.0
                            }
                        } else {
                            5.0
                        }
                    } else {
                        5.0
                    };
                    
                    let mut tooltip_data = HashMap::new();
                    tooltip_data.insert("X".to_string(), format!("{:.3}", x));
                    tooltip_data.insert("Y".to_string(), format!("{:.3}", y));
                    tooltip_data.insert("Z".to_string(), format!("{:.3}", z));
                    
                    points.push(PlotPoint {
                        x: norm_x,
                        y: norm_y,
                        z: Some(norm_z),
                        label: Some(format!("Point {}", i)),
                        color: Some(color),
                        size: Some(size),
                        series_id: Some("3d_points".to_string()),
                        tooltip_data,
                    });
                }
            }
        }
        
        // Create series
        series.push(DataSeries {
            id: "3d_points".to_string(),
            name: "3D Points".to_string(),
            points: points.clone(),
            color: Color32::BLUE,
            visible: true,
            style: super::SeriesStyle::Points { size: 5.0, shape: super::MarkerShape::Circle },
        });
        
        Ok(PlotData {
            points,
            series,
            metadata: PlotMetadata {
                title: config.title.clone(),
                x_label: config.x_column.clone(),
                y_label: config.y_column.clone(),
                show_legend: config.show_legend,
                show_grid: config.show_grid,
                color_scheme: config.color_scheme.clone(),
                extra_data: None,
            },
            statistics: None,
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("No data available for 3D scatter plot").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("3D Scatter Plot").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Data Points:");
                ui.label(format!("{}", data.points.len()));
            });
            
            ui.separator();
            
            // 3D visualization area with improved styling
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(500.0));
            
            // Create a bordered area for the 3D plot
            ui.allocate_ui(plot_size, |ui| {
                let plot_area = ui.available_rect_before_wrap();
                
                // Draw background
                ui.painter().rect_filled(
                    plot_area,
                    4.0,
                    Color32::from_gray(25),
                );
                
                // Draw border
                ui.painter().rect_stroke(
                    plot_area,
                    2.0,
                    Stroke::new(1.0, Color32::from_gray(80)),
                );
                
                let mut camera = Camera3D::default();
                camera.update_from_angles();
                
                // Handle camera interaction
                let _response = self.handle_camera_interaction(ui, plot_area, &mut camera);
                
                // Convert PlotPoints to 3D points
                let mut points_3d = Vec::new();
                for (i, point) in data.points.iter().enumerate() {
                    if let Some(z) = point.z {
                        points_3d.push(Point3D {
                            position: Vec3::new(point.x as f32, point.y as f32, z as f32),
                            color: point.color.unwrap_or(Color32::BLUE),
                            size: point.size.unwrap_or(5.0),
                            label: point.label.clone(),
                            index: i,
                        });
                    }
                }
                
                // Draw 3D scene
                self.draw_axes(ui, plot_area, &camera);
                self.draw_points(ui, plot_area, &points_3d, &camera);
                
                // Show camera info with better styling
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Camera:").strong());
                    ui.label(format!("Yaw: {:.1}°", camera.yaw.to_degrees()));
                    ui.label(format!("Pitch: {:.1}°", camera.pitch.to_degrees()));
                    ui.label(format!("Distance: {:.1}", camera.distance));
                });
                
                // Controls with better styling
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Controls:").strong());
                    ui.label("🖱️ Drag to rotate");
                    ui.label("🔍 Scroll to zoom");
                });
            });
        });
    }
}
