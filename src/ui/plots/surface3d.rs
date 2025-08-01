use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct Surface3dPlot;

impl PlotTrait for Surface3dPlot {
    fn name(&self) -> &'static str {
        "3D Surface Plot"
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
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for 3D surface plots".to_string());
        }
        
        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column)
            .ok_or("X column not found")?;
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or("Y column not found")?;
        
        // Find Z column (use Y as Z if no separate Z column)
        let z_idx = if let Some(z_col) = &config.color_column {
            if !z_col.is_empty() {
                query_result.columns.iter().position(|c| c == z_col)
            } else {
                None
            }
        } else {
            None
        };
        
        let mut points = Vec::new();
        let mut x_values = Vec::new();
        let mut y_values = Vec::new();
        let mut z_values = Vec::new();
        
        for (row_idx, row) in query_result.rows.iter().enumerate() {
            if row.len() > x_idx && row.len() > y_idx {
                let x_val = row[x_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse X value '{}' as number", row[x_idx]))?;
                let y_val = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse Y value '{}' as number", row[y_idx]))?;
                
                let z_val = if let Some(z_idx) = z_idx {
                    if row.len() > z_idx {
                        row[z_idx].parse::<f64>().unwrap_or(0.0)
                    } else {
                        0.0
                    }
                } else {
                    y_val // Use Y value as Z if no separate Z column
                };
                
                x_values.push(x_val);
                y_values.push(y_val);
                z_values.push(z_val);
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("X".to_string(), x_val.to_string());
                tooltip_data.insert("Y".to_string(), y_val.to_string());
                tooltip_data.insert("Z".to_string(), z_val.to_string());
                tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
                
                points.push(PlotPoint {
                    x: x_val,
                    y: y_val,
                    z: Some(z_val),
                    label: None,
                    color: None,
                    size: None,
                    series_id: None,
                    tooltip_data,
                });
            }
        }
        
        // Create surface mesh data
        let surface_data = create_surface_mesh(&x_values, &y_values, &z_values);
        
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
            statistics: None,
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("No data available for 3D surface plot").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("3D Surface Plot").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Points:");
                ui.label(format!("{}", data.points.len()));
            });
            
            if let Some(first_point) = data.points.first() {
                ui.horizontal(|ui| {
                    ui.label("X Range:");
                    ui.label(format!("{:.2} to {:.2}", 
                        data.points.iter().map(|p| p.x).fold(f64::INFINITY, f64::min),
                        data.points.iter().map(|p| p.x).fold(f64::NEG_INFINITY, f64::max)
                    ));
                });
                
                ui.horizontal(|ui| {
                    ui.label("Y Range:");
                    ui.label(format!("{:.2} to {:.2}", 
                        data.points.iter().map(|p| p.y).fold(f64::INFINITY, f64::min),
                        data.points.iter().map(|p| p.y).fold(f64::NEG_INFINITY, f64::max)
                    ));
                });
                
                if let Some(z_val) = first_point.z {
                    ui.horizontal(|ui| {
                        ui.label("Z Range:");
                        ui.label(format!("{:.2} to {:.2}", 
                            data.points.iter().filter_map(|p| p.z).fold(f64::INFINITY, f64::min),
                            data.points.iter().filter_map(|p| p.z).fold(f64::NEG_INFINITY, f64::max)
                        ));
                    });
                }
            }
            
            ui.separator();
            
            // 3D Surface visualization using 2D projection
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(300.0));
            
            ui.allocate_ui(plot_size, |ui| {
                // Create a 2D projection of the 3D surface
                let plot = Plot::new("surface_3d")
                    .height(plot_size.y)
                    .allow_zoom(true)
                    .allow_drag(true)
                    .show_grid(true);
                
                plot.show(ui, |plot_ui| {
                    // Render surface as contour plot
                    render_surface_contour(plot_ui, data, config);
                    
                    // Render surface as heatmap
                    render_surface_heatmap(plot_ui, data, config);
                });
            });
            
            // Controls
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("View Mode:");
                ui.radio_value(&mut 0, 0, "Contour");
                ui.radio_value(&mut 0, 1, "Heatmap");
                ui.radio_value(&mut 0, 2, "Wireframe");
            });
            
            ui.horizontal(|ui| {
                ui.label("Color Scheme:");
                ui.radio_value(&mut 0, 0, "Viridis");
                ui.radio_value(&mut 0, 1, "Plasma");
                ui.radio_value(&mut 0, 2, "Blues");
            });
        });
    }
}

/// Create surface mesh data from scattered points
fn create_surface_mesh(x_values: &[f64], y_values: &[f64], z_values: &[f64]) -> Vec<(f64, f64, f64)> {
    if x_values.len() < 3 {
        return vec![];
    }
    
    // Simple triangulation for surface mesh
    let mut mesh = Vec::new();
    
    // Create a grid-based surface if we have enough points
    let x_min = x_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let x_max = x_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let y_min = y_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let y_max = y_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    
    let grid_size = 20;
    let dx = (x_max - x_min) / (grid_size - 1) as f64;
    let dy = (y_max - y_min) / (grid_size - 1) as f64;
    
    for i in 0..grid_size {
        for j in 0..grid_size {
            let x = x_min + i as f64 * dx;
            let y = y_min + j as f64 * dy;
            
            // Find closest point for Z value
            let mut min_dist = f64::INFINITY;
            let mut z = 0.0;
            
            for k in 0..x_values.len() {
                let dist = ((x - x_values[k]).powi(2) + (y - y_values[k]).powi(2)).sqrt();
                if dist < min_dist {
                    min_dist = dist;
                    z = z_values[k];
                }
            }
            
            mesh.push((x, y, z));
        }
    }
    
    mesh
}

/// Render surface as contour plot
fn render_surface_contour(plot_ui: &mut PlotUi, data: &PlotData, _config: &PlotConfiguration) {
    if data.points.is_empty() {
        return;
    }
    
    // Create contour lines
    let x_values: Vec<f64> = data.points.iter().map(|p| p.x).collect();
    let y_values: Vec<f64> = data.points.iter().map(|p| p.y).collect();
    let z_values: Vec<f64> = data.points.iter().filter_map(|p| p.z).collect();
    
    if z_values.is_empty() {
        return;
    }
    
    // Create contour levels
    let z_min = z_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let z_max = z_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let levels = 10;
    let dz = (z_max - z_min) / levels as f64;
    
    for level in 0..=levels {
        let contour_z = z_min + level as f64 * dz;
        
        // Simple contour line approximation
        let mut contour_points = Vec::new();
        for i in 0..data.points.len() - 1 {
            let z1 = data.points[i].z.unwrap_or(0.0);
            let z2 = data.points[i + 1].z.unwrap_or(0.0);
            
            if (z1 <= contour_z && z2 >= contour_z) || (z1 >= contour_z && z2 <= contour_z) {
                let t = (contour_z - z1) / (z2 - z1);
                let x = data.points[i].x + t * (data.points[i + 1].x - data.points[i].x);
                let y = data.points[i].y + t * (data.points[i + 1].y - data.points[i].y);
                contour_points.push([x, y]);
            }
        }
        
        if !contour_points.is_empty() {
            let color = Color32::from_rgb(
                (level * 25) as u8,
                (255 - level * 25) as u8,
                128
            );
            
            plot_ui.line(Line::new(PlotPoints::from(contour_points))
                .color(color)
                .width(1.0));
        }
    }
}

/// Render surface as heatmap
fn render_surface_heatmap(plot_ui: &mut PlotUi, data: &PlotData, _config: &PlotConfiguration) {
    if data.points.is_empty() {
        return;
    }
    
    // Create heatmap points
    let points: Vec<[f64; 2]> = data.points.iter()
        .map(|p| [p.x, p.y])
        .collect();
    
    // Color points by Z value
    let z_values: Vec<f64> = data.points.iter().filter_map(|p| p.z).collect();
    if !z_values.is_empty() {
        let z_min = z_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let z_max = z_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        for (i, point) in points.iter().enumerate() {
            if i < z_values.len() {
                let normalized_z = (z_values[i] - z_min) / (z_max - z_min);
                let color = Color32::from_rgb(
                    (normalized_z * 255.0) as u8,
                    ((1.0 - normalized_z) * 255.0) as u8,
                    128
                );
                
                plot_ui.points(egui_plot::Points::new(PlotPoints::from(vec![*point]))
                    .color(color)
                    .radius(3.0));
            }
        }
    }
}
