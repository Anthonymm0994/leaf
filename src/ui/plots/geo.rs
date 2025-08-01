use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct GeoPlot;

impl PlotTrait for GeoPlot {
    fn name(&self) -> &'static str {
        "Geographic Plot"
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
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for geographic plots".to_string());
        }
        
        // For large datasets, sample the data
        let max_points = 5000; // Limit for performance
        let sample_size = query_result.rows.len().min(max_points);
        let step = if query_result.rows.len() > max_points {
            query_result.rows.len() / max_points
        } else {
            1
        };
        
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
        
        let mut points = Vec::new();
        let mut geo_data = Vec::new();
        
        for (row_idx, row) in query_result.rows.iter().enumerate().step_by(step) {
            if row.len() > x_idx && row.len() > y_idx {
                // Parse longitude (X) value
                let lon_val = row[x_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse longitude value '{}' as number", row[x_idx]))?;
                
                // Parse latitude (Y) value
                let lat_val = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse latitude value '{}' as number", row[y_idx]))?;
                
                // Validate coordinates
                if lon_val < -180.0 || lon_val > 180.0 {
                    return Err(format!("Invalid longitude value: {}", lon_val));
                }
                if lat_val < -90.0 || lat_val > 90.0 {
                    return Err(format!("Invalid latitude value: {}", lat_val));
                }
                
                // Create color mapping
                let color = if let Some(color_idx) = color_idx {
                    if row.len() > color_idx {
                        let color_value = &row[color_idx];
                        if let Ok(num_val) = color_value.parse::<f64>() {
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
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Longitude".to_string(), lon_val.to_string());
                tooltip_data.insert("Latitude".to_string(), lat_val.to_string());
                tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
                
                points.push(PlotPoint {
                    x: lon_val,
                    y: lat_val,
                    z: None,
                    label: None,
                    color: Some(color),
                    size: None,
                    series_id: None,
                    tooltip_data,
                });
                
                geo_data.push(GeoDataPoint {
                    longitude: lon_val,
                    latitude: lat_val,
                    color,
                });
            }
        }
        
        // Calculate geographic statistics
        let statistics = calculate_geo_statistics(&geo_data);
        
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
                ui.label(RichText::new("No data available for geographic plot").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Geographic Plot").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Data Points:");
                ui.label(format!("{}", data.points.len()));
            });
            
            if let Some(first_point) = data.points.first() {
                ui.horizontal(|ui| {
                    ui.label("Longitude Range:");
                    ui.label(format!("{:.2}° to {:.2}°", 
                        data.points.iter().map(|p| p.x).fold(f64::INFINITY, f64::min),
                        data.points.iter().map(|p| p.x).fold(f64::NEG_INFINITY, f64::max)
                    ));
                });
                
                ui.horizontal(|ui| {
                    ui.label("Latitude Range:");
                    ui.label(format!("{:.2}° to {:.2}°", 
                        data.points.iter().map(|p| p.y).fold(f64::INFINITY, f64::min),
                        data.points.iter().map(|p| p.y).fold(f64::NEG_INFINITY, f64::max)
                    ));
                });
            }
            
            // Show geographic statistics
            if let Some(stats) = &data.statistics {
                ui.separator();
                ui.label(RichText::new("Geographic Statistics").strong());
                ui.horizontal(|ui| {
                    ui.label("Center Longitude:");
                    ui.label(format!("{:.3}°", stats.mean_x));
                });
                ui.horizontal(|ui| {
                    ui.label("Center Latitude:");
                    ui.label(format!("{:.3}°", stats.mean_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Geographic Spread:");
                    ui.label(format!("{:.3}°", stats.std_y));
                });
            }
            
            ui.separator();
            
            // Geographic plot visualization
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(400.0));
            
            ui.allocate_ui(plot_size, |ui| {
                render_geo_plot(ui, data, config, plot_size);
            });
            
            // Configuration panel
            ui.separator();
            ui.label(RichText::new("Configuration").strong());
            ui.horizontal(|ui| {
                ui.label("Projection:");
                ui.radio_value(&mut 0, 0, "Mercator");
                ui.radio_value(&mut 0, 1, "Equal Area");
                ui.radio_value(&mut 0, 2, "Orthographic");
            });
            
            ui.horizontal(|ui| {
                ui.label("Show Grid:");
                ui.checkbox(&mut true, "");
            });
            
            ui.horizontal(|ui| {
                ui.label("Show Labels:");
                ui.checkbox(&mut true, "");
            });
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() && config.show_legend {
            ui.group(|ui| {
                ui.label(RichText::new("Geographic Points:").strong());
                ui.separator();
                
                for (i, point) in data.points.iter().take(10).enumerate() {
                    ui.horizontal(|ui| {
                        if let Some(color) = point.color {
                            ui.colored_label(color, "●");
                        }
                        ui.label(format!("Point {}", i + 1));
                    });
                }
                
                if data.points.len() > 10 {
                    ui.label(format!("... and {} more points", data.points.len() - 10));
                }
            });
        }
    }
    
    fn handle_interaction(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) -> Option<super::PlotInteraction> {
        // Handle hover and selection for geographic plot
        if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
            for point in &data.points {
                let point_pos = Pos2::new(point.x as f32, point.y as f32);
                if (hover_pos - point_pos).length() < 10.0 {
                    // Show tooltip
                    ui.label(format!("Longitude: {:.3}° | Latitude: {:.3}°", point.x, point.y));
                    break;
                }
            }
        }
        
        None
    }
}

/// Geographic data point structure
#[derive(Debug, Clone)]
struct GeoDataPoint {
    longitude: f64,
    latitude: f64,
    color: Color32,
}

/// Calculate geographic statistics
fn calculate_geo_statistics(geo_data: &[GeoDataPoint]) -> super::DataStatistics {
    if geo_data.is_empty() {
        return super::DataStatistics {
            mean_x: 0.0,
            mean_y: 0.0,
            std_x: 0.0,
            std_y: 0.0,
            correlation: None,
            count: 0,
        };
    }
    
    let longitudes: Vec<f64> = geo_data.iter().map(|d| d.longitude).collect();
    let latitudes: Vec<f64> = geo_data.iter().map(|d| d.latitude).collect();
    
    let mean_lon = longitudes.iter().sum::<f64>() / longitudes.len() as f64;
    let mean_lat = latitudes.iter().sum::<f64>() / latitudes.len() as f64;
    
    let variance_lon = longitudes.iter()
        .map(|l| (l - mean_lon).powi(2))
        .sum::<f64>() / longitudes.len() as f64;
    let std_lon = variance_lon.sqrt();
    
    let variance_lat = latitudes.iter()
        .map(|l| (l - mean_lat).powi(2))
        .sum::<f64>() / latitudes.len() as f64;
    let std_lat = variance_lat.sqrt();
    
    // Calculate correlation between longitude and latitude
    let correlation = if std_lon > 0.0 && std_lat > 0.0 {
        let covariance = longitudes.iter().zip(latitudes.iter())
            .map(|(l, lat)| (l - mean_lon) * (lat - mean_lat))
            .sum::<f64>() / longitudes.len() as f64;
        Some(covariance / (std_lon * std_lat))
    } else {
        None
    };
    
    super::DataStatistics {
        mean_x: mean_lon,
        mean_y: mean_lat,
        std_x: std_lon,
        std_y: std_lat,
        correlation,
        count: longitudes.len(),
    }
}

/// Render geographic plot
fn render_geo_plot(ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration, size: Vec2) {
    if data.points.is_empty() {
        return;
    }
    
    let margin = 50.0;
    let plot_width = size.x - 2.0 * margin;
    let plot_height = size.y - 2.0 * margin;
    
    // Draw world map outline (simplified)
    draw_world_map(ui, margin, plot_width, plot_height);
    
    // Draw data points
    for point in &data.points {
        let color = point.color.unwrap_or(Color32::BLUE);
        
        // Convert geographic coordinates to screen coordinates
        let x = (margin as f64 + (point.x + 180.0) / 360.0 * plot_width as f64) as f32;
        let y = (margin as f64 + (90.0 - point.y) / 180.0 * plot_height as f64) as f32;
        
        // Draw point
        ui.painter().circle_filled(
            Pos2::new(x, y),
            4.0,
            color,
        );
        
        // Draw point border
        ui.painter().circle_stroke(
            Pos2::new(x, y),
            4.0,
            Stroke::new(1.0, Color32::BLACK),
        );
    }
    
    // Draw grid lines
    draw_geo_grid(ui, margin, plot_width, plot_height);
    
    // Draw axis labels
    draw_geo_labels(ui, margin, plot_width, plot_height);
}

/// Draw simplified world map outline
fn draw_world_map(ui: &mut Ui, margin: f32, width: f32, height: f32) {
    // Draw continent outlines (simplified)
    let continents = vec![
        // North America
        vec![
            Pos2::new(margin + 0.1 * width, margin + 0.3 * height),
            Pos2::new(margin + 0.3 * width, margin + 0.2 * height),
            Pos2::new(margin + 0.2 * width, margin + 0.4 * height),
        ],
        // South America
        vec![
            Pos2::new(margin + 0.25 * width, margin + 0.5 * height),
            Pos2::new(margin + 0.3 * width, margin + 0.8 * height),
            Pos2::new(margin + 0.2 * width, margin + 0.7 * height),
        ],
        // Europe
        vec![
            Pos2::new(margin + 0.45 * width, margin + 0.3 * height),
            Pos2::new(margin + 0.55 * width, margin + 0.25 * height),
            Pos2::new(margin + 0.5 * width, margin + 0.35 * height),
        ],
        // Africa
        vec![
            Pos2::new(margin + 0.5 * width, margin + 0.4 * height),
            Pos2::new(margin + 0.55 * width, margin + 0.7 * height),
            Pos2::new(margin + 0.45 * width, margin + 0.6 * height),
        ],
        // Asia
        vec![
            Pos2::new(margin + 0.6 * width, margin + 0.3 * height),
            Pos2::new(margin + 0.9 * width, margin + 0.25 * height),
            Pos2::new(margin + 0.8 * width, margin + 0.4 * height),
        ],
    ];
    
    for continent in continents {
        if continent.len() >= 3 {
            ui.painter().add(egui::Shape::convex_polygon(
                continent,
                Color32::from_gray(240),
                Stroke::new(1.0, Color32::from_gray(200)),
            ));
        }
    }
}

/// Draw geographic grid lines
fn draw_geo_grid(ui: &mut Ui, margin: f32, width: f32, height: f32) {
    // Draw longitude lines (meridians)
    for i in 0..=12 {
        let x = margin + (i as f32 / 12.0) * width;
        ui.painter().line_segment(
            [Pos2::new(x, margin), Pos2::new(x, margin + height)],
            Stroke::new(1.0, Color32::from_gray(220)),
        );
    }
    
    // Draw latitude lines (parallels)
    for i in 0..=6 {
        let y = margin + (i as f32 / 6.0) * height;
        ui.painter().line_segment(
            [Pos2::new(margin, y), Pos2::new(margin + width, y)],
            Stroke::new(1.0, Color32::from_gray(220)),
        );
    }
}

/// Draw geographic labels
fn draw_geo_labels(ui: &mut Ui, margin: f32, width: f32, height: f32) {
    // Draw longitude labels
    for i in 0..=6 {
        let x = margin + (i as f32 / 6.0) * width;
        let lon = (i as f32 - 3.0) * 60.0;
        ui.painter().text(
            Pos2::new(x, margin + height + 15.0),
            egui::Align2::CENTER_TOP,
            &format!("{:.0}°", lon),
            egui::FontId::proportional(10.0),
            Color32::GRAY,
        );
    }
    
    // Draw latitude labels
    for i in 0..=4 {
        let y = margin + (i as f32 / 4.0) * height;
        let lat = 90.0 - (i as f32 / 4.0) * 180.0;
        ui.painter().text(
            Pos2::new(margin - 15.0, y),
            egui::Align2::RIGHT_CENTER,
            &format!("{:.0}°", lat),
            egui::FontId::proportional(10.0),
            Color32::GRAY,
        );
    }
}
