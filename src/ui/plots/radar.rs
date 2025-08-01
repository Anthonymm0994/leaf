use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;
use std::f64::consts::PI;

pub struct RadarPlot;

impl RadarPlot {
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
}

impl PlotTrait for RadarPlot {
    fn name(&self) -> &'static str {
        "Radar Chart"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        None // Radar charts don't require a specific X column
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Float64, DataType::Int64, DataType::Utf8]
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("Color", vec![DataType::Float64, DataType::Int64, DataType::Utf8]),
            ("Size", vec![DataType::Float64, DataType::Int64]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    fn supports_multiple_series(&self) -> bool { true }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        // For radar charts, we need multiple numeric columns
        // We'll use the Y column as the primary column and find other numeric columns
        if config.y_column.is_empty() {
            return Err("At least one Y column is required for radar charts".to_string());
        }
        
        // Find all numeric columns for radar chart
        let mut numeric_columns = Vec::new();
        let mut column_indices = Vec::new();
        
        // Start with the Y column
        if let Some(idx) = query_result.columns.iter().position(|c| c == &config.y_column) {
            numeric_columns.push(&config.y_column);
            column_indices.push(idx);
        } else {
            return Err(format!("Column '{}' not found", config.y_column));
        }
        
        // Find additional numeric columns (up to 8 total for readability)
        for (i, col) in query_result.columns.iter().enumerate() {
            if numeric_columns.len() >= 8 {
                break; // Limit to 8 dimensions for readability
            }
            
            if col != &config.y_column && !numeric_columns.contains(&col) {
                // Check if this column contains numeric data
                let mut has_numeric_data = false;
                for row in &query_result.rows {
                    if row.len() > i {
                        if row[i].parse::<f64>().is_ok() {
                            has_numeric_data = true;
                            break;
                        }
                    }
                }
                
                if has_numeric_data {
                    numeric_columns.push(col);
                    column_indices.push(i);
                }
            }
        }
        
        if numeric_columns.len() < 3 {
            return Err("Need at least 3 numeric columns for radar chart".to_string());
        }
        
        // Calculate min/max for each column for normalization
        let mut column_ranges = Vec::new();
        for &col_idx in &column_indices {
            let mut min_val = f64::INFINITY;
            let mut max_val = f64::NEG_INFINITY;
            
            for row in &query_result.rows {
                if row.len() > col_idx {
                    if let Ok(val) = row[col_idx].parse::<f64>() {
                        min_val = min_val.min(val);
                        max_val = max_val.max(val);
                    }
                }
            }
            
            column_ranges.push((min_val, max_val));
        }
        
        let mut points = Vec::new();
        let mut series_data = Vec::new();
        
        for (row_idx, row) in query_result.rows.iter().enumerate() {
            if row.len() >= column_indices.iter().max().unwrap_or(&0) + 1 {
                let mut point_data = Vec::new();
                let mut tooltip_data = HashMap::new();
                
                // Extract and normalize values for each column
                for (i, &col_idx) in column_indices.iter().enumerate() {
                    if row.len() > col_idx {
                        let value = row[col_idx].parse::<f64>().unwrap_or(0.0);
                        let (min_val, max_val) = column_ranges[i];
                        let range = max_val - min_val;
                        
                        // Normalize to 0-1 range
                        let normalized = if range > 0.0 {
                            (value - min_val) / range
                        } else {
                            0.5
                        };
                        
                        point_data.push(normalized);
                        tooltip_data.insert(query_result.columns[col_idx].clone(), format!("{:.3}", value));
                    } else {
                        point_data.push(0.0);
                    }
                }
                
                // Create color mapping
                let color = if let Some(color_col) = &config.color_column {
                    if !color_col.is_empty() {
                        let color_idx = query_result.columns.iter().position(|c| c == color_col);
                        if let Some(idx) = color_idx {
                            if row.len() > idx {
                                let color_value = &row[idx];
                                // Create color based on value
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
                        }
                    } else {
                        Color32::BLUE
                    }
                } else {
                    // Use a gradient based on row index
                    let hue = (row_idx as f32 * 137.5) % 360.0; // Golden angle
                    let (r, g, b) = self.hsv_to_rgb(hue, 0.8, 0.9);
                    Color32::from_rgb(r, g, b)
                };
                
                points.push(PlotPoint {
                    x: 0.0, // Will be set during rendering
                    y: 0.0, // Will be set during rendering
                    z: None,
                    label: None,
                    color: Some(color),
                    size: None,
                    series_id: Some(format!("row_{}", row_idx)),
                    tooltip_data,
                });
                
                series_data.push(point_data);
            }
        }
        
        // Store the column information in metadata for rendering
        let mut metadata = super::PlotMetadata {
            title: config.title.clone(),
            x_label: "Dimensions".to_string(),
            y_label: "Values".to_string(),
            show_legend: config.show_legend,
            show_grid: config.show_grid,
            color_scheme: config.color_scheme.clone(),
            extra_data: None,
        };
        
        // Store additional data for radar chart
        metadata.extra_data = Some(serde_json::json!({
            "columns": numeric_columns,
            "column_ranges": column_ranges,
            "series_data": series_data
        }));
        
        Ok(PlotData {
            points,
            series: vec![],
            metadata,
            statistics: None,
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("No data available for radar chart").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Radar Chart").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Data Points:");
                ui.label(format!("{}", data.points.len()));
            });
            
            ui.horizontal(|ui| {
                ui.label("Dimensions:");
                ui.label(format!("{}", 5)); // Simplified for now
            });
            
            ui.separator();
            
            // Radar chart visualization
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(400.0));
            
            ui.allocate_ui(plot_size, |ui| {
                render_radar_chart(ui, data, config, plot_size);
            });
            
            // Controls
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Display Mode:");
                ui.radio_value(&mut 0, 0, "Lines");
                ui.radio_value(&mut 0, 1, "Area");
                ui.radio_value(&mut 0, 2, "Both");
            });
            
            ui.horizontal(|ui| {
                ui.label("Color By:");
                ui.radio_value(&mut 0, 0, "None");
                ui.radio_value(&mut 0, 1, "First Column");
                ui.radio_value(&mut 0, 2, "Last Column");
            });
        });
    }
}

/// Render radar chart
fn render_radar_chart(ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration, size: Vec2) {
    let center = Pos2::new(size.x / 2.0, size.y / 2.0);
    let radius = (size.x.min(size.y) / 2.0 - 50.0).max(50.0);
    
    // Extract column information from metadata
    let columns = if let Some(extra_data) = &data.metadata.extra_data {
        if let Some(cols) = extra_data.get("columns") {
            if let Some(col_array) = cols.as_array() {
                col_array.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    } else {
        vec![]
    };
    
    let column_ranges = if let Some(extra_data) = &data.metadata.extra_data {
        if let Some(ranges) = extra_data.get("column_ranges") {
            if let Some(range_array) = ranges.as_array() {
                range_array.iter()
                    .filter_map(|v| {
                        if let Some(arr) = v.as_array() {
                            if arr.len() == 2 {
                                Some((arr[0].as_f64().unwrap_or(0.0), arr[1].as_f64().unwrap_or(1.0)))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    } else {
        vec![]
    };
    
    let series_data = if let Some(extra_data) = &data.metadata.extra_data {
        if let Some(series) = extra_data.get("series_data") {
            if let Some(series_array) = series.as_array() {
                series_array.iter()
                    .filter_map(|v| {
                        if let Some(arr) = v.as_array() {
                            Some(arr.iter()
                                .filter_map(|val| val.as_f64())
                                .collect::<Vec<_>>())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    } else {
        vec![]
    };
    
    let num_dimensions = columns.len();
    
    if num_dimensions < 3 {
        ui.centered_and_justified(|ui| {
            ui.label("Need at least 3 dimensions for radar chart");
        });
        return;
    }
    
    // Draw radar grid
    draw_radar_grid(ui, center, radius, num_dimensions);
    
    // Draw dimension labels
    draw_radar_labels(ui, center, radius, &columns);
    
    // Draw data polygons
    for (point_idx, point) in data.points.iter().enumerate() {
        let color = point.color.unwrap_or(Color32::BLUE);
        
        if point_idx < series_data.len() {
            let values = &series_data[point_idx];
            
            // Draw radar polygon
            draw_radar_polygon(ui, center, radius, values, color);
        }
    }
}

/// Draw radar grid (concentric circles and radial lines)
fn draw_radar_grid(ui: &mut Ui, center: Pos2, radius: f32, num_dimensions: usize) {
    // Draw concentric circles with different opacities
    for i in 1..=5 {
        let circle_radius = radius * i as f32 / 5.0;
        let alpha = (255 - (i * 40)) as u8;
        ui.painter().circle_stroke(
            center,
            circle_radius,
            Stroke::new(1.0, Color32::from_rgba_unmultiplied(150, 150, 150, alpha)),
        );
    }
    
    // Draw radial lines
    for i in 0..num_dimensions {
        let angle = 2.0 * PI * i as f64 / num_dimensions as f64 - PI / 2.0;
        let end_x = center.x + (radius * angle.cos() as f32);
        let end_y = center.y + (radius * angle.sin() as f32);
        
        ui.painter().line_segment(
            [center, Pos2::new(end_x, end_y)],
            Stroke::new(1.0, Color32::from_gray(120)),
        );
    }
    
    // Draw center point
    ui.painter().circle_filled(center, 3.0, Color32::WHITE);
}

/// Draw dimension labels around the radar
fn draw_radar_labels(ui: &mut Ui, center: Pos2, radius: f32, dimensions: &[&str]) {
    let label_radius = radius + 30.0;
    
    for (i, dimension) in dimensions.iter().enumerate() {
        let angle = 2.0 * PI * i as f64 / dimensions.len() as f64 - PI / 2.0;
        let x = center.x + (label_radius * angle.cos() as f32);
        let y = center.y + (label_radius * angle.sin() as f32);
        
        // Truncate long labels to prevent overlap
        let display_label = if dimension.len() > 12 {
            &dimension[..12]
        } else {
            dimension
        };
        
        ui.painter().text(
            Pos2::new(x, y),
            egui::Align2::CENTER_CENTER,
            display_label,
            egui::FontId::proportional(11.0),
            Color32::WHITE,
        );
    }
}

/// Draw radar polygon for a data series
fn draw_radar_polygon(ui: &mut Ui, center: Pos2, radius: f32, values: &[f64], color: Color32) {
    if values.is_empty() {
        return;
    }
    
    let num_dimensions = values.len();
    let mut polygon_points = Vec::new();
    
    // Calculate polygon vertices
    for (i, &value) in values.iter().enumerate() {
        let angle = 2.0 * PI * i as f64 / num_dimensions as f64 - PI / 2.0;
        let point_radius = radius * value as f32;
        let x = center.x + (point_radius * angle.cos() as f32);
        let y = center.y + (point_radius * angle.sin() as f32);
        polygon_points.push(Pos2::new(x, y));
    }
    
    // Draw filled polygon with alpha
    if polygon_points.len() >= 3 {
        let fill_color = Color32::from_rgba_unmultiplied(
            color.r(), color.g(), color.b(), 80
        );
        ui.painter().add(egui::Shape::convex_polygon(
            polygon_points.clone(),
            fill_color,
            Stroke::new(2.0, color),
        ));
    }
    
    // Draw polygon outline
    if polygon_points.len() >= 2 {
        for i in 0..polygon_points.len() {
            let start = polygon_points[i];
            let end = polygon_points[(i + 1) % polygon_points.len()];
            
            ui.painter().line_segment(
                [start, end],
                Stroke::new(2.5, color),
            );
        }
    }
    
    // Draw vertices with enhanced styling
    for &point in &polygon_points {
        // Outer glow
        ui.painter().circle_filled(
            point,
            6.0,
            Color32::from_rgba_unmultiplied(
                color.r(), color.g(), color.b(), 60
            ),
        );
        
        // Inner core
        ui.painter().circle_filled(
            point,
            4.0,
            color,
        );
        
        // Highlight
        ui.painter().circle_stroke(
            point,
            4.0,
            Stroke::new(1.0, Color32::WHITE),
        );
    }
}
