use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct ParallelCoordinatesPlot;

impl ParallelCoordinatesPlot {
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

impl PlotTrait for ParallelCoordinatesPlot {
    fn name(&self) -> &'static str {
        "Parallel Coordinates"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        None // Parallel coordinates don't require a specific X column
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
        // For parallel coordinates, we need multiple numeric columns
        // We'll use the Y column as the primary column and find other numeric columns
        if config.y_column.is_empty() {
            return Err("At least one Y column is required for parallel coordinates".to_string());
        }
        
        // Find all numeric columns for parallel coordinates
        let mut numeric_columns = Vec::new();
        let mut column_indices = Vec::new();
        
        // Start with the Y column
        if let Some(idx) = query_result.columns.iter().position(|c| c == &config.y_column) {
            numeric_columns.push(&config.y_column);
            column_indices.push(idx);
        } else {
            return Err(format!("Column '{}' not found", config.y_column));
        }
        
        // Find additional numeric columns (up to 6 total for readability)
        for (i, col) in query_result.columns.iter().enumerate() {
            if numeric_columns.len() >= 6 {
                break; // Limit to 6 dimensions for readability
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
        
        if numeric_columns.len() < 2 {
            return Err("Need at least 2 numeric columns for parallel coordinates".to_string());
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
        
        // Store additional data for parallel coordinates
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
                ui.label(RichText::new("No data available for parallel coordinates plot").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Parallel Coordinates Plot").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Data Points:");
                ui.label(format!("{}", data.points.len()));
            });
            
            ui.horizontal(|ui| {
                ui.label("Dimensions:");
                ui.label(format!("{}", 3)); // Simplified for now
            });
            
            ui.separator();
            
            // Parallel coordinates visualization
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(400.0));
            
            ui.allocate_ui(plot_size, |ui| {
                render_parallel_coordinates(ui, data, config, plot_size);
            });
            
            // Controls
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Display Mode:");
                ui.radio_value(&mut 0, 0, "Lines");
                ui.radio_value(&mut 0, 1, "Points");
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

/// Render parallel coordinates plot
fn render_parallel_coordinates(ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration, size: Vec2) {
    let margin = 50.0;
    let plot_width = size.x - 2.0 * margin;
    let plot_height = size.y - 2.0 * margin;
    
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
    
    if num_dimensions < 2 {
        ui.centered_and_justified(|ui| {
            ui.label("Need at least 2 dimensions for parallel coordinates");
        });
        return;
    }
    
    let dimension_width = plot_width / (num_dimensions - 1) as f32;
    
    // Draw dimension axes
    for (i, dimension) in columns.iter().enumerate() {
        let x = margin + i as f32 * dimension_width;
        
        // Draw axis line
        ui.painter().line_segment(
            [Pos2::new(x, margin), Pos2::new(x, margin + plot_height)],
            Stroke::new(2.0, Color32::from_gray(120)),
        );
        
        // Draw axis label
        ui.painter().text(
            Pos2::new(x, margin + plot_height + 20.0),
            egui::Align2::CENTER_TOP,
            dimension,
            egui::FontId::proportional(12.0),
            Color32::WHITE,
        );
        
        // Draw min and max labels if we have range data
        if i < column_ranges.len() {
            let (min_val, max_val) = column_ranges[i];
            ui.painter().text(
                Pos2::new(x, margin + plot_height + 5.0),
                egui::Align2::CENTER_TOP,
                &format!("{:.1}", max_val),
                egui::FontId::proportional(10.0),
                Color32::from_gray(150),
            );
            
            ui.painter().text(
                Pos2::new(x, margin - 5.0),
                egui::Align2::CENTER_BOTTOM,
                &format!("{:.1}", min_val),
                egui::FontId::proportional(10.0),
                Color32::from_gray(150),
            );
        }
    }
    
    // Draw data lines
    for (point_idx, point) in data.points.iter().enumerate() {
        let color = point.color.unwrap_or(Color32::BLUE);
        
        if point_idx < series_data.len() {
            let values = &series_data[point_idx];
            
            // Draw polyline connecting all dimensions
            let mut line_points = Vec::new();
            for (i, &value) in values.iter().enumerate() {
                let x = margin + i as f32 * dimension_width;
                let y = margin + plot_height - (value * plot_height as f64) as f32;
                line_points.push(Pos2::new(x, y));
            }
            
            if line_points.len() >= 2 {
                // Draw the polyline with alpha for better visibility
                for i in 0..line_points.len() - 1 {
                    ui.painter().line_segment(
                        [line_points[i], line_points[i + 1]],
                        Stroke::new(1.5, Color32::from_rgba_unmultiplied(
                            color.r(), color.g(), color.b(), 180
                        )),
                    );
                }
                
                // Draw points at each dimension
                for &point_pos in &line_points {
                    ui.painter().circle_filled(
                        point_pos,
                        3.0,
                        color,
                    );
                }
            }
        }
    }
    
    // Draw grid lines for better readability
    for i in 0..num_dimensions {
        let x = margin + i as f32 * dimension_width;
        
        // Draw horizontal grid lines
        for j in 0..=10 {
            let y = margin + (j as f32 / 10.0) * plot_height;
            ui.painter().line_segment(
                [Pos2::new(x - 2.0, y), Pos2::new(x + 2.0, y)],
                Stroke::new(1.0, Color32::from_gray(60)),
            );
        }
    }
}
