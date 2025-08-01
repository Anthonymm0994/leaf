use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct StreamPlot;

impl PlotTrait for StreamPlot {
    fn name(&self) -> &'static str {
        "Stream Graph"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![DataType::Float64, DataType::Int64, DataType::Utf8])
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
    fn supports_multiple_series(&self) -> bool { true }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for stream graphs".to_string());
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
        
        let mut points = Vec::new();
        let mut stream_data = Vec::new();
        
        for (row_idx, row) in query_result.rows.iter().enumerate() {
            if row.len() > x_idx && row.len() > y_idx {
                // Parse time value
                let time_val = if let Ok(timestamp) = row[x_idx].parse::<f64>() {
                    timestamp
                } else {
                    // Try to parse as date string or use row index
                    row_idx as f64
                };
                
                let value = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse Y value '{}' as number", row[y_idx]))?;
                
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
                tooltip_data.insert("Time".to_string(), time_val.to_string());
                tooltip_data.insert("Value".to_string(), value.to_string());
                tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
                
                points.push(PlotPoint {
                    x: time_val,
                    y: value,
                    z: None,
                    label: None,
                    color: Some(color),
                    size: None,
                    series_id: None,
                    tooltip_data,
                });
                
                stream_data.push(StreamDataPoint {
                    time: time_val,
                    value,
                    color,
                });
            }
        }
        
        // Sort by time
        points.sort_by(|a, b| a.x.partial_cmp(&b.x).unwrap_or(std::cmp::Ordering::Equal));
        
        // Calculate stream statistics
        let statistics = calculate_stream_statistics(&stream_data);
        
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
                ui.label(RichText::new("No data available for stream graph").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Stream Graph").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Data Points:");
                ui.label(format!("{}", data.points.len()));
            });
            
            if let Some(first_point) = data.points.first() {
                let last_point = data.points.last().unwrap();
                ui.horizontal(|ui| {
                    ui.label("Time Range:");
                    ui.label(format!("{:.2} to {:.2}", first_point.x, last_point.x));
                });
                
                ui.horizontal(|ui| {
                    ui.label("Value Range:");
                    ui.label(format!("{:.2} to {:.2}", 
                        data.points.iter().map(|p| p.y).fold(f64::INFINITY, f64::min),
                        data.points.iter().map(|p| p.y).fold(f64::NEG_INFINITY, f64::max)
                    ));
                });
            }
            
            // Show stream statistics
            if let Some(stats) = &data.statistics {
                ui.separator();
                ui.label(RichText::new("Stream Statistics").strong());
                ui.horizontal(|ui| {
                    ui.label("Mean Value:");
                    ui.label(format!("{:.3}", stats.mean_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Total Flow:");
                    ui.label(format!("{:.3}", stats.mean_y * data.points.len() as f64));
                });
                if let Some(corr) = stats.correlation {
                    ui.horizontal(|ui| {
                        ui.label("Trend:");
                        ui.label(if corr > 0.1 { "Increasing" } 
                               else if corr < -0.1 { "Decreasing" } 
                               else { "Stable" });
                    });
                }
            }
            
            ui.separator();
            
            // Stream graph visualization
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(300.0));
            
            ui.allocate_ui(plot_size, |ui| {
                render_stream_graph(ui, data, config, plot_size);
            });
            
            // Controls
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Stream Type:");
                ui.radio_value(&mut 0, 0, "Centered");
                ui.radio_value(&mut 0, 1, "Stacked");
                ui.radio_value(&mut 0, 2, "Mirrored");
            });
            
            ui.horizontal(|ui| {
                ui.label("Color Scheme:");
                ui.radio_value(&mut 0, 0, "Sequential");
                ui.radio_value(&mut 0, 1, "Categorical");
                ui.radio_value(&mut 0, 2, "Diverging");
            });
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration) {
        if !data.series.is_empty() {
            ui.group(|ui| {
                ui.label(RichText::new("Series:").strong());
                ui.separator();
                
                for series in &data.series {
                    if series.visible {
                        ui.horizontal(|ui| {
                            ui.colored_label(series.color, "—");
                            ui.label(&series.name);
                        });
                    }
                }
            });
        }
    }
    
    fn handle_interaction(&self, ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration) -> Option<super::PlotInteraction> {
        if !data.series.is_empty() {
            ui.vertical(|ui| {
                for series in &data.series {
                    let mut is_visible = series.visible;
                    if ui.checkbox(&mut is_visible, &series.name).changed() {
                        return Some(super::PlotInteraction::SeriesToggled(series.id.clone()));
                    }
                }
                
                None
            }).inner
        } else {
            None
        }
    }
}

/// Stream data point structure
#[derive(Debug, Clone)]
struct StreamDataPoint {
    time: f64,
    value: f64,
    color: Color32,
}

/// Calculate stream statistics
fn calculate_stream_statistics(stream_data: &[StreamDataPoint]) -> super::DataStatistics {
    if stream_data.is_empty() {
        return super::DataStatistics {
            mean_x: 0.0,
            mean_y: 0.0,
            std_x: 0.0,
            std_y: 0.0,
            correlation: None,
            count: 0,
        };
    }
    
    let values: Vec<f64> = stream_data.iter().map(|d| d.value).collect();
    let times: Vec<f64> = stream_data.iter().map(|d| d.time).collect();
    
    let mean_y = values.iter().sum::<f64>() / values.len() as f64;
    let mean_x = times.iter().sum::<f64>() / times.len() as f64;
    
    let variance_y = values.iter()
        .map(|y| (y - mean_y).powi(2))
        .sum::<f64>() / values.len() as f64;
    let std_y = variance_y.sqrt();
    
    let variance_x = times.iter()
        .map(|x| (x - mean_x).powi(2))
        .sum::<f64>() / times.len() as f64;
    let std_x = variance_x.sqrt();
    
    // Calculate correlation
    let correlation = if std_x > 0.0 && std_y > 0.0 {
        let covariance = times.iter().zip(values.iter())
            .map(|(x, y)| (x - mean_x) * (y - mean_y))
            .sum::<f64>() / times.len() as f64;
        Some(covariance / (std_x * std_y))
    } else {
        None
    };
    
    super::DataStatistics {
        mean_x,
        mean_y,
        std_x,
        std_y,
        correlation,
        count: values.len(),
    }
}

/// Render stream graph
fn render_stream_graph(ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration, size: Vec2) {
    if data.points.is_empty() {
        return;
    }
    
    let margin = 50.0;
    let plot_width = size.x - 2.0 * margin;
    let plot_height = size.y - 2.0 * margin;
    
    // Generate multiple streams for demonstration
    let num_streams = 3;
    let mut streams = Vec::new();
    
    for stream_idx in 0..num_streams {
        let mut stream_points = Vec::new();
        for (i, point) in data.points.iter().enumerate() {
            let time = point.x;
            let base_value = point.y;
            let stream_value = base_value * (0.5 + 0.5 * (stream_idx as f64 + 1.0) / num_streams as f64);
            stream_points.push((time, stream_value));
        }
        streams.push(stream_points);
    }
    
    // Draw stream areas
    for (stream_idx, stream_points) in streams.iter().enumerate() {
        let color = Color32::from_rgb(
            (stream_idx * 80) as u8,
            (255 - stream_idx * 80) as u8,
            128
        );
        
        // Create stream area
        let mut area_points = Vec::new();
        
        // Add points for upper boundary
        for &(time, value) in stream_points {
            let x = (margin as f64 + (time / data.points.last().unwrap().x) * plot_width as f64) as f32;
            let y = (margin as f64 + plot_height as f64 - (value / 10.0) * plot_height as f64) as f32;
            area_points.push(Pos2::new(x, y));
        }
        
        // Add points for lower boundary (mirror)
        for &(time, value) in stream_points.iter().rev() {
            let x = (margin as f64 + (time / data.points.last().unwrap().x) * plot_width as f64) as f32;
            let y = (margin as f64 + plot_height as f64 + (value / 10.0) * plot_height as f64) as f32;
            area_points.push(Pos2::new(x, y));
        }
        
        // Draw filled area
        if area_points.len() >= 3 {
            ui.painter().add(egui::Shape::convex_polygon(
                area_points,
                color.linear_multiply(0.3),
                Stroke::new(1.0, color),
            ));
        }
    }
    
    // Draw center line
    ui.painter().line_segment(
        [Pos2::new(margin, margin + plot_height), 
         Pos2::new(margin + plot_width, margin + plot_height)],
        Stroke::new(2.0, Color32::BLACK),
    );
    
    // Draw axis labels
    ui.painter().text(
        Pos2::new(margin + plot_width / 2.0, margin + plot_height + 30.0),
        egui::Align2::CENTER_TOP,
        "Time",
        egui::FontId::proportional(12.0),
        Color32::BLACK,
    );
    
    ui.painter().text(
        Pos2::new(margin - 30.0, margin + plot_height / 2.0),
        egui::Align2::CENTER_CENTER,
        "Value",
        egui::FontId::proportional(12.0),
        Color32::BLACK,
    );
}
