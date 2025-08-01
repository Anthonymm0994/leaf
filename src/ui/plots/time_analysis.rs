use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct TimeAnalysisPlot;

impl PlotTrait for TimeAnalysisPlot {
    fn name(&self) -> &'static str {
        "Time Series Analysis"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![DataType::Utf8, DataType::Int64, DataType::Float64])
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
            return Err("X and Y columns are required for time series analysis".to_string());
        }
        
        // For large datasets, sample the data
        let max_points = 10000; // Limit for performance
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
        let mut time_values = Vec::new();
        let mut y_values = Vec::new();
        
        for (row_idx, row) in query_result.rows.iter().enumerate().step_by(step) {
            if row.len() > x_idx && row.len() > y_idx {
                // Parse time value
                let time_val = if let Ok(timestamp) = row[x_idx].parse::<f64>() {
                    timestamp
                } else {
                    // Try to parse as date string or use row index
                    row_idx as f64
                };
                
                let y_val = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse Y value '{}' as number", row[y_idx]))?;
                
                time_values.push(time_val);
                y_values.push(y_val);
                
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
                tooltip_data.insert("Value".to_string(), y_val.to_string());
                tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
                
                points.push(PlotPoint {
                    x: time_val,
                    y: y_val,
                    z: None,
                    label: None,
                    color: Some(color),
                    size: None,
                    series_id: None,
                    tooltip_data,
                });
            }
        }
        
        // Sort by time
        points.sort_by(|a, b| a.x.partial_cmp(&b.x).unwrap_or(std::cmp::Ordering::Equal));
        
        // Calculate time series statistics
        let statistics = calculate_time_series_statistics(&time_values, &y_values);
        
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
                ui.label(RichText::new("No data available for time series analysis").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Time Series Analysis").heading());
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
            
            // Show time series statistics
            if let Some(stats) = &data.statistics {
                ui.separator();
                ui.label(RichText::new("Time Series Statistics").strong());
                ui.horizontal(|ui| {
                    ui.label("Trend:");
                    ui.label(if stats.correlation.unwrap_or(0.0) > 0.1 { "Increasing" } 
                           else if stats.correlation.unwrap_or(0.0) < -0.1 { "Decreasing" } 
                           else { "Stable" });
                });
                ui.horizontal(|ui| {
                    ui.label("Volatility:");
                    ui.label(format!("{:.3}", stats.std_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Mean Value:");
                    ui.label(format!("{:.3}", stats.mean_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Trend Strength:");
                    ui.label(format!("{:.3}", stats.correlation.unwrap_or(0.0).abs()));
                });
            }
            
            ui.separator();
            
            // Time series visualization
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(300.0));
            
            ui.allocate_ui(plot_size, |ui| {
                let plot = Plot::new("time_series")
                    .view_aspect(2.0)
                    .include_y(0.0)
                    .include_y(100.0);
                
                plot.show(ui, |plot_ui| {
                    render_time_series_line(plot_ui, data, config);
                    render_moving_average(plot_ui, data, config);
                    render_trend_line(plot_ui, data, config);
                });
            });
            
            // Configuration panel
            ui.separator();
            ui.label(RichText::new("Configuration").strong());
            ui.horizontal(|ui| {
                ui.label("Analysis Type:");
                ui.radio_value(&mut 0, 0, "Trend");
                ui.radio_value(&mut 0, 1, "Seasonality");
                ui.radio_value(&mut 0, 2, "Decomposition");
            });
            
            ui.horizontal(|ui| {
                ui.label("Show Moving Average:");
                ui.checkbox(&mut true, "");
            });
            
            ui.horizontal(|ui| {
                ui.label("Show Trend Line:");
                ui.checkbox(&mut true, "");
            });
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() && config.show_legend {
            ui.group(|ui| {
                ui.label(RichText::new("Time Series Components:").strong());
                ui.separator();
                
                ui.horizontal(|ui| {
                    ui.colored_label(Color32::BLUE, "●");
                    ui.label("Original Data");
                });
                
                ui.horizontal(|ui| {
                    ui.colored_label(Color32::RED, "●");
                    ui.label("Moving Average");
                });
                
                ui.horizontal(|ui| {
                    ui.colored_label(Color32::GREEN, "●");
                    ui.label("Trend Line");
                });
            });
        }
    }
    
    fn handle_interaction(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) -> Option<super::PlotInteraction> {
        // Handle hover and selection for time series
        if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
            for point in &data.points {
                let point_pos = Pos2::new(point.x as f32, point.y as f32);
                if (hover_pos - point_pos).length() < 10.0 {
                    // Show tooltip
                    ui.label(format!("Time: {:.3} | Value: {:.3}", point.x, point.y));
                    break;
                }
            }
        }
        
        None
    }
}

/// Calculate time series statistics
fn calculate_time_series_statistics(time_values: &[f64], y_values: &[f64]) -> super::DataStatistics {
    if y_values.is_empty() {
        return super::DataStatistics {
            mean_x: 0.0,
            mean_y: 0.0,
            std_x: 0.0,
            std_y: 0.0,
            correlation: None,
            count: 0,
        };
    }
    
    let mean_y = y_values.iter().sum::<f64>() / y_values.len() as f64;
    let mean_x = time_values.iter().sum::<f64>() / time_values.len() as f64;
    
    let variance_y = y_values.iter()
        .map(|y| (y - mean_y).powi(2))
        .sum::<f64>() / y_values.len() as f64;
    let std_y = variance_y.sqrt();
    
    let variance_x = time_values.iter()
        .map(|x| (x - mean_x).powi(2))
        .sum::<f64>() / time_values.len() as f64;
    let std_x = variance_x.sqrt();
    
    // Calculate correlation
    let correlation = if std_x > 0.0 && std_y > 0.0 {
        let covariance = time_values.iter().zip(y_values.iter())
            .map(|(x, y)| (x - mean_x) * (y - mean_y))
            .sum::<f64>() / time_values.len() as f64;
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
        count: y_values.len(),
    }
}

/// Render time series line
fn render_time_series_line(plot_ui: &mut PlotUi, data: &PlotData, _config: &PlotConfiguration) {
    if data.points.len() < 2 {
        return;
    }
    
    let points: Vec<[f64; 2]> = data.points.iter()
        .map(|p| [p.x, p.y])
        .collect();
    
    plot_ui.line(Line::new(PlotPoints::from(points))
        .color(Color32::BLUE)
        .width(2.0));
    
    // Draw points
    for point in &data.points {
        let color = point.color.unwrap_or(Color32::BLUE);
        plot_ui.points(egui_plot::Points::new(PlotPoints::from(vec![[point.x, point.y]]))
            .color(color)
            .radius(3.0));
    }
}

/// Render moving average
fn render_moving_average(plot_ui: &mut PlotUi, data: &PlotData, _config: &PlotConfiguration) {
    if data.points.len() < 5 {
        return;
    }
    
    let window_size = 5;
    let mut moving_avg_points = Vec::new();
    
    for i in window_size - 1..data.points.len() {
        let window_sum: f64 = data.points[i - window_size + 1..=i]
            .iter()
            .map(|p| p.y)
            .sum();
        let avg = window_sum / window_size as f64;
        moving_avg_points.push([data.points[i].x, avg]);
    }
    
    if !moving_avg_points.is_empty() {
        plot_ui.line(Line::new(PlotPoints::from(moving_avg_points))
            .color(Color32::RED)
            .width(2.0));
    }
}

/// Render trend line
fn render_trend_line(plot_ui: &mut PlotUi, data: &PlotData, _config: &PlotConfiguration) {
    if data.points.len() < 2 {
        return;
    }
    
    // Calculate linear regression
    let n = data.points.len() as f64;
    let sum_x: f64 = data.points.iter().map(|p| p.x).sum();
    let sum_y: f64 = data.points.iter().map(|p| p.y).sum();
    let sum_xy: f64 = data.points.iter().map(|p| p.x * p.y).sum();
    let sum_x2: f64 = data.points.iter().map(|p| p.x * p.x).sum();
    
    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x);
    let intercept = (sum_y - slope * sum_x) / n;
    
    // Draw trend line
    let x_min = data.points.iter().map(|p| p.x).fold(f64::INFINITY, f64::min);
    let x_max = data.points.iter().map(|p| p.x).fold(f64::NEG_INFINITY, f64::max);
    
    let y1 = slope * x_min + intercept;
    let y2 = slope * x_max + intercept;
    
    plot_ui.line(Line::new(PlotPoints::from(vec![[x_min, y1], [x_max, y2]]))
        .color(Color32::GREEN)
        .width(2.0));
}
