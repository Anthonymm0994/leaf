use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct CandlestickPlot;

impl PlotTrait for CandlestickPlot {
    fn name(&self) -> &'static str {
        "Candlestick Chart"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![DataType::Utf8, DataType::Int64, DataType::Float64])
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Float64, DataType::Int64]
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("High", vec![DataType::Float64, DataType::Int64]),
            ("Low", vec![DataType::Float64, DataType::Int64]),
            ("Close", vec![DataType::Float64, DataType::Int64]),
            ("Volume", vec![DataType::Float64, DataType::Int64]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for candlestick charts".to_string());
        }
        
        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column)
            .ok_or("X column not found")?;
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or("Y column not found")?;
        
        // Find OHLC columns
        let high_idx = if let Some(high_col) = &config.color_column {
            if !high_col.is_empty() {
                query_result.columns.iter().position(|c| c == high_col)
            } else {
                None
            }
        } else {
            None
        };
        
        let low_idx = if let Some(low_col) = &config.size_column {
            if !low_col.is_empty() {
                query_result.columns.iter().position(|c| c == low_col)
            } else {
                None
            }
        } else {
            None
        };
        
        let mut points = Vec::new();
        let mut candlestick_data = Vec::new();
        
        for (row_idx, row) in query_result.rows.iter().enumerate() {
            if row.len() > x_idx && row.len() > y_idx {
                // Parse time value
                let time_val = if let Ok(timestamp) = row[x_idx].parse::<f64>() {
                    timestamp
                } else {
                    // Try to parse as date string or use row index
                    row_idx as f64
                };
                
                let open_val = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse Open value '{}' as number", row[y_idx]))?;
                
                // Parse High, Low, Close values
                let high_val = if let Some(high_idx) = high_idx {
                    if row.len() > high_idx {
                        row[high_idx].parse::<f64>().unwrap_or(open_val)
                    } else {
                        open_val
                    }
                } else {
                    open_val * 1.02 // Default high
                };
                
                let low_val = if let Some(low_idx) = low_idx {
                    if row.len() > low_idx {
                        row[low_idx].parse::<f64>().unwrap_or(open_val)
                    } else {
                        open_val
                    }
                } else {
                    open_val * 0.98 // Default low
                };
                
                // Use open as close if no close column
                let close_val = open_val;
                
                // Determine if bullish or bearish
                let is_bullish = close_val >= open_val;
                let color = if is_bullish {
                    Color32::from_rgb(0, 150, 0) // Green for bullish
                } else {
                    Color32::from_rgb(200, 0, 0) // Red for bearish
                };
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Time".to_string(), time_val.to_string());
                tooltip_data.insert("Open".to_string(), open_val.to_string());
                tooltip_data.insert("High".to_string(), high_val.to_string());
                tooltip_data.insert("Low".to_string(), low_val.to_string());
                tooltip_data.insert("Close".to_string(), close_val.to_string());
                tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
                
                points.push(PlotPoint {
                    x: time_val,
                    y: open_val,
                    z: None,
                    label: None,
                    color: Some(color),
                    size: None,
                    series_id: None,
                    tooltip_data,
                });
                
                candlestick_data.push(CandlestickData {
                    time: time_val,
                    open: open_val,
                    high: high_val,
                    low: low_val,
                    close: close_val,
                    is_bullish,
                });
            }
        }
        
        // Sort by time
        points.sort_by(|a, b| a.x.partial_cmp(&b.x).unwrap_or(std::cmp::Ordering::Equal));
        
        // Calculate candlestick statistics
        let statistics = calculate_candlestick_statistics(&candlestick_data);
        
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
                ui.label(RichText::new("No data available for candlestick chart").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Candlestick Chart").heading());
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
                    ui.label("Price Range:");
                    ui.label(format!("{:.2} to {:.2}", 
                        data.points.iter().map(|p| p.y).fold(f64::INFINITY, f64::min),
                        data.points.iter().map(|p| p.y).fold(f64::NEG_INFINITY, f64::max)
                    ));
                });
            }
            
            // Show candlestick statistics
            if let Some(stats) = &data.statistics {
                ui.separator();
                ui.label(RichText::new("Market Statistics").strong());
                ui.horizontal(|ui| {
                    ui.label("Average Price:");
                    ui.label(format!("{:.3}", stats.mean_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Price Volatility:");
                    ui.label(format!("{:.3}", stats.std_y));
                });
                if let Some(corr) = stats.correlation {
                    ui.horizontal(|ui| {
                        ui.label("Price Trend:");
                        ui.label(if corr > 0.1 { "Bullish" } 
                               else if corr < -0.1 { "Bearish" } 
                               else { "Sideways" });
                    });
                }
            }
            
            ui.separator();
            
            // Candlestick visualization
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(300.0));
            
            ui.allocate_ui(plot_size, |ui| {
                let plot = Plot::new("candlestick")
                    .height(plot_size.y)
                    .allow_zoom(true)
                    .allow_drag(true)
                    .show_grid(true);
                
                plot.show(ui, |plot_ui| {
                    // Render candlesticks
                    render_candlesticks(plot_ui, data, config);
                    
                    // Render volume bars (if available)
                    render_volume_bars(plot_ui, data, config);
                });
            });
            
            // Controls
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Display Mode:");
                ui.radio_value(&mut 0, 0, "Candlesticks");
                ui.radio_value(&mut 0, 1, "Line");
                ui.radio_value(&mut 0, 2, "Both");
            });
            
            ui.horizontal(|ui| {
                ui.label("Show Volume:");
                ui.radio_value(&mut 0, 0, "Yes");
                ui.radio_value(&mut 0, 1, "No");
            });
        });
    }
}

/// Candlestick data structure
#[derive(Debug, Clone)]
struct CandlestickData {
    time: f64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    is_bullish: bool,
}

/// Calculate candlestick statistics
fn calculate_candlestick_statistics(candlestick_data: &[CandlestickData]) -> super::DataStatistics {
    if candlestick_data.is_empty() {
        return super::DataStatistics {
            mean_x: 0.0,
            mean_y: 0.0,
            std_x: 0.0,
            std_y: 0.0,
            correlation: None,
            count: 0,
        };
    }
    
    let prices: Vec<f64> = candlestick_data.iter().map(|c| c.close).collect();
    let times: Vec<f64> = candlestick_data.iter().map(|c| c.time).collect();
    
    let mean_y = prices.iter().sum::<f64>() / prices.len() as f64;
    let mean_x = times.iter().sum::<f64>() / times.len() as f64;
    
    let variance_y = prices.iter()
        .map(|y| (y - mean_y).powi(2))
        .sum::<f64>() / prices.len() as f64;
    let std_y = variance_y.sqrt();
    
    let variance_x = times.iter()
        .map(|x| (x - mean_x).powi(2))
        .sum::<f64>() / times.len() as f64;
    let std_x = variance_x.sqrt();
    
    // Calculate correlation
    let correlation = if std_x > 0.0 && std_y > 0.0 {
        let covariance = times.iter().zip(prices.iter())
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
        count: prices.len(),
    }
}

/// Render candlesticks
fn render_candlesticks(plot_ui: &mut PlotUi, data: &PlotData, _config: &PlotConfiguration) {
    if data.points.is_empty() {
        return;
    }
    
    // Generate sample candlestick data for demonstration
    for (i, point) in data.points.iter().enumerate() {
        let time = point.x;
        let open = point.y;
        let close = open * (1.0 + ((i as f64 % 3.0) - 1.0) * 0.02); // Simulate price movement
        let high = open.max(close) * 1.01;
        let low = open.min(close) * 0.99;
        let is_bullish = close >= open;
        
        let color = if is_bullish {
            Color32::from_rgb(0, 150, 0) // Green for bullish
        } else {
            Color32::from_rgb(200, 0, 0) // Red for bearish
        };
        
        // Draw candlestick body
        let body_width = 0.8;
        let body_height = (close - open).abs();
        let body_y = if is_bullish { open } else { close };
        
        // Draw body rectangle
        let body_points = vec![
            [time - body_width/2.0, body_y],
            [time + body_width/2.0, body_y],
            [time + body_width/2.0, body_y + body_height],
            [time - body_width/2.0, body_y + body_height],
        ];
        
        // Fill body
        plot_ui.polygon(egui_plot::Polygon::new(PlotPoints::from(body_points))
            .fill_color(color)
            .stroke(Stroke::new(1.0, color)));
        
        // Draw wicks (high-low lines)
        let wick_width = 0.1;
        let wick_points = vec![
            [time, low],
            [time, high],
        ];
        
        plot_ui.line(Line::new(PlotPoints::from(wick_points))
            .color(color)
            .width(1.0));
        
        // Draw horizontal lines at open and close
        let open_line = vec![
            [time - body_width/2.0, open],
            [time + body_width/2.0, open],
        ];
        
        let close_line = vec![
            [time - body_width/2.0, close],
            [time + body_width/2.0, close],
        ];
        
        plot_ui.line(Line::new(PlotPoints::from(open_line))
            .color(Color32::BLACK)
            .width(1.0));
        
        plot_ui.line(Line::new(PlotPoints::from(close_line))
            .color(Color32::BLACK)
            .width(1.0));
    }
}

/// Render volume bars
fn render_volume_bars(plot_ui: &mut PlotUi, data: &PlotData, _config: &PlotConfiguration) {
    if data.points.is_empty() {
        return;
    }
    
    // Generate sample volume data
    for (i, point) in data.points.iter().enumerate() {
        let time = point.x;
        let volume = 100.0 + (i as f64 % 10.0) * 50.0; // Simulate volume
        
        // Draw volume bar
        let bar_width = 0.6;
        let bar_height = volume / 1000.0; // Scale volume for display
        
        let bar_points = vec![
            [time - bar_width/2.0, 0.0],
            [time + bar_width/2.0, 0.0],
            [time + bar_width/2.0, bar_height],
            [time - bar_width/2.0, bar_height],
        ];
        
        plot_ui.polygon(egui_plot::Polygon::new(PlotPoints::from(bar_points))
            .fill_color(Color32::from_gray(150))
            .stroke(Stroke::new(1.0, Color32::from_gray(100))));
    }
}
