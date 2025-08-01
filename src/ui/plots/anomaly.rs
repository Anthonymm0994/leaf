use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotSpecificConfig, ColorScheme};
use egui::{Ui, Color32, RichText};
use egui_plot::{Plot, PlotUi, Text, PlotPoint, Polygon, PlotPoints, Line, Points};
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;
use crate::core::QueryResult;

pub struct AnomalyPlot;

#[derive(Debug, Clone)]
pub struct AnomalyResult {
    pub index: usize,
    pub value: f64,
    pub score: f64,
    pub method: String,
    pub severity: AnomalySeverity,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AnomalySeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl AnomalyPlot {
    /// Detect anomalies using Z-score method
    fn detect_zscore_anomalies(&self, values: &[f64], threshold: f64) -> Vec<AnomalyResult> {
        if values.len() < 3 {
            return Vec::new();
        }
        
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();
        
        if std_dev == 0.0 {
            return Vec::new();
        }
        
        let mut anomalies = Vec::new();
        for (i, &value) in values.iter().enumerate() {
            let z_score = (value - mean).abs() / std_dev;
            if z_score > threshold {
                let severity = if z_score > 3.0 {
                    AnomalySeverity::Critical
                } else if z_score > 2.5 {
                    AnomalySeverity::High
                } else if z_score > 2.0 {
                    AnomalySeverity::Medium
                } else {
                    AnomalySeverity::Low
                };
                
                anomalies.push(AnomalyResult {
                    index: i,
                    value,
                    score: z_score,
                    method: "Z-Score".to_string(),
                    severity,
                });
            }
        }
        
        anomalies
    }
    
    /// Detect anomalies using IQR method
    fn detect_iqr_anomalies(&self, values: &[f64]) -> Vec<AnomalyResult> {
        if values.len() < 4 {
            return Vec::new();
        }
        
        let mut sorted_values = values.to_vec();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let q1_idx = (sorted_values.len() as f64 * 0.25) as usize;
        let q3_idx = (sorted_values.len() as f64 * 0.75) as usize;
        let q1 = sorted_values[q1_idx];
        let q3 = sorted_values[q3_idx];
        let iqr = q3 - q1;
        
        let lower_bound = q1 - 1.5 * iqr;
        let upper_bound = q3 + 1.5 * iqr;
        
        let mut anomalies = Vec::new();
        for (i, &value) in values.iter().enumerate() {
            if value < lower_bound || value > upper_bound {
                let distance = if value < lower_bound {
                    (lower_bound - value) / iqr
                } else {
                    (value - upper_bound) / iqr
                };
                
                let severity = if distance > 2.0 {
                    AnomalySeverity::Critical
                } else if distance > 1.5 {
                    AnomalySeverity::High
                } else if distance > 1.0 {
                    AnomalySeverity::Medium
                } else {
                    AnomalySeverity::Low
                };
                
                anomalies.push(AnomalyResult {
                    index: i,
                    value,
                    score: distance,
                    method: "IQR".to_string(),
                    severity,
                });
            }
        }
        
        anomalies
    }
    
    /// Detect anomalies using moving average method
    fn detect_moving_average_anomalies(&self, values: &[f64], window_size: usize, threshold: f64) -> Vec<AnomalyResult> {
        if values.len() < window_size * 2 {
            return Vec::new();
        }
        
        let mut anomalies = Vec::new();
        
        for i in window_size..values.len() {
            let window_start = i - window_size;
            let window_end = i;
            let window_values = &values[window_start..window_end];
            
            let mean = window_values.iter().sum::<f64>() / window_size as f64;
            let variance = window_values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / window_size as f64;
            let std_dev = variance.sqrt();
            
            if std_dev > 0.0 {
                let current_value = values[i];
                let z_score = (current_value - mean).abs() / std_dev;
                
                if z_score > threshold {
                    let severity = if z_score > 3.0 {
                        AnomalySeverity::Critical
                    } else if z_score > 2.5 {
                        AnomalySeverity::High
                    } else if z_score > 2.0 {
                        AnomalySeverity::Medium
                    } else {
                        AnomalySeverity::Low
                    };
                    
                    anomalies.push(AnomalyResult {
                        index: i,
                        value: current_value,
                        score: z_score,
                        method: "Moving Average".to_string(),
                        severity,
                    });
                }
            }
        }
        
        anomalies
    }
    
    /// Get color for anomaly severity
    fn get_anomaly_color(&self, severity: &AnomalySeverity) -> Color32 {
        match severity {
            AnomalySeverity::Low => Color32::from_rgb(255, 255, 0), // Yellow
            AnomalySeverity::Medium => Color32::from_rgb(255, 165, 0), // Orange
            AnomalySeverity::High => Color32::from_rgb(255, 69, 0), // Red-Orange
            AnomalySeverity::Critical => Color32::from_rgb(255, 0, 0), // Red
        }
    }
}

impl PlotTrait for AnomalyPlot {
    fn name(&self) -> &'static str {
        "Anomaly Detection"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![DataType::Int64, DataType::Float64]) // X can be index or time
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Int64, DataType::Float64]
    }
    
    fn supports_multiple_series(&self) -> bool {
        false
    }
    
    fn get_default_config(&self) -> super::PlotConfiguration {
        super::PlotConfiguration {
            title: "Anomaly Detection".to_string(),
            x_column: String::new(),
            y_column: String::new(),
            color_column: None,
            size_column: None,
            group_column: None,
            show_legend: true,
            show_grid: true,
            show_axes_labels: true,
            color_scheme: ColorScheme::Viridis,
            marker_size: 4.0,
            line_width: 2.0,
            allow_zoom: true,
            allow_pan: true,
            allow_selection: true,
            show_tooltips: true,
            plot_specific: PlotSpecificConfig::None,
        }
    }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &super::PlotConfiguration) -> Result<PlotData, String> {
        if config.y_column.is_empty() {
            return Err("Y column is required for anomaly detection".to_string());
        }
        
        // Find Y column index
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or_else(|| format!("Y column '{}' not found", config.y_column))?;
        
        // Extract numeric values
        let mut values = Vec::new();
        for row in &query_result.rows {
            if row.len() > y_idx {
                if let Ok(val) = row[y_idx].parse::<f64>() {
                    values.push(val);
                }
            }
        }
        
        if values.is_empty() {
            return Err("No valid numeric data found in Y column".to_string());
        }
        
        // Detect anomalies using multiple methods
        let mut all_anomalies = Vec::new();
        
        // Z-score method
        let zscore_anomalies = self.detect_zscore_anomalies(&values, 2.0);
        all_anomalies.extend(zscore_anomalies);
        
        // IQR method
        let iqr_anomalies = self.detect_iqr_anomalies(&values);
        all_anomalies.extend(iqr_anomalies);
        
        // Moving average method (if enough data)
        if values.len() >= 10 {
            let ma_anomalies = self.detect_moving_average_anomalies(&values, 5, 2.0);
            all_anomalies.extend(ma_anomalies);
        }
        
        // Create plot points
        let mut points = Vec::new();
        let mut series = Vec::new();
        
        // Add normal data points
        for (i, &value) in values.iter().enumerate() {
            let is_anomaly = all_anomalies.iter().any(|a| a.index == i);
            
            let mut tooltip_data = HashMap::new();
            tooltip_data.insert("Index".to_string(), i.to_string());
            tooltip_data.insert("Value".to_string(), format!("{:.3}", value));
            tooltip_data.insert("Anomaly".to_string(), if is_anomaly { "Yes".to_string() } else { "No".to_string() });
            
            let color = if is_anomaly {
                let anomaly = all_anomalies.iter().find(|a| a.index == i).unwrap();
                self.get_anomaly_color(&anomaly.severity)
            } else {
                Color32::from_rgb(100, 150, 255)
            };
            
            points.push(super::PlotPoint {
                x: i as f64,
                y: value,
                z: None,
                label: Some(format!("Value: {:.3}", value)),
                color: Some(color),
                size: Some(if is_anomaly { 8.0 } else { 4.0 }),
                series_id: Some(if is_anomaly { "anomaly".to_string() } else { "normal".to_string() }),
                tooltip_data,
            });
        }
        
        // Create series
        let normal_points: Vec<super::PlotPoint> = points.iter()
            .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "normal"))
            .cloned()
            .collect();
        
        let anomaly_points: Vec<super::PlotPoint> = points.iter()
            .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "anomaly"))
            .cloned()
            .collect();
        
        series.push(super::DataSeries {
            id: "normal".to_string(),
            name: "Normal Data".to_string(),
            points: normal_points,
            color: Color32::from_rgb(100, 150, 255),
            visible: true,
            style: super::SeriesStyle::Points { size: 4.0, shape: super::MarkerShape::Circle },
        });
        
        series.push(super::DataSeries {
            id: "anomaly".to_string(),
            name: "Anomalies".to_string(),
            points: anomaly_points,
            color: Color32::from_rgb(255, 100, 100),
            visible: true,
            style: super::SeriesStyle::Points { size: 8.0, shape: super::MarkerShape::Cross },
        });
        
        // Calculate statistics
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();
        
        Ok(PlotData {
            points,
            series,
            metadata: super::PlotMetadata {
                title: format!("Anomaly Detection - {}", config.y_column),
                x_label: "Index".to_string(),
                y_label: config.y_column.clone(),
                show_legend: true,
                show_grid: true,
                color_scheme: ColorScheme::Viridis,
                extra_data: None,
            },
            statistics: Some(super::DataStatistics {
                mean_x: 0.0,
                mean_y: mean,
                std_x: 0.0,
                std_y: std_dev,
                correlation: None,
                count: values.len(),
            }),
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &super::PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label("No data available for anomaly detection");
            });
            return;
        }
        
        // Create the plot
        let plot = Plot::new("anomaly_detection")
            .x_axis_label(&data.metadata.x_label)
            .y_axis_label(&data.metadata.y_label)
            .show_grid(config.show_grid)
            .allow_zoom(config.allow_zoom)
            .allow_drag(config.allow_pan)
            .allow_boxed_zoom(config.allow_zoom)
            .legend(egui_plot::Legend::default());
        
        plot.show(ui, |plot_ui| {
            // Render normal data points
            let normal_points: Vec<&super::PlotPoint> = data.points.iter()
                .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "normal"))
                .collect();
            
            if !normal_points.is_empty() {
                let normal_coords: Vec<[f64; 2]> = normal_points.iter()
                    .map(|p| [p.x, p.y])
                    .collect();
                
                let normal_series = Points::new(normal_coords)
                    .color(Color32::from_rgb(100, 150, 255))
                    .radius(4.0)
                    .shape(egui_plot::MarkerShape::Circle)
                    .name("Normal Data");
                
                plot_ui.points(normal_series);
            }
            
            // Render anomaly points
            let anomaly_points: Vec<&super::PlotPoint> = data.points.iter()
                .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "anomaly"))
                .collect();
            
            if !anomaly_points.is_empty() {
                let anomaly_coords: Vec<[f64; 2]> = anomaly_points.iter()
                    .map(|p| [p.x, p.y])
                    .collect();
                
                let anomaly_series = Points::new(anomaly_coords)
                    .color(Color32::from_rgb(255, 100, 100))
                    .radius(8.0)
                    .shape(egui_plot::MarkerShape::Cross)
                    .name("Anomalies");
                
                plot_ui.points(anomaly_series);
            }
        });
        
        // Show comprehensive legend and summary
        let anomaly_count = data.points.iter()
            .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "anomaly"))
            .count();
        
        ui.collapsing("Anomaly Detection Legend & Summary", |ui| {
            // Data series section
            ui.label(RichText::new("Data Series:").strong());
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(100, 150, 255), "●");
                ui.label("Normal Data Points");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 100, 100), "✚");
                ui.label("Anomaly Points");
            });
            
            ui.separator();
            
            // Severity levels section
            ui.label(RichText::new("Anomaly Severity Levels:").strong());
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 255, 0), "●");
                ui.label("Low (|z| > 2.0) - Minor deviations");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 165, 0), "●");
                ui.label("Medium (|z| > 2.5) - Moderate outliers");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 69, 0), "●");
                ui.label("High (|z| > 3.0) - Significant outliers");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 0, 0), "●");
                ui.label("Critical (|z| > 3.5) - Extreme outliers");
            });
            
            ui.separator();
            
            // Detection methods section
            ui.label(RichText::new("Detection Methods:").strong());
            ui.label("• Z-Score: Statistical outliers based on standard deviations");
            ui.label("• IQR: Quartile-based outliers using interquartile range");
            ui.label("• Moving Average: Temporal anomalies in time series data");
            
            ui.separator();
            
            // Summary statistics
            ui.label(RichText::new("Summary Statistics:").strong());
            ui.horizontal(|ui| {
                ui.label("Total Data Points:");
                ui.label(format!("{}", data.points.len()));
            });
            ui.horizontal(|ui| {
                ui.label("Anomalies Detected:");
                ui.label(format!("{}", anomaly_count));
            });
            ui.horizontal(|ui| {
                ui.label("Anomaly Rate:");
                ui.label(format!("{:.2}%", (anomaly_count as f64 / data.points.len() as f64) * 100.0));
            });
            
            // Additional statistics if available
            if let Some(stats) = &data.statistics {
                ui.horizontal(|ui| {
                    ui.label("Mean Value:");
                    ui.label(format!("{:.2}", stats.mean_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Standard Deviation:");
                    ui.label(format!("{:.2}", stats.std_y));
                });
            }
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, _config: &super::PlotConfiguration) {
        ui.group(|ui| {
            ui.label(RichText::new("Anomaly Detection Legend").strong());
            ui.separator();
            
            // Data series
            ui.label(RichText::new("Data Series:").strong());
            for series in &data.series {
                let mut is_visible = series.visible;
                if ui.checkbox(&mut is_visible, &series.name).changed() {
                    // Note: This would require mutable access to data
                }
                
                ui.horizontal(|ui| {
                    match &series.style {
                        super::SeriesStyle::Points { size: _, shape } => {
                            let shape_text = match shape {
                                super::MarkerShape::Circle => "●",
                                super::MarkerShape::Cross => "✚",
                                _ => "●",
                            };
                            ui.colored_label(series.color, shape_text);
                        },
                        _ => {
                            ui.colored_label(series.color, "●");
                        }
                    }
                    
                    if !is_visible {
                        ui.label(RichText::new(&series.name).strikethrough());
                    } else {
                        ui.label(&series.name);
                    }
                });
            }
            
            ui.separator();
            
            // Severity levels
            ui.label(RichText::new("Anomaly Severity:").strong());
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 255, 0), "●");
                ui.label("Low (|z| > 2.0)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 165, 0), "●");
                ui.label("Medium (|z| > 2.5)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 69, 0), "●");
                ui.label("High (|z| > 3.0)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(255, 0, 0), "●");
                ui.label("Critical (|z| > 3.5)");
            });
            
            ui.separator();
            
            // Detection methods
            ui.label(RichText::new("Detection Methods:").strong());
            ui.label("• Z-Score: Statistical outliers");
            ui.label("• IQR: Quartile-based outliers");
            ui.label("• Moving Average: Temporal anomalies");
            
            // Statistics if available
            if let Some(stats) = &data.statistics {
                ui.separator();
                ui.label(RichText::new("Statistics:").strong());
                ui.horizontal(|ui| {
                    ui.label("Mean:");
                    ui.label(format!("{:.2}", stats.mean_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Std Dev:");
                    ui.label(format!("{:.2}", stats.std_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Total Points:");
                    ui.label(format!("{}", stats.count));
                });
            }
        });
    }
    
    fn handle_interaction(&self, _ui: &mut Ui, _data: &PlotData, _config: &super::PlotConfiguration) -> Option<super::PlotInteraction> {
        None
    }
}

