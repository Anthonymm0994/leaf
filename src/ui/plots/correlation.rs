use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotSpecificConfig, ColorScheme};
use egui::{Ui, Color32, RichText, Stroke};
use egui_plot::{Plot, PlotUi, Text, PlotPoint, Polygon, PlotPoints};
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;
use crate::core::QueryResult;

pub struct CorrelationPlot;

impl CorrelationPlot {
    /// Compute correlation matrix for numeric columns
    fn compute_correlation_matrix(&self, query_result: &QueryResult) -> Result<(Vec<Vec<f64>>, Vec<String>), String> {
        // Find all numeric columns
        let mut numeric_columns = Vec::new();
        let mut numeric_indices = Vec::new();
        
        for (i, (col_name, col_type)) in query_result.columns.iter().zip(query_result.column_types.iter()).enumerate() {
            if matches!(col_type, DataType::Int64 | DataType::Float64) {
                numeric_columns.push(col_name.clone());
                numeric_indices.push(i);
            }
        }
        
        if numeric_columns.len() < 2 {
            return Err("Need at least 2 numeric columns for correlation analysis".to_string());
        }
        
        // Extract numeric data
        let mut numeric_data = vec![Vec::new(); numeric_columns.len()];
        
        for row in &query_result.rows {
            for (col_idx, &data_idx) in numeric_indices.iter().enumerate() {
                if row.len() > data_idx {
                    if let Ok(val) = row[data_idx].parse::<f64>() {
                        numeric_data[col_idx].push(val);
                    }
                }
            }
        }
        
        // Ensure all columns have the same number of valid values
        let min_length = numeric_data.iter().map(|v| v.len()).min().unwrap_or(0);
        if min_length < 2 {
            return Err("Need at least 2 valid data points for correlation".to_string());
        }
        
        // Truncate all columns to the minimum length
        for col_data in &mut numeric_data {
            col_data.truncate(min_length);
        }
        
        // Compute correlation matrix
        let n_cols = numeric_columns.len();
        let mut correlation_matrix = vec![vec![0.0; n_cols]; n_cols];
        
        for i in 0..n_cols {
            for j in 0..n_cols {
                if i == j {
                    correlation_matrix[i][j] = 1.0;
                } else {
                    correlation_matrix[i][j] = self.pearson_correlation(&numeric_data[i], &numeric_data[j]);
                }
            }
        }
        
        Ok((correlation_matrix, numeric_columns))
    }
    
    /// Compute Pearson correlation coefficient
    fn pearson_correlation(&self, x: &[f64], y: &[f64]) -> f64 {
        if x.len() != y.len() || x.len() < 2 {
            return 0.0;
        }
        
        let n = x.len() as f64;
        let sum_x: f64 = x.iter().sum();
        let sum_y: f64 = y.iter().sum();
        let sum_x_sq: f64 = x.iter().map(|&v| v * v).sum();
        let sum_y_sq: f64 = y.iter().map(|&v| v * v).sum();
        let sum_xy: f64 = x.iter().zip(y.iter()).map(|(&a, &b)| a * b).sum();
        
        let numerator = n * sum_xy - sum_x * sum_y;
        let denominator = ((n * sum_x_sq - sum_x * sum_x) * (n * sum_y_sq - sum_y * sum_y)).sqrt();
        
        if denominator == 0.0 {
            0.0
        } else {
            numerator / denominator
        }
    }
    
    /// Get color for correlation value using a more sophisticated color scheme
    fn get_correlation_color(&self, correlation: f64) -> Color32 {
        // Use a more sophisticated diverging color scheme
        let abs_corr = correlation.abs();
        
        if correlation < 0.0 {
            // Negative correlations: Cool colors (blues to purples)
            if abs_corr > 0.8 {
                Color32::from_rgb(49, 54, 149) // Dark blue
            } else if abs_corr > 0.6 {
                Color32::from_rgb(69, 117, 180) // Medium blue
            } else if abs_corr > 0.4 {
                Color32::from_rgb(116, 173, 209) // Light blue
            } else if abs_corr > 0.2 {
                Color32::from_rgb(171, 217, 233) // Very light blue
            } else {
                Color32::from_rgb(224, 243, 248) // Almost white
            }
        } else {
            // Positive correlations: Warm colors (yellows to reds)
            if abs_corr > 0.8 {
                Color32::from_rgb(165, 0, 38) // Dark red
            } else if abs_corr > 0.6 {
                Color32::from_rgb(215, 48, 39) // Medium red
            } else if abs_corr > 0.4 {
                Color32::from_rgb(244, 109, 67) // Light red
            } else if abs_corr > 0.2 {
                Color32::from_rgb(253, 174, 97) // Orange
            } else {
                Color32::from_rgb(254, 224, 144) // Light yellow
            }
        }
    }
}

impl PlotTrait for CorrelationPlot {
    fn name(&self) -> &'static str {
        "Correlation Matrix"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        None // No X column required, uses all numeric columns
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Float64] // Any numeric column will do
    }
    
    fn supports_multiple_series(&self) -> bool {
        false
    }
    
    fn get_default_config(&self) -> super::PlotConfiguration {
        super::PlotConfiguration {
            title: "Correlation Matrix".to_string(),
            x_column: String::new(),
            y_column: String::new(),
            color_column: None,
            size_column: None,
            group_column: None,
            show_legend: true,
            show_grid: false,
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
    
    fn prepare_data(&self, query_result: &QueryResult, _config: &super::PlotConfiguration) -> Result<PlotData, String> {
        // Compute correlation matrix
        let (correlation_matrix, column_names) = self.compute_correlation_matrix(query_result)?;
        
        // Create plot data structure
        let mut points = Vec::new();
        let mut series = Vec::new();
        
        // Add correlation values as points for visualization
        for (i, row) in correlation_matrix.iter().enumerate() {
            for (j, &correlation) in row.iter().enumerate() {
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Row".to_string(), column_names[i].clone());
                tooltip_data.insert("Column".to_string(), column_names[j].clone());
                tooltip_data.insert("Correlation".to_string(), format!("{:.3}", correlation));
                
                points.push(super::PlotPoint {
                    x: i as f64,
                    y: j as f64,
                    z: Some(correlation),
                    label: Some(format!("{:.3}", correlation)),
                    color: Some(self.get_correlation_color(correlation)),
                    size: Some((correlation.abs() * 10.0 + 2.0) as f32),
                    series_id: None,
                    tooltip_data,
                });
            }
        }
        
        // Create a single series for the correlation matrix
        series.push(super::DataSeries {
            id: "correlation".to_string(),
            name: "Correlation Matrix".to_string(),
            points: points.clone(),
            color: Color32::BLUE,
            visible: true,
            style: super::SeriesStyle::Points { size: 8.0, shape: super::MarkerShape::Square },
        });
        
        Ok(PlotData {
            points,
            series,
            metadata: super::PlotMetadata {
                title: "Correlation Matrix".to_string(),
                x_label: "Variables".to_string(),
                y_label: "Variables".to_string(),
                show_legend: true,
                show_grid: false,
                color_scheme: ColorScheme::Viridis,
                extra_data: None,
            },
            statistics: None,
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &super::PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label("No numeric columns found for correlation analysis");
                ui.label(RichText::new("Select numeric columns to compute correlations").weak());
            });
            return;
        }
        
        // Create heatmap-style visualization
        let plot = Plot::new("correlation_matrix")
            .x_axis_label(&data.metadata.x_label)
            .y_axis_label(&data.metadata.y_label)
            .show_grid(false)
            .allow_zoom(config.allow_zoom)
            .allow_drag(config.allow_pan)
            .allow_boxed_zoom(config.allow_zoom);
        
        plot.show(ui, |plot_ui| {
            // Render correlation matrix as colored squares with improved styling
            for point in &data.points {
                let x = point.x;
                let y = point.y;
                if let Some(correlation) = point.z {
                    let color = self.get_correlation_color(correlation);
                    let size = 0.9; // Fixed size for cleaner appearance
                    
                    // Create a rounded square for each correlation value
                    let half_size = size / 2.0;
                    let square_points = vec![
                        [x - half_size, y - half_size],
                        [x + half_size, y - half_size],
                        [x + half_size, y + half_size],
                        [x - half_size, y + half_size],
                    ];
                    
                    // Use softer stroke color
                    let stroke_color = if correlation.abs() > 0.5 {
                        Color32::from_gray(80)
                    } else {
                        Color32::from_gray(120)
                    };
                    
                    let square = Polygon::new(square_points)
                        .fill_color(color)
                        .stroke(Stroke::new(0.5, stroke_color))
                        .name(format!("{:.3}", correlation));
                    
                    plot_ui.polygon(square);
                    
                    // Add correlation value as text with better contrast
                    if correlation.abs() > 0.4 {
                        let text_color = if correlation.abs() > 0.7 {
                            Color32::WHITE
                        } else if correlation.abs() > 0.5 {
                            Color32::from_gray(30)
                        } else {
                            Color32::from_gray(60)
                        };
                        
                        plot_ui.text(Text::new(
                            PlotPoint::new(x, y),
                            RichText::new(format!("{:.2}", correlation))
                                .size(9.0)
                                .color(text_color)
                        ));
                    }
                }
            }
        });
        
        // Add comprehensive correlation interpretation
        ui.collapsing("Correlation Matrix Guide", |ui| {
            ui.label(RichText::new("Color Interpretation:").strong());
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(165, 0, 38), "■");
                ui.label("Strong Positive (>0.8)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(215, 48, 39), "■");
                ui.label("Moderate Positive (0.6-0.8)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(244, 109, 67), "■");
                ui.label("Weak Positive (0.4-0.6)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(253, 174, 97), "■");
                ui.label("Very Weak Positive (0.2-0.4)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(254, 224, 144), "■");
                ui.label("Negligible (<0.2)");
            });
            
            ui.separator();
            
            ui.label(RichText::new("Negative Correlations:").strong());
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(49, 54, 149), "■");
                ui.label("Strong Negative (>0.8)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(69, 117, 180), "■");
                ui.label("Moderate Negative (0.6-0.8)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(116, 173, 209), "■");
                ui.label("Weak Negative (0.4-0.6)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(171, 217, 233), "■");
                ui.label("Very Weak Negative (0.2-0.4)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(224, 243, 248), "■");
                ui.label("Negligible (<0.2)");
            });
            
            ui.separator();
            
            ui.label(RichText::new("Key Features:").strong());
            ui.label("• Diagonal values are always 1.0 (perfect self-correlation)");
            ui.label("• Matrix is symmetric (top-right mirrors bottom-left)");
            ui.label("• Values range from -1.0 to +1.0");
            ui.label("• Only numeric columns are included in analysis");
            
            ui.separator();
            
            ui.label(RichText::new("Interpretation Guide:").strong());
            ui.label("• |r| > 0.8: Very strong correlation");
            ui.label("• 0.6 < |r| < 0.8: Strong correlation");
            ui.label("• 0.4 < |r| < 0.6: Moderate correlation");
            ui.label("• 0.2 < |r| < 0.4: Weak correlation");
            ui.label("• |r| < 0.2: Negligible correlation");
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, _data: &PlotData, _config: &super::PlotConfiguration) {
        ui.group(|ui| {
            ui.label(RichText::new("Correlation Matrix Legend").strong());
            ui.separator();
            
            ui.label(RichText::new("Positive Correlations:").strong());
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(165, 0, 38), "■");
                ui.label("Strong (>0.8)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(215, 48, 39), "■");
                ui.label("Moderate (0.6-0.8)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(244, 109, 67), "■");
                ui.label("Weak (0.4-0.6)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(253, 174, 97), "■");
                ui.label("Very Weak (0.2-0.4)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(254, 224, 144), "■");
                ui.label("Negligible (<0.2)");
            });
            
            ui.separator();
            
            ui.label(RichText::new("Negative Correlations:").strong());
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(49, 54, 149), "■");
                ui.label("Strong (>0.8)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(69, 117, 180), "■");
                ui.label("Moderate (0.6-0.8)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(116, 173, 209), "■");
                ui.label("Weak (0.4-0.6)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(171, 217, 233), "■");
                ui.label("Very Weak (0.2-0.4)");
            });
            ui.horizontal(|ui| {
                ui.colored_label(Color32::from_rgb(224, 243, 248), "■");
                ui.label("Negligible (<0.2)");
            });
            
            ui.separator();
            ui.label("Diagonal values are always 1.0 (perfect self-correlation)");
        });
    }
    
    fn handle_interaction(&self, _ui: &mut Ui, _data: &PlotData, _config: &super::PlotConfiguration) -> Option<super::PlotInteraction> {
        None
    }
}
