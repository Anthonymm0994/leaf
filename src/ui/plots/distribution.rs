use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotSpecificConfig, ColorScheme};
use egui::{Ui, Color32, RichText, Stroke};
use egui_plot::{Plot, PlotUi, Text, PlotPoint, Polygon, PlotPoints, Line, Points};
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;
use crate::core::QueryResult;

pub struct DistributionPlot;

impl DistributionPlot {
    /// Calculate histogram bins and counts
    fn calculate_histogram(&self, values: &[f64], bin_count: usize) -> (Vec<f64>, Vec<f64>, Vec<usize>) {
        if values.is_empty() {
            return (Vec::new(), Vec::new(), Vec::new());
        }
        
        let min_val = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_val = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        if min_val == max_val {
            return (vec![min_val], vec![min_val], vec![values.len()]);
        }
        
        let bin_width = (max_val - min_val) / bin_count as f64;
        let mut bins = Vec::new();
        let mut bin_edges = Vec::new();
        let mut counts = vec![0; bin_count];
        
        // Create bin edges
        for i in 0..=bin_count {
            bin_edges.push(min_val + i as f64 * bin_width);
        }
        
        // Create bin centers
        for i in 0..bin_count {
            bins.push(min_val + (i as f64 + 0.5) * bin_width);
        }
        
        // Count values in each bin
        for &value in values {
            if value >= min_val && value <= max_val {
                let bin_idx = ((value - min_val) / bin_width).floor() as usize;
                let bin_idx = bin_idx.min(bin_count - 1);
                counts[bin_idx] += 1;
            }
        }
        
        (bins, bin_edges, counts)
    }
    
    /// Calculate kernel density estimation
    fn calculate_kde(&self, values: &[f64], bandwidth: f64, points: usize) -> Vec<(f64, f64)> {
        if values.is_empty() {
            return Vec::new();
        }
        
        let min_val = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_val = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let range = max_val - min_val;
        
        let mut kde_points = Vec::new();
        for i in 0..points {
            let x = min_val + (i as f64 / (points - 1) as f64) * range;
            let density = self.kernel_density(x, values, bandwidth);
            kde_points.push((x, density));
        }
        
        kde_points
    }
    
    /// Kernel density estimation using Gaussian kernel
    fn kernel_density(&self, x: f64, data: &[f64], bandwidth: f64) -> f64 {
        let n = data.len() as f64;
        let mut sum = 0.0;
        
        for &value in data {
            let diff = (x - value) / bandwidth;
            sum += (-0.5 * diff * diff).exp();
        }
        
        sum / (n * bandwidth * (2.0 * std::f64::consts::PI).sqrt())
    }
    
    /// Calculate statistical summary
    fn calculate_statistics(&self, values: &[f64]) -> HashMap<String, f64> {
        if values.is_empty() {
            return HashMap::new();
        }
        
        let n = values.len() as f64;
        let sum: f64 = values.iter().sum();
        let mean = sum / n;
        
        let variance = values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / n;
        let std_dev = variance.sqrt();
        
        let mut sorted_values = values.to_vec();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let median = if n as usize % 2 == 0 {
            let mid = n as usize / 2;
            (sorted_values[mid - 1] + sorted_values[mid]) / 2.0
        } else {
            sorted_values[n as usize / 2]
        };
        
        let q1_idx = (n * 0.25) as usize;
        let q3_idx = (n * 0.75) as usize;
        let q1 = sorted_values[q1_idx];
        let q3 = sorted_values[q3_idx];
        
        let min_val = sorted_values[0];
        let max_val = sorted_values[sorted_values.len() - 1];
        
        let mut stats = HashMap::new();
        stats.insert("count".to_string(), n);
        stats.insert("mean".to_string(), mean);
        stats.insert("median".to_string(), median);
        stats.insert("std_dev".to_string(), std_dev);
        stats.insert("variance".to_string(), variance);
        stats.insert("min".to_string(), min_val);
        stats.insert("max".to_string(), max_val);
        stats.insert("q1".to_string(), q1);
        stats.insert("q3".to_string(), q3);
        stats.insert("iqr".to_string(), q3 - q1);
        
        stats
    }
}

impl PlotTrait for DistributionPlot {
    fn name(&self) -> &'static str {
        "Distribution Plot"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        None // No X column required, uses Y column for distribution
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Int64, DataType::Float64]
    }
    
    fn supports_multiple_series(&self) -> bool {
        false
    }
    
    fn get_default_config(&self) -> super::PlotConfiguration {
        super::PlotConfiguration {
            title: "Distribution Analysis".to_string(),
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
            return Err("Y column is required for distribution analysis".to_string());
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
        
        // Calculate statistics
        let stats = self.calculate_statistics(&values);
        
        // Calculate histogram
        let bin_count = 20; // Default bin count
        let (bins, bin_edges, counts) = self.calculate_histogram(&values, bin_count);
        
        // Calculate KDE
        let bandwidth = 0.5; // Default bandwidth
        let kde_points = self.calculate_kde(&values, bandwidth, 100);
        
        // Create plot points
        let mut points = Vec::new();
        
        // Add histogram bars
        for (i, (bin_center, &count)) in bins.iter().zip(counts.iter()).enumerate() {
            let mut tooltip_data = HashMap::new();
            tooltip_data.insert("Bin".to_string(), format!("{:.2}", bin_center));
            tooltip_data.insert("Count".to_string(), count.to_string());
            tooltip_data.insert("Frequency".to_string(), format!("{:.3}", count as f64 / values.len() as f64));
            
            points.push(super::PlotPoint {
                x: *bin_center,
                y: count as f64,
                z: None,
                label: Some(format!("Count: {}", count)),
                color: Some(Color32::from_rgb(100, 150, 255)),
                size: Some(10.0),
                series_id: Some("histogram".to_string()),
                tooltip_data,
            });
        }
        
        // Add KDE points
        for (x, density) in kde_points {
            let mut tooltip_data = HashMap::new();
            tooltip_data.insert("X".to_string(), format!("{:.2}", x));
            tooltip_data.insert("Density".to_string(), format!("{:.4}", density));
            
            points.push(super::PlotPoint {
                x,
                y: density * values.len() as f64, // Scale to match histogram
                z: None,
                label: Some(format!("Density: {:.4}", density)),
                color: Some(Color32::from_rgb(255, 100, 100)),
                size: Some(2.0),
                series_id: Some("kde".to_string()),
                tooltip_data,
            });
        }
        
        // Create series
        let mut series = Vec::new();
        
        // Histogram series
        let histogram_points: Vec<super::PlotPoint> = points.iter()
            .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "histogram"))
            .cloned()
            .collect();
        
        series.push(super::DataSeries {
            id: "histogram".to_string(),
            name: "Histogram".to_string(),
            points: histogram_points,
            color: Color32::from_rgb(100, 150, 255),
            visible: true,
            style: super::SeriesStyle::Bars { width: 0.8 },
        });
        
        // KDE series
        let kde_plot_points: Vec<super::PlotPoint> = points.iter()
            .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "kde"))
            .cloned()
            .collect();
        
        series.push(super::DataSeries {
            id: "kde".to_string(),
            name: "Density Curve".to_string(),
            points: kde_plot_points,
            color: Color32::from_rgb(255, 100, 100),
            visible: true,
            style: super::SeriesStyle::Lines { width: 2.0, style: super::LineStyle::Solid },
        });
        
        Ok(PlotData {
            points,
            series,
            metadata: super::PlotMetadata {
                title: format!("Distribution of {}", config.y_column),
                x_label: config.y_column.clone(),
                y_label: "Frequency".to_string(),
                show_legend: true,
                show_grid: true,
                color_scheme: ColorScheme::Viridis,
            },
            statistics: Some(super::DataStatistics {
                mean_x: stats.get("mean").copied().unwrap_or(0.0),
                mean_y: 0.0, // Not applicable for distribution
                std_x: stats.get("std_dev").copied().unwrap_or(0.0),
                std_y: 0.0,
                correlation: None,
                count: values.len(),
            }),
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &super::PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label("No data available for distribution analysis");
            });
            return;
        }
        
        // Create the plot
        let plot = Plot::new("distribution_plot")
            .x_axis_label(&data.metadata.x_label)
            .y_axis_label(&data.metadata.y_label)
            .show_grid(config.show_grid)
            .allow_zoom(config.allow_zoom)
            .allow_drag(config.allow_pan)
            .allow_boxed_zoom(config.allow_zoom);
        
        plot.show(ui, |plot_ui| {
            // Render histogram bars
            let histogram_points: Vec<&super::PlotPoint> = data.points.iter()
                .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "histogram"))
                .collect();
            
            for point in histogram_points {
                let x = point.x;
                let y = point.y;
                let bar_width = 0.8;
                let bar_points = vec![
                    [x - bar_width/2.0, 0.0],
                    [x + bar_width/2.0, 0.0],
                    [x + bar_width/2.0, y],
                    [x - bar_width/2.0, y],
                ];
                
                let bar = Polygon::new(bar_points)
                    .fill_color(point.color.unwrap_or(Color32::from_rgb(100, 150, 255)))
                    .stroke(Stroke::new(1.0, Color32::from_gray(100)))
                    .name(format!("Count: {}", y as i32));
                
                plot_ui.polygon(bar);
            }
            
            // Render KDE curve
            let kde_points: Vec<&super::PlotPoint> = data.points.iter()
                .filter(|p| p.series_id.as_ref().map_or(false, |id| id == "kde"))
                .collect();
            
            if kde_points.len() > 1 {
                let curve_points: Vec<[f64; 2]> = kde_points.iter()
                    .map(|p| [p.x, p.y])
                    .collect();
                
                let curve = Line::new(curve_points)
                    .color(Color32::from_rgb(255, 100, 100))
                    .width(2.0)
                    .name("Density Curve");
                
                plot_ui.line(curve);
            }
        });
        
        // Show statistical summary
        if let Some(stats) = &data.statistics {
            ui.collapsing("Statistical Summary", |ui| {
                ui.horizontal(|ui| {
                    ui.label("Count:");
                    ui.label(format!("{}", stats.count));
                });
                ui.horizontal(|ui| {
                    ui.label("Mean:");
                    ui.label(format!("{:.3}", stats.mean_x));
                });
                ui.horizontal(|ui| {
                    ui.label("Std Dev:");
                    ui.label(format!("{:.3}", stats.std_x));
                });
                ui.horizontal(|ui| {
                    ui.label("Min:");
                    ui.label(format!("{:.3}", data.points.iter().map(|p| p.x).fold(f64::INFINITY, f64::min)));
                });
                ui.horizontal(|ui| {
                    ui.label("Max:");
                    ui.label(format!("{:.3}", data.points.iter().map(|p| p.x).fold(f64::NEG_INFINITY, f64::max)));
                });
            });
        }
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, _config: &super::PlotConfiguration) {
        ui.group(|ui| {
            ui.label(RichText::new("Distribution Plot Legend").strong());
            ui.separator();
            
            for series in &data.series {
                let mut is_visible = series.visible;
                if ui.checkbox(&mut is_visible, &series.name).changed() {
                    // Note: This would require mutable access to data
                }
                
                ui.horizontal(|ui| {
                    match &series.style {
                        super::SeriesStyle::Bars { width: _ } => {
                            ui.colored_label(series.color, "■");
                        },
                        super::SeriesStyle::Lines { width: _, style } => {
                            let style_text = match style {
                                super::LineStyle::Solid => "———",
                                super::LineStyle::Dashed => "---",
                                super::LineStyle::Dotted => "...",
                                super::LineStyle::DashDot => "-.-.",
                            };
                            ui.colored_label(series.color, style_text);
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
        });
    }
    
    fn handle_interaction(&self, _ui: &mut Ui, _data: &PlotData, _config: &super::PlotConfiguration) -> Option<super::PlotInteraction> {
        None
    }
}
