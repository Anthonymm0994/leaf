use super::{
    Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, DataSeries, PlotMetadata, ColorScheme,
    PlotInteraction, PlotSpecificConfig, ViolinPlotConfig, DataStatistics, SeriesStyle,
};
use crate::core::QueryResult;
use crate::ui::plots::data_processor::DataProcessor;
use egui::{Color32, Ui, RichText, Stroke};
use egui_plot::{Plot, PlotPoint as EguiPlotPoint, PlotPoints, Polygon, MarkerShape as EguiMarkerShape, Line, Points, Legend, MarkerShape};
use std::collections::HashMap;

/// Box plot statistics for violin plots
#[derive(Debug, Clone)]
pub struct BoxPlotStats {
    pub min: f64,
    pub q1: f64,
    pub median: f64,
    pub q3: f64,
    pub max: f64,
    pub mean: f64,
    pub outliers: Vec<f64>,
    pub group: Option<String>,
    pub count: usize,
    pub lower_fence: f64,
    pub upper_fence: f64,
}

pub struct ViolinPlot {
    // Cache for KDE calculations to avoid recomputing for the same data
    kde_cache: std::cell::RefCell<HashMap<String, Vec<(f64, f64)>>>,
}

impl ViolinPlot {
    /// Create a new ViolinPlot instance
    pub fn new() -> Self {
        Self {
            kde_cache: std::cell::RefCell::new(HashMap::new()),
        }
    }

    /// Calculate kernel density estimation for violin plot
    fn calculate_kde(&self, data: &[f64], bandwidth: f32, points: usize) -> Vec<(f64, f64)> {
        if data.is_empty() {
            return Vec::new();
        }
        
        // Create a cache key based on data hash, bandwidth and points
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for val in data {
            std::hash::Hash::hash(&val.to_bits(), &mut hasher);
        }
        let cache_key = format!("{:x}_{}_{}",
            std::hash::Hasher::finish(&hasher),
            bandwidth,
            points
        );
        
        // Check if we have this KDE calculation cached
        let mut cache = self.kde_cache.borrow_mut();
        if let Some(cached_kde) = cache.get(&cache_key) {
            return cached_kde.clone();
        }
        
        // Find min and max values
        let min_val = data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_val = data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        // Create evaluation points
        let mut kde_points = Vec::with_capacity(points);
        let range = max_val - min_val;
        let padding = range * 0.1; // Add 10% padding on each side
        let step = (range + 2.0 * padding) / (points as f64 - 1.0);
        
        for i in 0..points {
            let x = min_val - padding + i as f64 * step;
            let density = self.kernel_density(x, data, bandwidth as f64);
            kde_points.push((x, density));
        }
        
        // Normalize density values
        let max_density = kde_points.iter().map(|(_, d)| *d).fold(0.0, f64::max);
        if max_density > 0.0 {
            for point in &mut kde_points {
                point.1 /= max_density;
            }
        }
        
        // Cache the result
        cache.insert(cache_key, kde_points.clone());
        
        kde_points
    }
    
    /// Gaussian kernel density estimation
    fn kernel_density(&self, x: f64, data: &[f64], bandwidth: f64) -> f64 {
        let n = data.len() as f64;
        let h = bandwidth;
        
        let sum: f64 = data.iter()
            .map(|&xi| {
                let z = (x - xi) / h;
                (-0.5 * z * z).exp()
            })
            .sum();
        
        sum / (n * (2.0 * std::f64::consts::PI).sqrt() * h)
    }
    
    /// Create violin shape from KDE points
    fn create_violin_shape(&self, kde_points: &[(f64, f64)], x_pos: f64, width: f32) -> Vec<EguiPlotPoint> {
        let half_width = width as f64 / 2.0;
        let mut shape = Vec::with_capacity(kde_points.len() * 2);
        
        // Left side of violin (reversed)
        for (y, density) in kde_points.iter() {
            shape.push(EguiPlotPoint::new(x_pos - density * half_width, *y));
        }
        
        // Right side of violin
        for (y, density) in kde_points.iter().rev() {
            shape.push(EguiPlotPoint::new(x_pos + density * half_width, *y));
        }
        
        shape
    }
    
    /// Process data for violin plot
    fn process_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<(String, Vec<f64>, BoxPlotStats)>, String> {
        let _data_processor = DataProcessor::new();
        
        // Get column indices
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or_else(|| format!("Y column '{}' not found", config.y_column))?;
        
        // Check if we have a group column
        let group_col = config.group_column.as_ref();
        
        // Process data based on whether we have groups
        if let Some(group_col) = group_col {
            // Get unique groups
            let group_idx = query_result.columns.iter().position(|c| c == group_col)
                .ok_or_else(|| format!("Group column '{}' not found", group_col))?;
            
            let mut groups = std::collections::HashMap::new();
            
            // Collect data points for each group
            for row in &query_result.rows {
                if row.len() > y_idx && row.len() > group_idx {
                    if let Ok(y_val) = row[y_idx].parse::<f64>() {
                        let group = row[group_idx].clone();
                        groups.entry(group).or_insert_with(Vec::new).push(y_val);
                    }
                }
            }
            
            // Calculate box plot statistics for each group
            let mut result = Vec::new();
            for (group, values) in groups {
                // Calculate box plot statistics manually since compute_single_box_plot_stats is private
                let stats = self.calculate_box_plot_stats(&values, Some(group.clone()));
                result.push((group, values, stats));
            }
            
            // Sort groups by name
            result.sort_by(|a, b| a.0.cmp(&b.0));
            
            Ok(result)
        } else {
            // No grouping, process all data together
            let mut values = Vec::new();
            
            // Collect all data points
            for row in &query_result.rows {
                if row.len() > y_idx {
                    if let Ok(y_val) = row[y_idx].parse::<f64>() {
                        values.push(y_val);
                    }
                }
            }
            
            // Calculate box plot statistics manually
            let stats = self.calculate_box_plot_stats(&values, None);
            
            Ok(vec![(config.y_column.clone(), values, stats)])
        }
    }
    
    /// Calculate box plot statistics manually
    fn calculate_box_plot_stats(&self, values: &[f64], group: Option<String>) -> BoxPlotStats {
        if values.is_empty() {
            // Return default stats for empty data
            return BoxPlotStats {
                group,
                min: 0.0,
                q1: 0.0,
                median: 0.0,
                q3: 0.0,
                max: 0.0,
                mean: 0.0,
                outliers: vec![],
                count: 0,
                lower_fence: 0.0,
                upper_fence: 0.0,
            };
        }
        
        // Sort values for percentile calculations
        let mut sorted_values = values.to_vec();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let count = sorted_values.len();
        
        // Calculate min, max
        let min = *sorted_values.first().unwrap();
        let max = *sorted_values.last().unwrap();
        
        // Calculate quartiles
        let q1_idx = (count as f64 * 0.25) as usize;
        let median_idx = (count as f64 * 0.5) as usize;
        let q3_idx = (count as f64 * 0.75) as usize;
        
        let q1 = sorted_values[q1_idx];
        let median = sorted_values[median_idx];
        let q3 = sorted_values[q3_idx];
        
        // Calculate mean
        let sum: f64 = sorted_values.iter().sum();
        let mean = sum / count as f64;
        
        // Calculate IQR and fences for outlier detection
        let iqr = q3 - q1;
        let lower_fence = q1 - 1.5 * iqr;
        let upper_fence = q3 + 1.5 * iqr;
        
        // Find outliers
        let outliers: Vec<f64> = sorted_values.iter()
            .filter(|&&x| x < lower_fence || x > upper_fence)
            .cloned()
            .collect();
        
        BoxPlotStats {
            group,
            min,
            q1,
            median,
            q3,
            max,
            mean,
            outliers,
            count,
            lower_fence,
            upper_fence,
        }
    }
}

impl Default for ViolinPlot {
    fn default() -> Self {
        Self::new()
    }
}

impl PlotTrait for ViolinPlot {
    fn name(&self) -> &'static str {
        "Violin Plot"
    }
    
    fn required_x_types(&self) -> Option<Vec<datafusion::arrow::datatypes::DataType>> {
        // Violin plots don't strictly need an X column, but can use categorical X for grouping
        None
    }
    
    fn required_y_types(&self) -> Vec<datafusion::arrow::datatypes::DataType> {
        // Y axis must be numeric for distribution analysis
        vec![
            datafusion::arrow::datatypes::DataType::Int8, datafusion::arrow::datatypes::DataType::Int16, datafusion::arrow::datatypes::DataType::Int32, datafusion::arrow::datatypes::DataType::Int64,
            datafusion::arrow::datatypes::DataType::UInt8, datafusion::arrow::datatypes::DataType::UInt16, datafusion::arrow::datatypes::DataType::UInt32, datafusion::arrow::datatypes::DataType::UInt64,
            datafusion::arrow::datatypes::DataType::Float16, datafusion::arrow::datatypes::DataType::Float32, datafusion::arrow::datatypes::DataType::Float64,
            datafusion::arrow::datatypes::DataType::Decimal128(38, 10), datafusion::arrow::datatypes::DataType::Decimal256(76, 10),
        ]
    }
    
    fn supports_multiple_series(&self) -> bool {
        true
    }
    
    fn get_default_config(&self) -> PlotConfiguration {
        let mut config = PlotConfiguration::default();
        config.plot_specific = PlotSpecificConfig::Violin(ViolinPlotConfig::default());
        config
    }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        // Process data for violin plot
        let violin_data = self.process_data(query_result, config)?;
        
        // Create series for each group
        let mut series = Vec::new();
        let colors = super::get_categorical_colors(&config.color_scheme);
        
        for (i, (group, _values, stats)) in violin_data.iter().enumerate() {
            let color = colors[i % colors.len()];
            
            // Create tooltip data
            let mut tooltip_data = HashMap::new();
            tooltip_data.insert("Group".to_string(), group.clone());
            tooltip_data.insert("Count".to_string(), stats.count.to_string());
            tooltip_data.insert("Min".to_string(), format!("{:.2}", stats.min));
            tooltip_data.insert("Q1".to_string(), format!("{:.2}", stats.q1));
            tooltip_data.insert("Median".to_string(), format!("{:.2}", stats.median));
            tooltip_data.insert("Q3".to_string(), format!("{:.2}", stats.q3));
            tooltip_data.insert("Max".to_string(), format!("{:.2}", stats.max));
            tooltip_data.insert("Mean".to_string(), format!("{:.2}", stats.mean));
            
            // Create a single point to represent this violin
            let point = super::PlotPoint {
                x: i as f64,
                y: stats.median,
                z: None,
                label: Some(group.clone()),
                color: Some(color),
                size: None,
                series_id: Some(group.clone()),
                tooltip_data,
            };
            
            // Create series
            let series_data = DataSeries {
                id: group.clone(),
                name: group.clone(),
                points: vec![point],
                color,
                visible: true,
                style: SeriesStyle::Points { size: 0.0, shape: super::MarkerShape::Circle }, // Style doesn't matter for violin plots
            };
            
            series.push(series_data);
        }
        
        // Create plot metadata
        let metadata = PlotMetadata {
            title: config.title.clone(),
            x_label: if config.group_column.is_some() { config.group_column.as_ref().unwrap().clone() } else { "Group".to_string() },
            y_label: config.y_column.clone(),
            show_legend: config.show_legend,
            show_grid: config.show_grid,
            color_scheme: config.color_scheme.clone(),
            extra_data: None,
        };
        
        // Flatten points for backward compatibility
        let points = series.iter().flat_map(|s| s.points.clone()).collect();
        
        // Store the raw data and statistics for rendering
        let statistics = Some(DataStatistics {
            mean_x: 0.0,
            mean_y: 0.0,
            std_x: 0.0,
            std_y: 0.0,
            correlation: None,
            count: violin_data.iter().map(|(_, values, _)| values.len()).sum(),
        });
        
        Ok(PlotData {
            points,
            series,
            metadata,
            statistics,
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label("No data points to display");
                ui.label(RichText::new("Configure Y column for distribution analysis").weak());
            });
            return;
        }
        
        // Get violin plot specific config
        let violin_config = if let PlotSpecificConfig::Violin(cfg) = &config.plot_specific {
            cfg
        } else {
            // Use default config if not properly configured
            &ViolinPlotConfig::default()
        };
        
        // Create plot
        let plot = Plot::new("violin_plot")
            .x_axis_label(&data.metadata.x_label)
            .y_axis_label(&data.metadata.y_label)
            .show_grid(data.metadata.show_grid)
            .allow_zoom(config.allow_zoom)
            .allow_drag(config.allow_pan)
            .allow_boxed_zoom(config.allow_zoom);
        
        // Add legend if enabled
        let plot = if data.metadata.show_legend {
            plot.legend(Legend::default())
        } else {
            plot
        };
        
        // Extract raw data from the series for violin plot rendering
        let mut violin_data = Vec::new();
        
        // If we have multiple series (groups), process each one
        if data.series.len() > 1 {
            for (i, series) in data.series.iter().enumerate() {
                if !series.visible {
                    continue;
                }
                
                // Extract Y values from the series points
                let values: Vec<f64> = series.points.iter().map(|p| p.y).collect();
                
                // Calculate box plot statistics
                let stats = self.calculate_box_plot_stats(&values, Some(series.name.clone()));
                
                violin_data.push((series.name.clone(), values, stats));
            }
        } else {
            // Single group - extract all Y values
            let values: Vec<f64> = data.points.iter().map(|p| p.y).collect();
            let stats = self.calculate_box_plot_stats(&values, None);
            let group_name = if let Some(series) = data.series.first() {
                series.name.clone()
            } else {
                config.y_column.clone()
            };
            violin_data.push((group_name, values, stats));
        }
        
        // Find global min/max for comparison mode
        let mut global_min = f64::INFINITY;
        let mut global_max = f64::NEG_INFINITY;
        
        if violin_config.comparison_mode {
            for (_, _, stats) in &violin_data {
                global_min = global_min.min(stats.min);
                global_max = global_max.max(stats.max);
            }
        }
        
        plot.show(ui, |plot_ui| {
            // Determine x positions based on comparison mode
            let x_positions: Vec<f64> = if violin_config.comparison_mode && violin_data.len() > 1 {
                // In comparison mode, center all violins at x=0
                vec![0.0; violin_data.len()]
            } else {
                // Normal mode, space violins along x-axis
                (0..violin_data.len()).map(|i| i as f64).collect()
            };
            
            // Render each violin
            for (i, ((group, values, stats), x_pos)) in violin_data.iter().zip(x_positions.iter()).enumerate() {
                let color = data.series[i].color;
                
                // Calculate KDE for violin shape
                let kde_points = self.calculate_kde(values, violin_config.bandwidth, violin_config.kde_points);
                
                // Create violin shape
                let violin_shape = self.create_violin_shape(&kde_points, *x_pos, violin_config.violin_width);
                
                // Convert to the format expected by Polygon::new
                let plot_points: Vec<[f64; 2]> = violin_shape.iter()
                    .map(|p| [p.x, p.y])
                    .collect();
                
                // Draw violin shape
                let violin = Polygon::new(plot_points)
                    .fill_color(color.gamma_multiply(0.5))
                    .stroke(Stroke::new(1.0, color))
                    .name(group.clone());
                
                plot_ui.polygon(violin);
                
                // Draw distribution curve if enabled
                if violin_config.show_distribution_curve {
                    // Create a line for the right side of the violin (density curve)
                    let mut curve_points = Vec::with_capacity(kde_points.len());
                    let half_width = violin_config.violin_width as f64 / 2.0;
                    
                    for (y, density) in kde_points.iter() {
                        curve_points.push([*x_pos + density * half_width, *y]);
                    }
                    
                    let curve = Line::new(curve_points)
                        .color(color)
                        .width(2.0)
                        .name(format!("{} density", group));
                    
                    plot_ui.line(curve);
                }
                
                // Draw box plot overlay if enabled
                if violin_config.show_box_plot {
                    // Box width proportional to violin width
                    let box_half_width = violin_config.violin_width as f64 * 0.2;
                    
                    // Draw box (rectangle from Q1 to Q3)
                    let box_rect = Polygon::new(vec![
                        [*x_pos - box_half_width, stats.q1],
                        [*x_pos + box_half_width, stats.q1],
                        [*x_pos + box_half_width, stats.q3],
                        [*x_pos - box_half_width, stats.q3],
                    ])
                    .fill_color(color.gamma_multiply(0.2))
                    .stroke(Stroke::new(1.0, color));
                    
                    plot_ui.polygon(box_rect);
                    
                    // Draw median line
                    if violin_config.show_median {
                        let median_line = Line::new(vec![
                            [*x_pos - box_half_width, stats.median],
                            [*x_pos + box_half_width, stats.median],
                        ])
                        .color(color)
                        .width(2.0);
                        
                        plot_ui.line(median_line);
                    }
                    
                    // Draw quartile lines if enabled
                    if violin_config.show_quartiles {
                        // Draw whiskers (lines from box to min/max)
                        let upper_whisker = Line::new(vec![
                            [*x_pos, stats.q3],
                            [*x_pos, stats.max],
                        ])
                        .color(color)
                        .width(1.0);
                        
                        let lower_whisker = Line::new(vec![
                            [*x_pos, stats.q1],
                            [*x_pos, stats.min],
                        ])
                        .color(color)
                        .width(1.0);
                        
                        plot_ui.line(upper_whisker);
                        plot_ui.line(lower_whisker);
                        
                        // Draw whisker caps
                        let cap_width = box_half_width * 0.8;
                        let upper_cap = Line::new(vec![
                            [*x_pos - cap_width, stats.max],
                            [*x_pos + cap_width, stats.max],
                        ])
                        .color(color)
                        .width(1.0);
                        
                        let lower_cap = Line::new(vec![
                            [*x_pos - cap_width, stats.min],
                            [*x_pos + cap_width, stats.min],
                        ])
                        .color(color)
                        .width(1.0);
                        
                        plot_ui.line(upper_cap);
                        plot_ui.line(lower_cap);
                    }
                }
                
                // Draw mean if enabled
                if violin_config.show_mean {
                    let mean_line = Line::new(vec![
                        [*x_pos - violin_config.violin_width as f64 * 0.25, stats.mean],
                        [*x_pos + violin_config.violin_width as f64 * 0.25, stats.mean],
                    ])
                    .color(Color32::RED)
                    .width(2.0);
                    
                    plot_ui.line(mean_line);
                }
                
                // Draw outliers if enabled
                if violin_config.show_outliers && !stats.outliers.is_empty() {
                    let outlier_points: Vec<[f64; 2]> = stats.outliers.iter()
                        .map(|&y| [*x_pos, y])
                        .collect();
                    
                    let outliers = Points::new(outlier_points)
                        .color(color)
                        .radius(3.0)
                        .shape(EguiMarkerShape::Cross);
                    
                    plot_ui.points(outliers);
                }
                
                // Add group label in normal mode (not comparison mode)
                if !violin_config.comparison_mode || violin_data.len() == 1 {
                    // Calculate label position based on data range
                    let y_pos = if violin_config.comparison_mode {
                        global_min - (global_max - global_min) * 0.05
                    } else {
                        stats.min - (stats.max - stats.min) * 0.05
                    };
                    
                    plot_ui.text(egui_plot::Text::new(
                        EguiPlotPoint::new(*x_pos, y_pos),
                        RichText::new(group).color(color).size(10.0)
                    ));
                }
                
                // Add interactive tooltips for violin regions
                if config.show_tooltips {
                    // Create invisible points for tooltips at key statistical positions
                    let tooltip_points = vec![
                        [*x_pos, stats.min],
                        [*x_pos, stats.q1],
                        [*x_pos, stats.median],
                        [*x_pos, stats.q3],
                        [*x_pos, stats.max],
                        [*x_pos, stats.mean],
                    ];
                    
                    let tooltip_labels = vec![
                        format!("{}: Min = {:.2}", group, stats.min),
                        format!("{}: Q1 = {:.2}", group, stats.q1),
                        format!("{}: Median = {:.2}", group, stats.median),
                        format!("{}: Q3 = {:.2}", group, stats.q3),
                        format!("{}: Max = {:.2}", group, stats.max),
                        format!("{}: Mean = {:.2}", group, stats.mean),
                    ];
                    
                    for (point, label) in tooltip_points.iter().zip(tooltip_labels.iter()) {
                        let tooltip_point = Points::new(vec![*point])
                            .color(Color32::TRANSPARENT)
                            .radius(5.0)
                            .name(label);
                        
                        plot_ui.points(tooltip_point);
                    }
                }
            }
            
            // Add legend for comparison mode
            if violin_config.comparison_mode && violin_data.len() > 1 {
                for (i, (group, _, _)) in violin_data.iter().enumerate() {
                    let color = data.series[i].color;
                    let x_pos = 0.0; // All centered at x=0
                    let y_pos = global_max + (global_max - global_min) * 0.05 * (i as f64 + 1.0);
                    
                    // Add colored label for each group
                    plot_ui.text(egui_plot::Text::new(
                        EguiPlotPoint::new(x_pos, y_pos),
                        RichText::new(group).color(color).size(12.0)
                    ));
                }
            }
        });
        
        // Show statistics and interactive controls
        ui.horizontal(|ui| {
            ui.label(format!("Total points: {}", data.statistics.as_ref().map_or(0, |s| s.count)));
            
            if data.series.len() > 1 {
                ui.label(format!("Groups: {}", data.series.len()));
                
                // Add comparison mode toggle
                if ui.checkbox(&mut (violin_config.comparison_mode as bool), "Comparison Mode").changed() {
                    // This is a bit of a hack since we can't directly modify the config
                    // The actual change will happen on the next frame
                }
            }
        });
        
        // Add interactive controls for violin plot parameters
        ui.collapsing("Violin Plot Controls", |ui| {
            ui.horizontal(|ui| {
                ui.label("Bandwidth:");
                let mut bandwidth = violin_config.bandwidth;
                if ui.add(egui::Slider::new(&mut bandwidth, 0.1..=2.0).step_by(0.1)).changed() {
                    // This is a bit of a hack since we can't directly modify the config
                    // The actual change will happen on the next frame
                }
                
                ui.label("Width:");
                let mut width = violin_config.violin_width;
                if ui.add(egui::Slider::new(&mut width, 0.2..=1.5).step_by(0.1)).changed() {
                    // This is a bit of a hack since we can't directly modify the config
                    // The actual change will happen on the next frame
                }
            });
            
            ui.horizontal(|ui| {
                ui.checkbox(&mut (violin_config.show_box_plot as bool), "Show Box Plot");
                ui.checkbox(&mut (violin_config.show_mean as bool), "Show Mean");
                ui.checkbox(&mut (violin_config.show_median as bool), "Show Median");
            });
            
            ui.horizontal(|ui| {
                ui.checkbox(&mut (violin_config.show_quartiles as bool), "Show Quartiles");
                ui.checkbox(&mut (violin_config.show_outliers as bool), "Show Outliers");
                ui.checkbox(&mut (violin_config.show_distribution_curve as bool), "Show Density Curve");
            });
            
            ui.horizontal(|ui| {
                ui.checkbox(&mut (violin_config.normalize_width as bool), "Normalize Width");
                
                ui.label("KDE Points:");
                let mut kde_points = violin_config.kde_points as i32;
                if ui.add(egui::Slider::new(&mut kde_points, 50..=300).step_by(10.0)).changed() {
                    // This is a bit of a hack since we can't directly modify the config
                    // The actual change will happen on the next frame
                }
            });
            
            // Add statistical summary for the selected data
            ui.separator();
            ui.label(RichText::new("Statistical Summary").strong());
            
            if let Some(stats) = data.statistics.as_ref() {
                ui.label(format!("Total data points: {}", stats.count));
                
                if data.series.len() > 1 {
                    ui.label(format!("Number of groups: {}", data.series.len()));
                }
            }
            
            // Show distribution properties if we have data
            if !violin_data.is_empty() {
                let (_, values, stats) = &violin_data[0];
                if !values.is_empty() {
                    // Calculate additional statistics
                    let variance: f64 = values.iter()
                        .map(|&x| (x - stats.mean).powi(2))
                        .sum::<f64>() / values.len() as f64;
                    
                    let std_dev = variance.sqrt();
                    
                    // Calculate skewness
                    let skewness: f64 = values.iter()
                        .map(|&x| (x - stats.mean).powi(3))
                        .sum::<f64>() / (values.len() as f64 * std_dev.powi(3));
                    
                    // Calculate kurtosis
                    let kurtosis: f64 = values.iter()
                        .map(|&x| (x - stats.mean).powi(4))
                        .sum::<f64>() / (values.len() as f64 * variance.powi(2));
                    
                    ui.label(format!("Mean: {:.2}", stats.mean));
                    ui.label(format!("Median: {:.2}", stats.median));
                    ui.label(format!("Standard Deviation: {:.2}", std_dev));
                    ui.label(format!("Variance: {:.2}", variance));
                    ui.label(format!("Skewness: {:.2}", skewness));
                    ui.label(format!("Kurtosis: {:.2}", kurtosis));
                    
                    // Describe the distribution shape
                    let shape_description = if skewness < -0.5 {
                        "Negatively skewed (left-tailed)"
                    } else if skewness > 0.5 {
                        "Positively skewed (right-tailed)"
                    } else {
                        "Approximately symmetric"
                    };
                    
                    let peakedness = if kurtosis < 3.0 {
                        "Platykurtic (flatter than normal distribution)"
                    } else if kurtosis > 3.0 {
                        "Leptokurtic (more peaked than normal distribution)"
                    } else {
                        "Mesokurtic (similar to normal distribution)"
                    };
                    
                    ui.label(shape_description);
                    ui.label(peakedness);
                    
                    // Add outlier information if present
                    if !stats.outliers.is_empty() {
                        ui.label(format!("Outliers detected: {} points", stats.outliers.len()));
                        ui.label(format!("Outlier range: < {:.2} or > {:.2}", stats.lower_fence, stats.upper_fence));
                    } else {
                        ui.label("No outliers detected");
                    }
                }
            }
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() {
            ui.group(|ui| {
                ui.label(RichText::new("Groups:").strong());
                ui.separator();
                
                for series in &data.series {
                    if series.visible {
                        ui.horizontal(|ui| {
                            ui.colored_label(series.color, "▮");
                            ui.label(&series.name);
                        });
                    }
                }
                
                // Show explanation of violin plot
                ui.separator();
                ui.label(RichText::new("Violin Plot Elements:").strong());
                
                // Get violin plot specific config to check which elements are enabled
                let violin_config = if let PlotSpecificConfig::Violin(cfg) = &config.plot_specific {
                    cfg
                } else {
                    &ViolinPlotConfig::default()
                };
                
                ui.label("• Violin shape: Distribution density");
                
                if violin_config.show_box_plot {
                    ui.label("• Box: Quartiles (25%, 50%, 75%)");
                }
                
                if violin_config.show_mean {
                    ui.label("• Red line: Mean value");
                }
                
                if violin_config.show_quartiles {
                    ui.label("• Whiskers: Min/Max (excluding outliers)");
                }
                
                if violin_config.show_outliers {
                    ui.label("• Crosses: Outliers");
                }
                
                if violin_config.show_distribution_curve {
                    ui.label("• Curve: Kernel density estimation");
                }
                
                // Add statistical interpretation guide
                if data.series.len() == 1 && !data.points.is_empty() {
                    ui.separator();
                    ui.label(RichText::new("Distribution Guide:").strong());
                    ui.label("• Symmetric: Equal spread around median");
                    ui.label("• Skewed right: Longer tail on right side");
                    ui.label("• Skewed left: Longer tail on left side");
                    ui.label("• Bimodal: Two distinct peaks");
                    ui.label("• Wide spread: High variability in data");
                }
            });
        }
    }
    
    fn handle_interaction(&self, _ui: &mut Ui, _data: &PlotData, _config: &PlotConfiguration) -> Option<PlotInteraction> {
        // Enhanced interaction handling for violin plots
        // This could be extended in the future to support selection of specific distribution regions
        // or interactive bandwidth adjustment
        None
    }
}