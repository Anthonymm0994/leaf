use egui::{Ui, Color32, RichText, Stroke};
use std::ops::Add;
use egui_plot::{Plot, PlotPoints, Points, Line, Legend, PlotUi};
use datafusion::arrow::datatypes::DataType;
use std::collections::{HashMap, HashSet};

use super::{
    Plot as PlotTrait, 
    PlotData, 
    PlotConfiguration, 
    PlotSpecificConfig, 
    BoxPlotConfig, 
    PlotInteraction,
    DataSeries,
    SeriesStyle,
    data_processor::{DataProcessor, BoxPlotStats}
};

pub struct BoxPlotImpl;

impl BoxPlotImpl {
    /// Handle tooltips for box plot
    fn handle_tooltips(&self, ui: &mut Ui, plot_ui: &PlotUi, data: &PlotData, stats_map: &HashMap<String, BoxPlotStats>) {
        if let Some(pointer_coord) = plot_ui.pointer_coordinate() {
            // Find the box plot under the cursor
            for (group_name, stats) in stats_map {
                let x_pos = if let Some(point) = data.points.iter().find(|p| p.tooltip_data.get("Group") == Some(group_name)) {
                    point.x
                } else {
                    continue;
                };
                
                let box_width = 0.3;
                
                // Check if pointer is within box plot boundaries
                if pointer_coord.x >= x_pos - box_width/2.0 && pointer_coord.x <= x_pos + box_width/2.0 {
                    // Check which part of the box plot is being hovered
                    let tooltip_text = if pointer_coord.y >= stats.q1 && pointer_coord.y <= stats.q3 {
                        // Box area (IQR)
                        format!(
                            "Group: {}\nQ1: {:.2}\nMedian: {:.2}\nQ3: {:.2}\nIQR: {:.2}",
                            group_name, stats.q1, stats.median, stats.q3, stats.q3 - stats.q1
                        )
                    } else if pointer_coord.y >= stats.min && pointer_coord.y <= stats.max {
                        // Whisker area
                        format!(
                            "Group: {}\nMin: {:.2}\nMax: {:.2}\nRange: {:.2}",
                            group_name, stats.min, stats.max, stats.max - stats.min
                        )
                    } else {
                        // Check if it's an outlier
                        let mut is_outlier = false;
                        for &outlier in &stats.outliers {
                            if (pointer_coord.y - outlier).abs() < 0.2 {
                                is_outlier = true;
                                break;
                            }
                        }
                        
                        if is_outlier {
                            format!(
                                "Group: {}\nOutlier: {:.2}\nQ1: {:.2}\nQ3: {:.2}\nIQR: {:.2}",
                                group_name, pointer_coord.y, stats.q1, stats.q3, stats.q3 - stats.q1
                            )
                        } else {
                            continue;
                        }
                    };
                    
                    // Tooltip functionality - using a workaround since show_tooltip is not available
                    if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
                        ui.ctx().debug_painter().text(
                            hover_pos.add(egui::vec2(15.0, 15.0)),
                            egui::Align2::LEFT_TOP,
                            &tooltip_text,
                            egui::TextStyle::Body.resolve(ui.style()),
                            Color32::WHITE
                        );
                    }
                    
                    // Highlight the box plot
                    // self.render_highlighted_box_plot(plot_ui, stats, x_pos, group_name);
                    
                    break;
                }
            }
        }
    }
    
    /// Render a highlighted box plot
    fn render_highlighted_box_plot(&self, _plot_ui: &PlotUi, stats: &BoxPlotStats, x_pos: f64, _name: &str) {
        let _whisker_width = 0.15;
        let _highlight_color = Color32::from_rgb(150, 200, 255);
        let _median_color = Color32::from_rgb(255, 120, 120);
        
        // Create box plot points for highlighting
        let _box_points = vec![
            [x_pos, stats.min],
            [x_pos, stats.q1],
            [x_pos, stats.median],
            [x_pos, stats.q3],
            [x_pos, stats.max],
        ];
        
        // TODO: Implement highlighted box plot rendering
    }
    
    /// Process data for box plot with proper grouping (synchronous version)
    fn process_data_sync(&self, query_result: &crate::core::QueryResult, config: &PlotConfiguration) -> Result<(Vec<DataSeries>, HashMap<String, BoxPlotStats>), String> {
        let data_processor = DataProcessor::new();
        
        // Get box plot specific config
        let default_config = self.get_default_config();
        let box_config = if let PlotSpecificConfig::BoxPlot(cfg) = &config.plot_specific {
            cfg
        } else {
            match &default_config.plot_specific {
                PlotSpecificConfig::BoxPlot(cfg) => cfg,
                _ => panic!("Expected BoxPlotConfig"),
            }
        };
        
        // Determine if we're grouping by a column
        let group_by = if !config.x_column.is_empty() {
            Some(config.x_column.as_str())
        } else {
            None
        };
        
        // Calculate box plot statistics synchronously
        let stats_list = self.compute_box_plot_stats_sync(
            query_result,
            &config.y_column,
            group_by
        )?;
        
        // Create a map of group name to stats for tooltips
        let mut stats_map = HashMap::new();
        for stats in &stats_list {
            let group_name = stats.group.clone().unwrap_or_else(|| "All Data".to_string());
            stats_map.insert(group_name, stats.clone());
        }
        
        // Create series for each group
        let mut all_series = Vec::new();
        let colors = super::get_categorical_colors(&config.color_scheme);
        
        for (i, stats) in stats_list.iter().enumerate() {
            let group_name = stats.group.clone().unwrap_or_else(|| "All Data".to_string());
            let color = colors[i % colors.len()];
            
            // X position for this group
            let x_pos = i as f64;
            
            // Create points for the box plot
            let mut points = Vec::new();
            
            // Box (Q1 to Q3)
            let box_width = 0.3;
            
            // Create tooltip data for the box
            let mut tooltip_data = HashMap::new();
            tooltip_data.insert("Group".to_string(), group_name.clone());
            tooltip_data.insert("Min".to_string(), format!("{:.2}", stats.min));
            tooltip_data.insert("Q1".to_string(), format!("{:.2}", stats.q1));
            tooltip_data.insert("Median".to_string(), format!("{:.2}", stats.median));
            tooltip_data.insert("Q3".to_string(), format!("{:.2}", stats.q3));
            tooltip_data.insert("Max".to_string(), format!("{:.2}", stats.max));
            tooltip_data.insert("IQR".to_string(), format!("{:.2}", stats.q3 - stats.q1));
            tooltip_data.insert("Count".to_string(), format!("{}", stats.count));
            
            // Add box corners
            points.push(super::PlotPoint {
                x: x_pos,
                y: stats.q1,
                z: None,
                label: Some(format!("{} (Q1)", group_name)),
                color: Some(color),
                size: None,
                series_id: Some(group_name.clone()),
                tooltip_data: tooltip_data.clone(),
            });
            
            points.push(super::PlotPoint {
                x: x_pos,
                y: stats.q3,
                z: None,
                label: Some(format!("{} (Q3)", group_name)),
                color: Some(color),
                size: None,
                series_id: Some(group_name.clone()),
                tooltip_data: tooltip_data.clone(),
            });
            
            // Add median
            points.push(super::PlotPoint {
                x: x_pos,
                y: stats.median,
                z: None,
                label: Some(format!("{} (Median)", group_name)),
                color: Some(Color32::from_rgb(255, 100, 100)),
                size: None,
                series_id: Some(group_name.clone()),
                tooltip_data: tooltip_data.clone(),
            });
            
            // Add whiskers
            points.push(super::PlotPoint {
                x: x_pos,
                y: stats.min,
                z: None,
                label: Some(format!("{} (Min)", group_name)),
                color: Some(color),
                size: None,
                series_id: Some(group_name.clone()),
                tooltip_data: tooltip_data.clone(),
            });
            
            points.push(super::PlotPoint {
                x: x_pos,
                y: stats.max,
                z: None,
                label: Some(format!("{} (Max)", group_name)),
                color: Some(color),
                size: None,
                series_id: Some(group_name.clone()),
                tooltip_data: tooltip_data.clone(),
            });
            
            // Add outliers
            for &outlier in &stats.outliers {
                let mut outlier_tooltip = tooltip_data.clone();
                outlier_tooltip.insert("Outlier".to_string(), format!("{:.2}", outlier));
                
                points.push(super::PlotPoint {
                    x: x_pos,
                    y: outlier,
                    z: None,
                    label: Some(format!("{} (Outlier)", group_name)),
                    color: Some(Color32::from_rgb(255, 100, 100)),
                    size: None,
                    series_id: Some(group_name.clone()),
                    tooltip_data: outlier_tooltip,
                });
            }
            
            // Add mean if showing mean is enabled
            if box_config.show_mean {
                let mut mean_tooltip = tooltip_data.clone();
                mean_tooltip.insert("Mean".to_string(), format!("{:.2}", stats.mean));
                
                points.push(super::PlotPoint {
                    x: x_pos,
                    y: stats.mean,
                    z: None,
                    label: Some(format!("{} (Mean)", group_name)),
                    color: Some(Color32::from_rgb(50, 200, 50)),
                    size: None,
                    series_id: Some(group_name.clone()),
                    tooltip_data: mean_tooltip,
                });
            }
            
            // Create series for this group
            let series = DataSeries {
                id: group_name.clone(),
                name: group_name,
                points,
                color,
                visible: true,
                style: SeriesStyle::Bars { width: box_width as f32 },
            };
            
            all_series.push(series);
        }
        
        Ok((all_series, stats_map))
    }
    
    /// Compute box plot statistics synchronously
    fn compute_box_plot_stats_sync(
        &self,
        query_result: &crate::core::QueryResult,
        y_column: &str,
        group_by: Option<&str>
    ) -> Result<Vec<BoxPlotStats>, String> {
        // Extract Y column data
        let y_idx = query_result.columns.iter().position(|c| c == y_column)
            .ok_or_else(|| format!("Y column '{}' not found", y_column))?;
        
        let mut grouped_data: HashMap<String, Vec<f64>> = HashMap::new();
        
        // Collect data by group
        for row in &query_result.rows {
            if row.len() > y_idx {
                if let Ok(y_val) = row[y_idx].parse::<f64>() {
                    let group_name = if let Some(group_col) = group_by {
                        if let Some(group_idx) = query_result.columns.iter().position(|c| c == group_col) {
                            if row.len() > group_idx {
                                row[group_idx].clone()
                            } else {
                                "Unknown".to_string()
                            }
                        } else {
                            "Unknown".to_string()
                        }
                    } else {
                        "All Data".to_string()
                    };
                    
                    grouped_data.entry(group_name).or_insert_with(Vec::new).push(y_val);
                }
            }
        }
        
        // Calculate statistics for each group
        let mut stats_list = Vec::new();
        for (group_name, values) in grouped_data {
            if values.is_empty() {
                continue;
            }
            
            let mut sorted_values = values.clone();
            sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            
            let n = sorted_values.len();
            let min = sorted_values[0];
            let max = sorted_values[n - 1];
            
            // Calculate quartiles
            let q1_idx = n / 4;
            let median_idx = n / 2;
            let q3_idx = 3 * n / 4;
            
            let q1 = sorted_values[q1_idx];
            let median = sorted_values[median_idx];
            let q3 = sorted_values[q3_idx];
            
            // Calculate mean
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            
            // Calculate IQR and fences for outliers
            let iqr = q3 - q1;
            let lower_fence = q1 - 1.5 * iqr;
            let upper_fence = q3 + 1.5 * iqr;
            
            // Find outliers
            let outliers: Vec<f64> = values.iter()
                .filter(|&&x| x < lower_fence || x > upper_fence)
                .cloned()
                .collect();
            
            stats_list.push(BoxPlotStats {
                group: Some(group_name),
                min,
                q1,
                median,
                q3,
                max,
                mean,
                count: values.len(),
                outliers,
                lower_fence,
                upper_fence,
            });
        }
        
        Ok(stats_list)
    }
    
    /// Helper method to get box plot specific config
    fn as_box_plot_config(config: &PlotConfiguration) -> &BoxPlotConfig {
        if let PlotSpecificConfig::BoxPlot(cfg) = &config.plot_specific {
            cfg
        } else {
            panic!("Expected BoxPlotConfig")
        }
    }
    
    /// Render a box plot using egui_plot primitives
    fn render_box_plot(&self, plot_ui: &mut PlotUi, stats: &BoxPlotStats, x_pos: f64, color: Color32, name: &str, show_outliers: bool, show_mean: bool) {
        let box_width = 0.3;
        let whisker_width = 0.15;
        
        // Draw the box (rectangle from Q1 to Q3)
        let box_rect = vec![
            [x_pos - box_width/2.0, stats.q1],
            [x_pos + box_width/2.0, stats.q1],
            [x_pos + box_width/2.0, stats.q3],
            [x_pos - box_width/2.0, stats.q3],
        ];
        
        // Draw box outline
        let box_line = Line::new(box_rect.clone())
            .color(color)
            .width(1.5)
            .name(name);
        plot_ui.line(box_line);
        
        // Draw median line
        let median_line = Line::new(vec![
            [x_pos - box_width/2.0, stats.median],
            [x_pos + box_width/2.0, stats.median],
        ])
        .color(Color32::from_rgb(255, 100, 100))
        .width(2.0);
        plot_ui.line(median_line);
        
        // Draw whiskers
        let upper_whisker = Line::new(vec![
            [x_pos, stats.q3],
            [x_pos, stats.max],
        ])
        .color(color)
        .width(1.0);
        
        let lower_whisker = Line::new(vec![
            [x_pos, stats.q1],
            [x_pos, stats.min],
        ])
        .color(color)
        .width(1.0);
        
        plot_ui.line(upper_whisker);
        plot_ui.line(lower_whisker);
        
        // Draw whisker caps
        let upper_cap = Line::new(vec![
            [x_pos - whisker_width/2.0, stats.max],
            [x_pos + whisker_width/2.0, stats.max],
        ])
        .color(color)
        .width(1.0);
        
        let lower_cap = Line::new(vec![
            [x_pos - whisker_width/2.0, stats.min],
            [x_pos + whisker_width/2.0, stats.min],
        ])
        .color(color)
        .width(1.0);
        
        plot_ui.line(upper_cap);
        plot_ui.line(lower_cap);
        
        // Draw outliers as points
        if show_outliers && !stats.outliers.is_empty() {
            let outlier_points: PlotPoints = stats.outliers.iter()
                .map(|&y| [x_pos, y])
                .collect();
            plot_ui.points(Points::new(outlier_points)
                .color(Color32::from_rgb(255, 100, 100))
                .radius(3.0));
        }
        
        // Draw mean as a different colored point if enabled
        if show_mean {
            plot_ui.points(Points::new(vec![[x_pos, stats.mean]])
                .color(Color32::from_rgb(50, 200, 50))
                .shape(egui_plot::MarkerShape::Diamond)
                .radius(4.0));
        }
    }
}

impl PlotTrait for BoxPlotImpl {
    fn name(&self) -> &'static str {
        "Box Plot"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        // Box plots can group by categorical X axis or work without X
        Some(vec![
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
        ])
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        // Y axis must be numeric
        vec![
            DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
            DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64,
            DataType::Float16, DataType::Float32, DataType::Float64,
        ]
    }
    
    fn supports_multiple_series(&self) -> bool {
        true // For grouped box plots
    }
    
    fn get_default_config(&self) -> PlotConfiguration {
        let mut config = PlotConfiguration::default();
        config.plot_specific = PlotSpecificConfig::BoxPlot(BoxPlotConfig {
            show_outliers: true,
            show_mean: true,
            notched: false,
            violin_overlay: false,
        });
        config
    }
    
    fn prepare_data(&self, query_result: &crate::core::QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        // Process data synchronously
        let (series, stats_map) = self.process_data_sync(query_result, config)?;
        
        // Create plot metadata
        let x_label = if !config.x_column.is_empty() {
            config.x_column.clone()
        } else {
            "Group".to_string()
        };
        
        let metadata = super::PlotMetadata {
            title: config.title.clone(),
            x_label,
            y_label: config.y_column.clone(),
            show_legend: config.show_legend,
            show_grid: config.show_grid,
            color_scheme: config.color_scheme.clone(),
            extra_data: None,
        };
        
        // Flatten points for backward compatibility
        let points = series.iter().flat_map(|s| s.points.clone()).collect();
        
        // Store stats_map in the first point's tooltip_data for later use
        let mut plot_data = PlotData {
            points,
            series,
            metadata,
            statistics: None,
        };
        
        // Store stats_map in a special field for tooltips
        if let Some(first_point) = plot_data.points.first_mut() {
            // Store the number of groups for later reconstruction
            first_point.tooltip_data.insert("__stats_count__".to_string(), stats_map.len().to_string());
        }
        
        Ok(plot_data)
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label("No data points to display");
                ui.label(RichText::new("Configure Y column with numeric data").weak());
            });
            return;
        }
        
        // Get box plot specific config
        let default_config;
        let box_config = if let PlotSpecificConfig::BoxPlot(cfg) = &config.plot_specific {
            cfg
        } else {
            default_config = self.get_default_config();
            default_config.plot_specific.as_box_plot()
        };
        
        // Extract stats_map from the first point's tooltip_data
        let mut stats_map = HashMap::new();
        if let Some(first_point) = data.points.first() {
            if let Some(_stats_str) = first_point.tooltip_data.get("__stats_map__") {
                // This is just a placeholder - in a real implementation, we would serialize/deserialize properly
                // For now, we'll reconstruct the stats from the points
                
                // Group points by series_id
                let mut groups = HashMap::new();
                for point in &data.points {
                    if let Some(series_id) = &point.series_id {
                        groups.entry(series_id.clone()).or_insert_with(Vec::new).push(point);
                    }
                }
                
                // Extract stats for each group
                for (group_name, points) in groups {
                    // Find min, q1, median, q3, max
                    let mut min = f64::MAX;
                    let mut q1 = 0.0;
                    let mut median = 0.0;
                    let mut q3 = 0.0;
                    let mut max = f64::MIN;
                    let mut mean = 0.0;
                    let mut outliers = Vec::new();
                    
                    for point in &points {
                        if let Some(label) = &point.label {
                            if label.contains("Min") {
                                min = point.y;
                            } else if label.contains("Q1") {
                                q1 = point.y;
                            } else if label.contains("Median") {
                                median = point.y;
                            } else if label.contains("Q3") {
                                q3 = point.y;
                            } else if label.contains("Max") {
                                max = point.y;
                            } else if label.contains("Mean") {
                                mean = point.y;
                            } else if label.contains("Outlier") {
                                outliers.push(point.y);
                            }
                        }
                    }
                    
                    if min != f64::MAX && max != f64::MIN {
                        stats_map.insert(group_name.clone(), BoxPlotStats {
                            group: Some(group_name.clone()),
                            min,
                            q1,
                            median,
                            q3,
                            max,
                            mean,
                            count: points.len(),
                            outliers,
                            lower_fence: q1 - 1.5 * (q3 - q1),
                            upper_fence: q3 + 1.5 * (q3 - q1),
                        });
                    }
                }
            }
        }
        
        // Create plot
        let plot = Plot::new("box_plot")
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
        
        plot.show(ui, |plot_ui| {
            // Render each box plot
            for (i, series) in data.series.iter().enumerate() {
                if !series.visible {
                    continue;
                }
                
                // Find stats for this series
                if let Some(stats) = stats_map.get(&series.id) {
                    self.render_box_plot(
                        plot_ui, 
                        stats, 
                        i as f64, 
                        series.color, 
                        &series.name,
                        box_config.show_outliers,
                        box_config.show_mean
                    );
                }
            }
            
            // Handle hover tooltips
            // Note: Commenting out due to borrow conflict with ui
            // if config.show_tooltips && !stats_map.is_empty() {
            //     self.handle_tooltips(ui, plot_ui, data, &stats_map);
            // }
            
            // Add custom x-axis labels for groups
            if !config.x_column.is_empty() {
                for (i, series) in data.series.iter().enumerate() {
                    plot_ui.text(
                        egui_plot::Text::new(
                            egui_plot::PlotPoint::new(i as f64, plot_ui.plot_bounds().min()[1] - 0.05 * plot_ui.plot_bounds().height()),
                            &series.name
                        )
                        .color(series.color)
                        .anchor(egui::Align2::CENTER_TOP)
                    );
                }
            }
        });
        
        // Show statistics if available
        if !stats_map.is_empty() {
            ui.horizontal(|ui| {
                ui.label("Groups:");
                ui.label(format!("{}", stats_map.len()));
                
                if stats_map.len() == 1 {
                    if let Some(stats) = stats_map.values().next() {
                        ui.separator();
                        ui.label("Median:");
                        ui.label(format!("{:.3}", stats.median));
                        
                        ui.separator();
                        ui.label("IQR:");
                        ui.label(format!("{:.3}", stats.q3 - stats.q1));
                        
                        ui.separator();
                        ui.label(format!("Count: {}", stats.count));
                    }
                }
            });
        }
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration) {
        if !data.series.is_empty() {
            ui.group(|ui| {
                ui.label(RichText::new("Groups:").strong());
                ui.separator();
                
                for series in &data.series {
                    if series.visible {
                        ui.horizontal(|ui| {
                            ui.colored_label(series.color, "■");
                            ui.label(&series.name);
                        });
                    }
                }
                
                // Add legend for special elements
                ui.separator();
                ui.label(RichText::new("Elements:").strong());
                ui.horizontal(|ui| {
                    ui.colored_label(Color32::from_rgb(50, 200, 50), "◆");
                    ui.label("Mean");
                });
                ui.horizontal(|ui| {
                    ui.colored_label(Color32::from_rgb(255, 100, 100), "●");
                    ui.label("Outliers");
                });
            });
        }
    }
    
    fn handle_interaction(&self, _ui: &mut Ui, _data: &PlotData, _config: &PlotConfiguration) -> Option<PlotInteraction> {
        None
    }
}

// Extension trait for PlotSpecificConfig
trait AsBoxPlot {
    fn as_box_plot(&self) -> &BoxPlotConfig;
}

impl AsBoxPlot for PlotSpecificConfig {
    fn as_box_plot(&self) -> &BoxPlotConfig {
        match self {
            PlotSpecificConfig::BoxPlot(cfg) => cfg,
            _ => panic!("Expected BoxPlotConfig"),
        }
    }
}