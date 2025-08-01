use egui::{Ui, Color32, RichText};
use egui_plot::{Plot, PlotPoints, Points, Legend, PlotUi, Polygon, MarkerShape as EguiMarkerShape};
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;
use crate::core::QueryResult;

use super::{
    Plot as PlotTrait, 
    PlotData, 
    PlotConfiguration, 
    PlotSpecificConfig, 
    ScatterPlotConfig, 
    MarkerShape,
    LineStyle,
    PlotInteraction,
    DataSeries,
    SeriesStyle,
    PlotMetadata,
    DataStatistics,
    // Enhanced utilities
    categorical_color, viridis_color, plasma_color, diverging_color,
    calculate_statistics, extract_numeric_values, extract_string_values,
    get_categorical_colors
};

pub struct ScatterPlotImpl;

impl ScatterPlotImpl {
    /// Create selection rectangle for interactive selection
    fn create_selection_rectangle(&self, start: [f64; 2], end: [f64; 2]) -> Polygon {
        let points = vec![
            [start[0], start[1]],
            [end[0], start[1]],
            [end[0], end[1]],
            [start[0], end[1]],
        ];
        
        Polygon::new(PlotPoints::from(points))
            .fill_color(Color32::from_rgba_unmultiplied(100, 150, 255, 30))
            .stroke(egui::Stroke::new(1.0, Color32::from_rgba_unmultiplied(100, 150, 255, 150)))
    }

    /// Enhanced data processing with frog-viz patterns
    fn process_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<DataSeries>, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for scatter plot".to_string());
        }

        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column)
            .ok_or_else(|| format!("X column '{}' not found", config.x_column))?;
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or_else(|| format!("Y column '{}' not found", config.y_column))?;

        let color_idx = if let Some(color_col) = &config.color_column {
            if !color_col.is_empty() {
                query_result.columns.iter().position(|c| c == color_col)
            } else {
                None
            }
        } else {
            None
        };

        let size_idx = if let Some(size_col) = &config.size_column {
            if !size_col.is_empty() {
                query_result.columns.iter().position(|c| c == size_col)
            } else {
                None
            }
        } else {
            None
        };

        let mut points = Vec::new();
        let mut color_map = HashMap::new();
        let mut color_index = 0;

        // Enhanced data processing with professional color mapping
        for (_row_idx, row) in query_result.rows.iter().enumerate() {
            if row.len() > x_idx && row.len() > y_idx {
                let x_val = row[x_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse X value '{}' as number", row[x_idx]))?;
                let y_val = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse Y value '{}' as number", row[y_idx]))?;

                // Professional color mapping based on frog-viz patterns
                let point_color = if let Some(color_idx) = color_idx {
                    if row.len() > color_idx {
                        let color_value = &row[color_idx];
                        if let Some(&existing_color) = color_map.get(color_value) {
                            Some(existing_color)
                        } else {
                            let new_color = categorical_color(color_index);
                            color_map.insert(color_value.clone(), new_color);
                            color_index += 1;
                            Some(new_color)
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Size mapping with proper scaling
                let point_size = if let Some(size_idx) = size_idx {
                    if row.len() > size_idx {
                        row[size_idx].parse::<f32>().unwrap_or(config.marker_size)
                    } else {
                        config.marker_size
                    }
                } else {
                    config.marker_size
                };

                // Enhanced tooltip data with rich information
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("X".to_string(), row[x_idx].clone());
                tooltip_data.insert("Y".to_string(), row[y_idx].clone());
                
                if let Some(color_idx) = color_idx {
                    if row.len() > color_idx {
                        tooltip_data.insert(config.color_column.as_ref().unwrap().clone(), row[color_idx].clone());
                    }
                }

                if let Some(size_idx) = size_idx {
                    if row.len() > size_idx {
                        tooltip_data.insert(config.size_column.as_ref().unwrap().clone(), row[size_idx].clone());
                    }
                }

                points.push(super::PlotPoint {
                    x: x_val,
                    y: y_val,
                    z: None,
                    label: None,
                    color: point_color,
                    size: Some(point_size),
                    series_id: None,
                    tooltip_data,
                });
            }
        }

        // Create series based on color grouping
        let mut series = Vec::new();
        if let Some(color_col) = &config.color_column {
            if !color_col.is_empty() {
                // Group by color column
                let mut grouped_data: HashMap<String, Vec<super::PlotPoint>> = HashMap::new();
                
                for point in points {
                    let color_value = point.tooltip_data.get(color_col)
                        .unwrap_or(&"default".to_string())
                        .clone();
                    grouped_data.entry(color_value).or_insert_with(Vec::new).push(point);
                }

                // Create series for each color group
                for (i, (group_name, group_points)) in grouped_data.into_iter().enumerate() {
                    let color = categorical_color(i);
                    series.push(DataSeries {
                        id: group_name.clone(),
                        name: group_name,
                        points: group_points,
                        color,
                        visible: true,
                        style: SeriesStyle::Points { size: config.marker_size, shape: MarkerShape::Circle },
                    });
                }
            } else {
                // Single series
                series.push(DataSeries {
                    id: "main".to_string(),
                    name: "Scatter".to_string(),
                    points,
                    color: categorical_color(0),
                    visible: true,
                    style: SeriesStyle::Points { size: config.marker_size, shape: MarkerShape::Circle },
                });
            }
        } else {
            // Single series
            series.push(DataSeries {
                id: "main".to_string(),
                name: "Scatter".to_string(),
                points,
                color: categorical_color(0),
                visible: true,
                style: SeriesStyle::Points { size: config.marker_size, shape: MarkerShape::Circle },
            });
        }

        Ok(series)
    }

    /// Convert marker shape to egui marker shape
    fn to_egui_marker_shape(shape: &MarkerShape) -> EguiMarkerShape {
        match shape {
            MarkerShape::Circle => EguiMarkerShape::Circle,
            MarkerShape::Square => EguiMarkerShape::Square,
            MarkerShape::Diamond => EguiMarkerShape::Diamond,
            MarkerShape::Triangle => EguiMarkerShape::Up,
            MarkerShape::Cross => EguiMarkerShape::Cross,
            MarkerShape::Plus => EguiMarkerShape::Plus,
            MarkerShape::Star => EguiMarkerShape::Asterisk,
        }
    }

    /// Enhanced tooltip handling with better positioning and information
    fn handle_tooltips(&self, ui: &mut Ui, plot_ui: &PlotUi, data: &PlotData) {
        if let Some(pointer_coord) = plot_ui.pointer_coordinate() {
            let mut closest_point = None;
            let mut min_distance = f64::MAX;
            
            // Find the closest point to the cursor
            for series in &data.series {
                for point in &series.points {
                    let dx = point.x - pointer_coord.x;
                    let dy = point.y - pointer_coord.y;
                    let distance = (dx * dx + dy * dy).sqrt();
                    
                    if distance < min_distance && distance < 0.1 { // Threshold for detection
                        min_distance = distance;
                        closest_point = Some((point, series));
                    }
                }
            }
            
            // Show tooltip for the closest point
            if let Some((point, series)) = closest_point {
                let mut tooltip_text = String::new();
                tooltip_text.push_str(&format!("Series: {}\n", series.name));
                tooltip_text.push_str(&format!("X: {:.3}\n", point.x));
                tooltip_text.push_str(&format!("Y: {:.3}", point.y));
                
                // Add additional tooltip data if available
                for (key, value) in &point.tooltip_data {
                    if key != "X" && key != "Y" {
                        tooltip_text.push_str(&format!("\n{}: {}", key, value));
                    }
                }
                
                // Show tooltip at pointer position
                egui::show_tooltip_at_pointer(
                    ui.ctx(),
                    egui::LayerId::new(egui::Order::Tooltip, egui::Id::new("scatter_tooltip")),
                    egui::Id::new("scatter_tooltip"),
                    |ui: &mut egui::Ui| {
                        ui.label(RichText::new(tooltip_text).monospace());
                    }
                );
            }
        }
    }
}

pub struct ScatterPlot;

impl PlotTrait for ScatterPlot {
    fn name(&self) -> &'static str {
        "Scatter Plot"
    }

    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![
            DataType::Float64, DataType::Float32,
            DataType::Int64, DataType::Int32, DataType::Int16, DataType::Int8,
            DataType::UInt64, DataType::UInt32, DataType::UInt16, DataType::UInt8,
        ])
    }

    fn required_y_types(&self) -> Vec<DataType> {
        vec![
            DataType::Float64, DataType::Float32,
            DataType::Int64, DataType::Int32, DataType::Int16, DataType::Int8,
            DataType::UInt64, DataType::UInt32, DataType::UInt16, DataType::UInt8,
        ]
    }

    fn supports_color_mapping(&self) -> bool {
        true
    }

    fn supports_size_mapping(&self) -> bool {
        true
    }

    fn get_default_config(&self) -> PlotConfiguration {
        PlotConfiguration {
            title: "Scatter Plot".to_string(),
            x_column: String::new(),
            y_column: String::new(),
            color_column: None,
            size_column: None,
            group_column: None,
            show_legend: true,
            show_grid: true,
            show_axes_labels: true,
            color_scheme: super::ColorScheme::Viridis,
            marker_size: 3.0,
            line_width: 2.0,
            allow_zoom: true,
            allow_pan: true,
            allow_selection: true,
            show_tooltips: true,
            plot_specific: PlotSpecificConfig::ScatterPlot(ScatterPlotConfig::default()),
        }
    }

    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        let series = ScatterPlotImpl.process_data(query_result, config)?;
        
        // Calculate statistics for the data
        let statistics = if !series.is_empty() && !series[0].points.is_empty() {
            let x_values: Vec<f64> = series.iter()
                .flat_map(|s| s.points.iter().map(|p| p.x))
                .collect();
            let y_values: Vec<f64> = series.iter()
                .flat_map(|s| s.points.iter().map(|p| p.y))
                .collect();
            
            if !x_values.is_empty() && !y_values.is_empty() {
                let x_stats = calculate_statistics(&x_values);
                let y_stats = calculate_statistics(&y_values);
                
                // Calculate correlation between X and Y
                let correlation = if x_values.len() == y_values.len() {
                    let n = x_values.len() as f64;
                    let sum_x: f64 = x_values.iter().sum();
                    let sum_y: f64 = y_values.iter().sum();
                    let sum_xy: f64 = x_values.iter().zip(y_values.iter()).map(|(x, y)| x * y).sum();
                    let sum_x2: f64 = x_values.iter().map(|x| x * x).sum();
                    let sum_y2: f64 = y_values.iter().map(|y| y * y).sum();

                    let numerator = n * sum_xy - sum_x * sum_y;
                    let denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)).sqrt();

                    if denominator != 0.0 {
                        Some(numerator / denominator)
                    } else {
                        None
                    }
                } else {
                    None
                };

                Some(DataStatistics {
                    mean_x: x_stats.mean,
                    mean_y: y_stats.mean,
                    std_x: x_stats.std_dev,
                    std_y: y_stats.std_dev,
                    correlation,
                    count: x_values.len(),
                })
            } else {
                None
            }
        } else {
            None
        };
        
        let all_points: Vec<super::PlotPoint> = series.iter()
            .flat_map(|s| s.points.clone())
            .collect();
        
        Ok(PlotData {
            points: all_points,
            series,
            metadata: PlotMetadata {
                title: config.title.clone(),
                x_label: config.x_column.clone(),
                y_label: config.y_column.clone(),
                show_legend: config.show_legend,
                show_grid: config.show_grid,
                color_scheme: config.color_scheme.clone(),
                extra_data: None,
            },
            statistics,
        })
    }

    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        let scatter_config = if let PlotSpecificConfig::ScatterPlot(cfg) = &config.plot_specific {
            cfg
        } else {
            return;
        };

        // Performance optimization: limit points for large datasets
        let max_points = 100000; // Higher limit for scatter plots
        let mut total_points = 0;
        for series in &data.series {
            total_points += series.points.len();
        }
        
        if total_points > max_points {
            ui.colored_label(egui::Color32::YELLOW, 
                format!("⚠ Large dataset detected ({} points). Consider filtering data for better performance.", total_points));
        }

        // Create plot with proper configuration
        let mut plot = Plot::new("scatter_plot")
            .allow_zoom(config.allow_zoom)
            .allow_drag(config.allow_pan)
            .show_grid(config.show_grid)
            .legend(Legend::default().position(egui_plot::Corner::RightBottom));

        // Add axis labels if enabled
        if config.show_axes_labels {
            plot = plot
                .x_axis_label(config.x_column.clone())
                .y_axis_label(config.y_column.clone());
        }

        // Track hover state for highlighting
        let mut hovered_point: Option<(usize, usize)> = None; // (series_idx, point_idx)
        
        // Find the closest point to the pointer for precise hover detection
        let mut closest_point: Option<(usize, usize, f64)> = None; // (series_idx, point_idx, distance)
        
        plot.show(ui, |plot_ui| {
            // Find the closest point to the pointer for precise hover detection
            closest_point = None; // Reset for this frame
            
            for (series_idx, series) in data.series.iter().enumerate() {
                if !series.visible {
                    continue;
                }

                for (point_idx, point) in series.points.iter().enumerate() {
                    if let Some(pointer_coord) = plot_ui.pointer_coordinate() {
                        let distance = ((pointer_coord.x - point.x).powi(2) + 
                                     (pointer_coord.y - point.y).powi(2)).sqrt();
                        let point_size = point.size.unwrap_or(config.marker_size);
                        let hover_threshold = (point_size * 1.2) as f64; // More precise hover radius
                        
                        if distance < hover_threshold {
                            // Update closest point if this one is closer
                            if let Some((_, _, current_distance)) = closest_point {
                                if distance < current_distance {
                                    closest_point = Some((series_idx, point_idx, distance));
                                }
                            } else {
                                closest_point = Some((series_idx, point_idx, distance));
                            }
                        }
                    }
                }
            }
            
            // Now render all points with proper highlighting
            for (series_idx, series) in data.series.iter().enumerate() {
                if !series.visible {
                    continue;
                }

                for (point_idx, point) in series.points.iter().enumerate() {
                    // Check if this is the closest hovered point
                    let is_hovered = if let Some((hovered_series, hovered_point, _)) = closest_point {
                        series_idx == hovered_series && point_idx == hovered_point
                    } else {
                        false
                    };
                    
                    let marker_shape = ScatterPlotImpl::to_egui_marker_shape(&scatter_config.point_shape);
                    let point_size = point.size.unwrap_or(config.marker_size);
                    let alpha = 1.0;
                    
                    // Use point-specific color if available, otherwise fall back to series color
                    let point_color = point.color.unwrap_or(series.color).linear_multiply(alpha);
                    
                    // Add highlighting effect without changing base color
                    if is_hovered {
                        // Add a subtle border for highlighting
                        let highlighted_points = Points::new(PlotPoints::from(vec![[point.x, point.y]]))
                            .color(egui::Color32::WHITE)
                            .radius(point_size * 1.8)
                            .shape(marker_shape);
                        plot_ui.points(highlighted_points);
                    }
                    
                    let points = Points::new(PlotPoints::from(vec![[point.x, point.y]]))
                        .color(point_color)
                        .radius(if is_hovered { point_size * 1.5 } else { point_size })
                        .shape(marker_shape);
                    plot_ui.points(points);
                }
            }
        });
        
        // Handle tooltips outside the closure
        if config.show_tooltips {
            if let Some((series_idx, point_idx, _)) = closest_point {
                if let Some(series) = data.series.get(series_idx) {
                    if let Some(point) = series.points.get(point_idx) {
                        // Create comprehensive tooltip
                        let mut tooltip_text = String::new();
                        tooltip_text.push_str(&format!("Series: {}\n", series.name));
                        tooltip_text.push_str(&format!("X: {:.3}\n", point.x));
                        tooltip_text.push_str(&format!("Y: {:.3}\n", point.y));
                        
                        if let Some(size) = point.size {
                            tooltip_text.push_str(&format!("Size: {:.1}\n", size));
                        }
                        
                        // Add additional tooltip data
                        for (key, value) in &point.tooltip_data {
                            if key != "X" && key != "Y" && key != "Series" && key != "Size" {
                                tooltip_text.push_str(&format!("{}: {}\n", key, value));
                            }
                        }
                        
                        // Show tooltip at pointer position
                        if let Some(_pointer_pos) = ui.input(|i| i.pointer.hover_pos()) {
                            egui::show_tooltip_at_pointer(ui.ctx(), egui::LayerId::new(egui::Order::Tooltip, egui::Id::new("scatter_tooltip")), egui::Id::new("scatter_tooltip"), |ui| {
                                ui.label(tooltip_text);
                            });
                        }
                    }
                }
            }
        }
    }

    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() && config.show_legend {
            ui.group(|ui| {
                // Show plot title
                if !config.title.is_empty() {
                    ui.label(RichText::new(&config.title).strong().size(16.0));
                    ui.separator();
                }
                
                // Show dataset info
                let total_points: usize = data.series.iter().map(|s| s.points.len()).sum();
                ui.label(RichText::new(format!("Dataset: {} points", total_points)).italics());
                
                // Performance warning for large datasets
                if total_points > 100000 {
                    ui.colored_label(egui::Color32::YELLOW, 
                        "⚠ Large dataset - consider filtering for better performance");
                }
                
                ui.separator();
                ui.label(RichText::new("Series:").strong());
                ui.separator();
                
                // Sort series by name for consistent display
                let mut sorted_series: Vec<_> = data.series.iter().collect();
                sorted_series.sort_by(|a, b| a.name.cmp(&b.name));
                
                for series in &sorted_series {
                    let mut is_visible = series.visible;
                    if ui.checkbox(&mut is_visible, &series.name).changed() {
                        // Note: This would require mutable access to data
                    }
                    
                    // Show series style indicator with enhanced styling
                    ui.horizontal(|ui| {
                        match &series.style {
                            SeriesStyle::Points { size: _, shape } => {
                                let shape_text = match shape {
                                    MarkerShape::Circle => "●",
                                    MarkerShape::Square => "■",
                                    MarkerShape::Diamond => "◆",
                                    MarkerShape::Triangle => "▲",
                                    MarkerShape::Cross => "✚",
                                    MarkerShape::Plus => "➕",
                                    MarkerShape::Star => "★",
                                };
                                ui.colored_label(series.color, shape_text);
                            },
                            SeriesStyle::Lines { width: _, style } => {
                                let style_text = match style {
                                    LineStyle::Solid => "———",
                                    LineStyle::Dashed => "---",
                                    LineStyle::Dotted => "...",
                                    LineStyle::DashDot => "-.-.",
                                };
                                ui.colored_label(series.color, style_text);
                            },
                            SeriesStyle::Bars { width: _ } => {
                                ui.colored_label(series.color, "■");
                            },
                            SeriesStyle::Area { fill: _ } => {
                                ui.colored_label(series.color, "▬");
                            },
                        }
                        
                        if !is_visible {
                            ui.label(RichText::new(&series.name).strikethrough());
                        } else {
                            ui.label(&series.name);
                        }
                        
                        // Show point count for this series
                        ui.label(RichText::new(format!("({} points)", series.points.len())).weak());
                    });
                }
                
                // Show configuration details
                ui.separator();
                ui.label(RichText::new("Configuration:").strong());
                ui.horizontal(|ui| {
                    ui.label("Marker Size:");
                    ui.label(format!("{:.1}", config.marker_size));
                });
                // Get scatter plot specific config
                let default_config;
                let scatter_config = if let PlotSpecificConfig::ScatterPlot(cfg) = &config.plot_specific {
                    cfg
                } else {
                    default_config = self.get_default_config();
                    default_config.plot_specific.as_scatter_plot()
                };
                
                ui.horizontal(|ui| {
                    ui.label("Point Shape:");
                    ui.label(format!("{:?}", scatter_config.point_shape));
                });
                
                // Show enhanced statistics if available
                if let Some(stats) = &data.statistics {
                    ui.separator();
                    ui.label(RichText::new("Statistics:").strong());
                    ui.horizontal(|ui| {
                        ui.label("Count:");
                        ui.label(format!("{}", stats.count));
                    });
                    ui.horizontal(|ui| {
                        ui.label("Mean X:");
                        ui.label(format!("{:.3}", stats.mean_x));
                    });
                    ui.horizontal(|ui| {
                        ui.label("Mean Y:");
                        ui.label(format!("{:.3}", stats.mean_y));
                    });
                    ui.horizontal(|ui| {
                        ui.label("Std Dev X:");
                        ui.label(format!("{:.3}", stats.std_x));
                    });
                    ui.horizontal(|ui| {
                        ui.label("Std Dev Y:");
                        ui.label(format!("{:.3}", stats.std_y));
                    });
                    if let Some(corr) = stats.correlation {
                        ui.horizontal(|ui| {
                            ui.label("Correlation:");
                            ui.label(format!("{:.3}", corr));
                        });
                    }
                }
            });
        }
    }

    fn handle_interaction(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) -> Option<PlotInteraction> {
        // Enhanced interaction handling
        if config.allow_selection {
            // Handle point selection
            // Handle area selection
            // Handle series toggling
        }
        
        None
    }
}