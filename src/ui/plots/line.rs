use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Line, Plot, PlotPoints, Legend, PlotUi, Points, MarkerShape as EguiMarkerShape, 
                PlotBounds, Polygon, PlotPoint as EguiPlotPoint, LineStyle as EguiLineStyle};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use std::collections::HashMap;
use crate::core::QueryResult;

use super::{
    Plot as PlotTrait, 
    PlotData, 
    PlotConfiguration, 
    PlotSpecificConfig, 
    LineChartConfig, 
    LineStyle,
    PlotInteraction,
    DataSeries,
    SeriesStyle,
    MarkerShape,
    PlotMetadata,
    DataStatistics,
    data_processor::DataProcessor,
    // Enhanced utilities
    categorical_color, viridis_color, plasma_color, diverging_color,
    calculate_statistics, extract_numeric_values, extract_temporal_values,
    get_categorical_colors
};

#[derive(Clone)]
pub struct LineChartPlot;

impl LineChartPlot {
    /// Enhanced data processing based on frog-viz patterns
    fn extract_temporal_points(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<super::PlotPoint>, String> {
        if config.y_column.is_empty() {
            return Err("Y column not selected".to_string());
        }

        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or("Y column not found")?;
        
        let x_idx = if !config.x_column.is_empty() {
            query_result.columns.iter().position(|c| c == &config.x_column)
                .ok_or_else(|| format!("X column '{}' not found", config.x_column))?
        } else {
            return Err("X column is required for temporal data".to_string());
        };

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
        let mut color_map = HashMap::new();
        let mut color_index = 0;
        
        // Enhanced temporal data handling based on frog-viz patterns
        for (_row_idx, row) in query_result.rows.iter().enumerate() {
            if row.len() > y_idx && row.len() > x_idx {
                let y_val = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse Y value '{}' as number", row[y_idx]))?;
                
                // Enhanced temporal value parsing with proper error handling
                let x_val = match &query_result.column_types[x_idx] {
                    DataType::Date32 => {
                        let days = row[x_idx].parse::<i32>()
                            .map_err(|_| format!("Failed to parse Date32 value '{}'", row[x_idx]))?;
                        (days as f64) * 86400000.0 // days to ms
                    },
                    DataType::Date64 => {
                        row[x_idx].parse::<f64>()
                            .map_err(|_| format!("Failed to parse Date64 value '{}'", row[x_idx]))?
                    },
                    DataType::Timestamp(time_unit, _) => {
                        let timestamp = row[x_idx].parse::<i64>()
                            .map_err(|_| format!("Failed to parse Timestamp value '{}'", row[x_idx]))?;
                        
                        match time_unit {
                            TimeUnit::Second => (timestamp as f64) * 1000.0,
                            TimeUnit::Millisecond => timestamp as f64,
                            TimeUnit::Microsecond => (timestamp as f64) / 1000.0,
                            TimeUnit::Nanosecond => (timestamp as f64) / 1_000_000.0,
                        }
                    },
                    DataType::Time32(time_unit) => {
                        let time_val = row[x_idx].parse::<i32>()
                            .map_err(|_| format!("Failed to parse Time32 value '{}'", row[x_idx]))?;
                        
                        match time_unit {
                            TimeUnit::Second => (time_val as f64) * 1000.0,
                            TimeUnit::Millisecond => time_val as f64,
                            _ => time_val as f64,
                        }
                    },
                    DataType::Time64(time_unit) => {
                        let time_val = row[x_idx].parse::<i64>()
                            .map_err(|_| format!("Failed to parse Time64 value '{}'", row[x_idx]))?;
                        
                        match time_unit {
                            TimeUnit::Microsecond => (time_val as f64) / 1000.0,
                            TimeUnit::Nanosecond => (time_val as f64) / 1_000_000.0,
                            _ => time_val as f64,
                        }
                    },
                    _ => {
                        row[x_idx].parse::<f64>()
                            .map_err(|_| format!("Failed to parse X value '{}' as number", row[x_idx]))?
                    }
                };

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

                // Enhanced tooltip data with rich information
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("X".to_string(), row[x_idx].clone());
                tooltip_data.insert("Y".to_string(), row[y_idx].clone());
                
                if let Some(color_idx) = color_idx {
                    if row.len() > color_idx {
                        tooltip_data.insert(config.color_column.as_ref().unwrap().clone(), row[color_idx].clone());
                    }
                }

                points.push(super::PlotPoint {
                    x: x_val,
                    y: y_val,
                    z: None,
                    label: None,
                    color: point_color,
                    size: None,
                    series_id: None,
                    tooltip_data,
                });
            }
        }
        
        Ok(points)
    }
    
    /// Enhanced missing data handling based on frog-viz patterns
    fn handle_missing_data(&self, series: &mut Vec<DataSeries>, config: &PlotConfiguration) {
        let _line_config = if let PlotSpecificConfig::LineChart(cfg) = &config.plot_specific {
            cfg
        } else {
            return;
        };
        
        // For each series, check for large gaps that might indicate missing data
        for series in series.iter_mut() {
            if series.points.len() < 2 {
                continue;
            }
            
            // Sort points by X value
            series.points.sort_by(|a, b| a.x.partial_cmp(&b.x).unwrap_or(std::cmp::Ordering::Equal));
            
            // Calculate average X distance between consecutive points
            let mut total_distance = 0.0;
            let mut count = 0;
            
            for i in 1..series.points.len() {
                let distance = (series.points[i].x - series.points[i-1].x).abs();
                total_distance += distance;
                count += 1;
            }
            
            if count == 0 {
                continue;
            }
            
            let avg_distance = total_distance / count as f64;
            let gap_threshold = avg_distance * 3.0; // Consider a gap if distance is 3x the average
            
            // Mark gaps by setting z value to indicate a discontinuity
            for i in 1..series.points.len() {
                let distance = (series.points[i].x - series.points[i-1].x).abs();
                if distance > gap_threshold {
                    // Mark this as a gap by setting z to a special value
                    series.points[i].z = Some(-1.0); // Special value to indicate gap
                }
            }
        }
    }
    
    /// Create fill area for line charts
    fn create_fill_area(&self, series: &DataSeries, plot_ui: &mut PlotUi, bounds: &PlotBounds) {
        if series.points.len() < 2 {
            return;
        }
        
        // Performance optimization: Only create fill area if we have valid bounds
        if bounds.min()[1] >= bounds.max()[1] {
            return;
        }
        
        // Create polygon points for fill area with performance optimizations
        let mut polygon_points = Vec::with_capacity(series.points.len() * 2 + 2);
        
        // Add points from the line (top of fill area)
        for point in &series.points {
            if point.z != Some(-1.0) { // Skip gap points
                polygon_points.push([point.x, point.y]);
            }
        }
        
        // Early exit if we don't have enough points
        if polygon_points.len() < 2 {
            return;
        }
        
        // Add points from the bottom of the plot to close the polygon
        // Go in reverse order to create a proper closed polygon
        for point in series.points.iter().rev() {
            if point.z != Some(-1.0) { // Skip gap points
                polygon_points.push([point.x, bounds.min()[1]]);
            }
        }
        
        // Only create polygon if we have enough points and the area is reasonable
        if polygon_points.len() >= 3 {
            // Use a more efficient semi-transparent color
            let fill_color = series.color.linear_multiply(0.2); // More transparent for better performance
            
            // Create polygon with optimized rendering
            let polygon = Polygon::new(PlotPoints::from(polygon_points))
                .fill_color(fill_color)
                .stroke(Stroke::new(0.0, Color32::TRANSPARENT)); // No stroke for better performance
            
            plot_ui.polygon(polygon);
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
                    egui::LayerId::new(egui::Order::Tooltip, egui::Id::new("line_tooltip")),
                    egui::Id::new("line_tooltip"),
                    |ui: &mut egui::Ui| {
                        ui.label(RichText::new(tooltip_text).monospace());
                    }
                );
            }
        }
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
    
    /// Enhanced data processing with professional series grouping
    fn process_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<DataSeries>, String> {
        let data_processor = DataProcessor::new();
        
        // Performance optimization: Pre-allocate for large datasets
        let total_rows = query_result.rows.len();
        let estimated_series_count = if config.color_column.is_some() { 10 } else { 1 };
        
        // Debug: Print input data info
        println!("🔍 Line chart process_data debug:");
        println!("  Query result: {} columns, {} rows", query_result.columns.len(), total_rows);
        println!("  Config: X='{}', Y='{}'", config.x_column, config.y_column);
        
        // Extract points with temporal support and performance optimizations
        let points = if total_rows > 100_000 {
            self.extract_temporal_points_optimized(query_result, config, total_rows)?
        } else {
            self.extract_temporal_points(query_result, config)?
        };
        
        println!("  Extracted {} points", points.len());
        if !points.is_empty() {
            println!("    First point: ({}, {})", points[0].x, points[0].y);
            println!("    Last point: ({}, {})", points[points.len()-1].x, points[points.len()-1].y);
        }
        
        if points.is_empty() {
            return Err("No valid data points found".to_string());
        }
        
        // Performance optimization: Use sampling for very large datasets
        let processed_points = if total_rows > 500_000 {
            self.sample_points_for_performance(&points, total_rows)
        } else {
            points
        };
        
        // Group points by series if color column is specified
        let mut series = Vec::with_capacity(estimated_series_count);
        
        // For line charts, we want to create a single series unless explicitly grouping
        // Check if the color column is actually meant for grouping (not just a timestamp)
        let should_group_by_color = if let Some(color_col) = &config.color_column {
            if !color_col.is_empty() {
                // Check if the color column has many unique values (indicating it's for grouping)
                // vs few unique values (indicating it's just a timestamp or ID)
                let unique_values: std::collections::HashSet<String> = processed_points.iter()
                    .filter_map(|p| p.tooltip_data.get(color_col))
                    .cloned()
                    .collect();
                
                // If we have many unique values (more than 10% of points), treat as grouping
                // Otherwise, treat as a single series
                unique_values.len() > processed_points.len() / 10
            } else {
                false
            }
        } else {
            false
        };
        
        if should_group_by_color {
            // Group by color column with pre-allocated HashMap
            let mut grouped_data: HashMap<String, Vec<super::PlotPoint>> = HashMap::with_capacity(estimated_series_count);
            
            for point in processed_points {
                let series_name = point.tooltip_data.get(config.color_column.as_ref().unwrap())
                    .unwrap_or(&"default".to_string())
                    .clone();
                grouped_data.entry(series_name).or_insert_with(Vec::new).push(point);
            }
            
            println!("  Grouped into {} series by color column '{}'", grouped_data.len(), config.color_column.as_ref().unwrap());
            
            // Create series for each group with stable color mapping
            let mut sorted_groups: Vec<_> = grouped_data.into_iter().collect();
            sorted_groups.sort_by(|a, b| a.0.cmp(&b.0)); // Sort by series name for consistent colors
            
            for (i, (series_name, series_points)) in sorted_groups.into_iter().enumerate() {
                if series_points.is_empty() {
                    continue;
                }
                
                // Sort points by X value for proper line drawing
                let mut sorted_points = series_points;
                sorted_points.sort_by(|a, b| a.x.partial_cmp(&b.x).unwrap_or(std::cmp::Ordering::Equal));
                
                // Use stable color mapping based on series name hash
                let color = self.get_stable_color(&series_name);
                let mut data_series = DataSeries {
                    id: series_name.clone(),
                    name: series_name,
                    points: sorted_points,
                    color,
                    visible: true,
                    style: SeriesStyle::Lines { width: config.line_width, style: LineStyle::Solid },
                };
                
                // Handle missing data
                self.handle_missing_data(&mut vec![data_series.clone()], config);
                
                series.push(data_series);
            }
        } else {
            // Single series - ignore color column for basic line charts
            let mut sorted_points = processed_points;
            sorted_points.sort_by(|a, b| a.x.partial_cmp(&b.x).unwrap_or(std::cmp::Ordering::Equal));
            
            let mut data_series = DataSeries {
                id: "main".to_string(),
                name: "Line".to_string(),
                points: sorted_points,
                color: categorical_color(0),
                visible: true,
                style: SeriesStyle::Lines { width: config.line_width, style: LineStyle::Solid },
            };
            
            self.handle_missing_data(&mut vec![data_series.clone()], config);
            series.push(data_series);
        }
        
        println!("  Created {} series", series.len());
        for (i, s) in series.iter().enumerate() {
            println!("    Series {}: {} points", i, s.points.len());
            if !s.points.is_empty() {
                println!("      X range: {:.2} to {:.2}", s.points[0].x, s.points[s.points.len()-1].x);
                println!("      Y range: {:.2} to {:.2}", s.points[0].y, s.points[s.points.len()-1].y);
            }
        }
        
        Ok(series)
    }
    
    /// Get line pattern for different line styles
    fn get_line_pattern(&self, style: &LineStyle) -> &'static [f32] {
        match style {
            LineStyle::Solid => &[],
            LineStyle::Dashed => &[5.0, 5.0],
            LineStyle::Dotted => &[2.0, 2.0],
            LineStyle::DashDot => &[5.0, 2.0, 2.0, 2.0],
        }
    }
    
    /// Get stable color for a series name (consistent across renders)
    fn get_stable_color(&self, series_name: &str) -> Color32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        series_name.hash(&mut hasher);
        let hash = hasher.finish();
        
        // Use hash to get consistent color index
        let color_index = (hash % 20) as usize; // 20 colors available
        categorical_color(color_index)
    }
    
    /// Get shape by category value (consistent mapping)
    fn get_shape_by_category(&self, category: &str) -> EguiMarkerShape {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        category.hash(&mut hasher);
        let hash = hasher.finish();
        
        // Use hash to get consistent shape
        let shape_index = (hash % 7) as usize; // 7 shapes available
        match shape_index {
            0 => EguiMarkerShape::Circle,
            1 => EguiMarkerShape::Square,
            2 => EguiMarkerShape::Diamond,
            3 => EguiMarkerShape::Up,
            4 => EguiMarkerShape::Cross,
            5 => EguiMarkerShape::Plus,
            6 => EguiMarkerShape::Asterisk,
            _ => EguiMarkerShape::Circle,
        }
    }
    
    /// Optimized temporal points extraction for large datasets
    fn extract_temporal_points_optimized(&self, query_result: &QueryResult, config: &PlotConfiguration, total_rows: usize) -> Result<Vec<super::PlotPoint>, String> {
        if config.y_column.is_empty() {
            return Err("Y column not selected".to_string());
        }

        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or("Y column not found")?;
        
        let x_idx = if !config.x_column.is_empty() {
            query_result.columns.iter().position(|c| c == &config.x_column)
                .ok_or_else(|| format!("X column '{}' not found", config.x_column))?
        } else {
            return Err("X column is required for temporal data".to_string());
        };

        let color_idx = if let Some(color_col) = &config.color_column {
            if !color_col.is_empty() {
                query_result.columns.iter().position(|c| c == color_col)
            } else {
                None
            }
        } else {
            None
        };

        // Pre-allocate vectors for better performance
        let mut points = Vec::with_capacity(total_rows);
        let mut color_map = HashMap::with_capacity(100); // Pre-allocate color map
        let mut color_index = 0;
        
        // Use iterator for better performance
        for row in query_result.rows.iter() {
            if row.len() > y_idx && row.len() > x_idx {
                let y_val = row[y_idx].parse::<f64>()
                    .map_err(|_| format!("Failed to parse Y value '{}' as number", row[y_idx]))?;
                
                // Optimized temporal value parsing
                let x_val = match &query_result.column_types[x_idx] {
                    DataType::Date32 => {
                        let days = row[x_idx].parse::<i32>()
                            .map_err(|_| format!("Failed to parse Date32 value '{}'", row[x_idx]))?;
                        (days as f64) * 86400000.0
                    },
                    DataType::Date64 => {
                        row[x_idx].parse::<f64>()
                            .map_err(|_| format!("Failed to parse Date64 value '{}'", row[x_idx]))?
                    },
                    DataType::Timestamp(time_unit, _) => {
                        let timestamp = row[x_idx].parse::<i64>()
                            .map_err(|_| format!("Failed to parse Timestamp value '{}'", row[x_idx]))?;
                        
                        match time_unit {
                            TimeUnit::Second => (timestamp as f64) * 1000.0,
                            TimeUnit::Millisecond => timestamp as f64,
                            TimeUnit::Microsecond => (timestamp as f64) / 1000.0,
                            TimeUnit::Nanosecond => (timestamp as f64) / 1_000_000.0,
                        }
                    },
                    _ => {
                        row[x_idx].parse::<f64>()
                            .map_err(|_| format!("Failed to parse X value '{}' as number", row[x_idx]))?
                    }
                };

                // Optimized color mapping
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

                // Optimized tooltip data creation
                let mut tooltip_data = HashMap::with_capacity(3);
                tooltip_data.insert("X".to_string(), row[x_idx].clone());
                tooltip_data.insert("Y".to_string(), row[y_idx].clone());
                
                if let Some(color_idx) = color_idx {
                    if row.len() > color_idx {
                        tooltip_data.insert(config.color_column.as_ref().unwrap().clone(), row[color_idx].clone());
                    }
                }

                points.push(super::PlotPoint {
                    x: x_val,
                    y: y_val,
                    z: None,
                    label: None,
                    color: point_color,
                    size: None,
                    series_id: None,
                    tooltip_data,
                });
            }
        }
        
        Ok(points)
    }
    
    /// Sample points for performance with very large datasets
    fn sample_points_for_performance(&self, points: &[super::PlotPoint], total_rows: usize) -> Vec<super::PlotPoint> {
        if total_rows <= 100_000 {
            return points.to_vec();
        }
        
        // Use adaptive sampling based on dataset size
        let target_points = if total_rows > 1_000_000 {
            50_000 // Max 50K points for very large datasets
        } else if total_rows > 500_000 {
            25_000 // 25K points for large datasets
        } else {
            10_000 // 10K points for medium datasets
        };
        
        let step = total_rows / target_points;
        let mut sampled_points = Vec::with_capacity(target_points);
        
        for (i, point) in points.iter().enumerate() {
            if i % step == 0 || i == points.len() - 1 {
                sampled_points.push(point.clone());
            }
        }
        
        println!("📊 Sampled {} points from {} total points for performance", sampled_points.len(), total_rows);
        sampled_points
    }
}

impl PlotTrait for LineChartPlot {
    fn name(&self) -> &'static str {
        "Line Chart"
    }

    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![
            DataType::Float64, DataType::Float32,
            DataType::Int64, DataType::Int32, DataType::Int16, DataType::Int8,
            DataType::UInt64, DataType::UInt32, DataType::UInt16, DataType::UInt8,
            DataType::Date32, DataType::Date64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ])
    }

    fn required_y_types(&self) -> Vec<DataType> {
        vec![
            DataType::Float64, DataType::Float32,
            DataType::Int64, DataType::Int32, DataType::Int16, DataType::Int8,
            DataType::UInt64, DataType::UInt32, DataType::UInt16, DataType::UInt8,
        ]
    }

    fn supports_multiple_series(&self) -> bool {
        true
    }

    fn get_default_config(&self) -> PlotConfiguration {
        PlotConfiguration {
            title: "Line Chart".to_string(),
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
            plot_specific: PlotSpecificConfig::LineChart(LineChartConfig::default()),
        }
    }

    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        let series = self.process_data(query_result, config)?;
        
        // Calculate statistics for the data
        let statistics = if !series.is_empty() && !series[0].points.is_empty() {
            let y_values: Vec<f64> = series.iter()
                .flat_map(|s| s.points.iter().map(|p| p.y))
                .collect();
            
            if !y_values.is_empty() {
                let stats = calculate_statistics(&y_values);
                Some(DataStatistics {
                    mean_x: 0.0, // Would need X values to calculate
                    mean_y: stats.mean,
                    std_x: 0.0, // Would need X values to calculate
                    std_y: stats.std_dev,
                    correlation: None, // Would need both X and Y to calculate
                    count: stats.count,
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
        let line_config = if let PlotSpecificConfig::LineChart(cfg) = &config.plot_specific {
            cfg
        } else {
            return;
        };

        // Debug: Print rendering info
        println!("🔍 Line chart render debug:");
        println!("  Config: X='{}', Y='{}'", config.x_column, config.y_column);
        println!("  Series count: {}", data.series.len());
        for (i, series) in data.series.iter().enumerate() {
            println!("  Series {}: {} points, visible: {}", i, series.points.len(), series.visible);
            if !series.points.is_empty() {
                println!("    First point: ({}, {})", series.points[0].x, series.points[0].y);
                println!("    Last point: ({}, {})", series.points[series.points.len()-1].x, series.points[series.points.len()-1].y);
            }
        }

        // Performance optimization: limit points for large datasets
        let max_points = 50000; // Higher limit for line charts
        let mut total_points = 0;
        for series in &data.series {
            total_points += series.points.len();
        }
        
        if total_points > max_points {
            ui.colored_label(egui::Color32::YELLOW, 
                format!("⚠ Large dataset detected ({} points). Consider filtering data for better performance.", total_points));
        }
        
        // Performance warning for fill area with large datasets
        if line_config.fill_area && total_points > 10000 {
            ui.colored_label(egui::Color32::ORANGE, 
                "⚠ Fill area enabled with large dataset may impact performance. Consider disabling fill area for better responsiveness.");
        }

        // Create plot with proper configuration
        let mut plot = Plot::new("line_chart")
            .allow_zoom(config.allow_zoom)
            .allow_drag(config.allow_pan)
            .show_grid(config.show_grid)
            .legend(Legend::default().position(egui_plot::Corner::RightBottom))
            .auto_bounds_x()  // Auto-fit X bounds
            .auto_bounds_y(); // Auto-fit Y bounds

        // Add axis labels if enabled
        if config.show_axes_labels {
            plot = plot
                .x_axis_label(config.x_column.clone())
                .y_axis_label(config.y_column.clone());
        }

        // Performance optimization: Cache bounds to avoid recalculation
        let mut cached_bounds = None;
        
        // Track hover state for highlighting
        let mut hovered_point: Option<(usize, usize)> = None; // (series_idx, point_idx)
        let mut closest_distance = f64::INFINITY;
        
        plot.show(ui, |plot_ui| {
            // Cache plot bounds to avoid recalculation for fill areas
            if cached_bounds.is_none() {
                cached_bounds = Some(plot_ui.plot_bounds());
            }
            
            for (series_idx, series) in data.series.iter().enumerate() {
                if !series.visible {
                    continue;
                }

                // Create fill area first (behind the lines) if enabled
                if line_config.fill_area {
                    if let Some(ref bounds) = cached_bounds {
                        self.create_fill_area(series, plot_ui, bounds);
                    }
                }

                // Draw lines between points
                if series.points.len() > 1 {
                    let line_points: Vec<[f64; 2]> = series.points.iter()
                        .filter(|p| p.z != Some(-1.0)) // Skip gap points
                        .map(|p| [p.x, p.y])
                        .collect();

                    if line_points.len() >= 2 {
                        let mut line = Line::new(PlotPoints::from(line_points))
                            .color(series.color)
                            .width(config.line_width);

                        // Apply line style
                        if let SeriesStyle::Lines { style, .. } = &series.style {
                            let pattern = self.get_line_pattern(style);
                            if !pattern.is_empty() {
                                line = line.style(egui_plot::LineStyle::dashed_dense());
                            }
                        }

                        plot_ui.line(line);
                    }
                }

                // Draw points if enabled (only if not too many points for performance)
                if line_config.show_points && series.points.len() <= 1000 {
                    for (point_idx, point) in series.points.iter().enumerate() {
                        if point.z != Some(-1.0) { // Skip gap points
                            // Check if this point is being hovered
                            let is_hovered = if let Some(pointer_coord) = plot_ui.pointer_coordinate() {
                                let distance = ((pointer_coord.x - point.x).powi(2) + 
                                             (pointer_coord.y - point.y).powi(2)).sqrt();
                                
                                // Only consider points within hover radius
                                if distance < 12.0 {
                                    // Update closest point if this one is closer
                                    if distance < closest_distance {
                                        closest_distance = distance;
                                        hovered_point = Some((series_idx, point_idx));
                                    }
                                    // Check if this is the currently closest point
                                    Some((series_idx, point_idx)) == hovered_point
                                } else {
                                    false
                                }
                            } else {
                                false
                            };
                            
                            // Use original color for consistency with legend
                            let point_color = series.color;
                            
                            // Get shape for this point (support shape by category)
                            let shape = if let Some(shape_col) = &config.size_column {
                                // Use size column for shape mapping if specified
                                if let Some(shape_value) = point.tooltip_data.get(shape_col) {
                                    self.get_shape_by_category(shape_value)
                                } else {
                                    EguiMarkerShape::Circle
                                }
                            } else {
                                EguiMarkerShape::Circle
                            };
                            
                            let points = Points::new(PlotPoints::from(vec![[point.x, point.y]]))
                                .color(point_color)
                                .radius(if is_hovered { config.marker_size * 1.5 } else { config.marker_size })
                                .shape(shape);
                            
                            // Add highlighting effect without changing base color
                            if is_hovered {
                                // Add a subtle border for highlighting
                                let highlighted_points = Points::new(PlotPoints::from(vec![[point.x, point.y]]))
                                    .color(egui::Color32::WHITE)
                                    .radius(config.marker_size * 1.8)
                                    .shape(shape);
                                plot_ui.points(highlighted_points);
                            }
                            
                            plot_ui.points(points);
                        }
                    }
                }
            }
        });
        
        // Handle tooltips outside the closure
        if config.show_tooltips {
            if let Some((series_idx, point_idx)) = hovered_point {
                if let Some(series) = data.series.get(series_idx) {
                    if let Some(point) = series.points.get(point_idx) {
                        // Create comprehensive tooltip
                        let mut tooltip_text = String::new();
                        tooltip_text.push_str(&format!("Series: {}\n", series.name));
                        tooltip_text.push_str(&format!("X: {:.3}\n", point.x));
                        tooltip_text.push_str(&format!("Y: {:.3}\n", point.y));
                        
                        // Add additional tooltip data
                        for (key, value) in &point.tooltip_data {
                            if key != "X" && key != "Y" && key != "Series" {
                                tooltip_text.push_str(&format!("{}: {}\n", key, value));
                            }
                        }
                        
                        // Show tooltip at pointer position
                        if let Some(_pointer_pos) = ui.input(|i| i.pointer.hover_pos()) {
                            egui::show_tooltip_at_pointer(ui.ctx(), egui::LayerId::new(egui::Order::Tooltip, egui::Id::new("line_tooltip")), egui::Id::new("line_tooltip"), |ui| {
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
                if total_points > 50000 {
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
                
                // Show shape legend if shape by category is enabled
                if let Some(shape_col) = &config.size_column {
                    if !shape_col.is_empty() {
                        ui.separator();
                        ui.label(RichText::new("Shapes by Category:").strong());
                        
                        // Collect unique shapes and their categories
                        let mut shape_categories: std::collections::HashMap<String, EguiMarkerShape> = std::collections::HashMap::new();
                        for series in &data.series {
                            for point in &series.points {
                                if let Some(shape_value) = point.tooltip_data.get(shape_col) {
                                    let shape = self.get_shape_by_category(shape_value);
                                    shape_categories.insert(shape_value.clone(), shape);
                                }
                            }
                        }
                        
                        // Display shape legend
                        for (category, shape) in shape_categories.iter() {
                            ui.horizontal(|ui| {
                                let shape_text = match shape {
                                    EguiMarkerShape::Circle => "●",
                                    EguiMarkerShape::Square => "■",
                                    EguiMarkerShape::Diamond => "◆",
                                    EguiMarkerShape::Up => "▲",
                                    EguiMarkerShape::Cross => "✚",
                                    EguiMarkerShape::Plus => "➕",
                                    EguiMarkerShape::Asterisk => "★",
                                    _ => "●",
                                };
                                ui.label(shape_text);
                                ui.label(category);
                            });
                        }
                    }
                }
                
                // Show configuration details
                ui.separator();
                ui.label(RichText::new("Configuration:").strong());
                ui.horizontal(|ui| {
                    ui.label("Line Width:");
                    ui.label(format!("{:.1}", config.line_width));
                });
                ui.horizontal(|ui| {
                    ui.label("Marker Size:");
                    ui.label(format!("{:.1}", config.marker_size));
                });
                // Get line chart specific config
                let default_config;
                let line_config = if let PlotSpecificConfig::LineChart(cfg) = &config.plot_specific {
                    cfg
                } else {
                    default_config = self.get_default_config();
                    default_config.plot_specific.as_line_chart()
                };
                
                ui.horizontal(|ui| {
                    ui.label("Show Points:");
                    ui.label(if line_config.show_points { "Yes" } else { "No" });
                });
                ui.horizontal(|ui| {
                    ui.label("Fill Area:");
                    ui.label(if line_config.fill_area { "Yes" } else { "No" });
                });
                ui.horizontal(|ui| {
                    ui.label("Smooth Lines:");
                    ui.label(if line_config.smooth_lines { "Yes" } else { "No" });
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
                        ui.label("Mean Y:");
                        ui.label(format!("{:.3}", stats.mean_y));
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