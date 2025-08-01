use egui::{Ui, Color32, RichText};
use egui_plot::{Bar, BarChart, Plot, Legend, PlotUi, PlotPoint as EguiPlotPoint};
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;
use crate::core::QueryResult;

use super::{
    Plot as PlotTrait, 
    PlotData, 
    PlotConfiguration, 
    PlotSpecificConfig, 
    BarChartConfig, 
    StackingMode, 
    SortOrder, 
    PlotInteraction,
    DataSeries,
    SeriesStyle,
    PlotMetadata,
    BarOrientation,
    data_processor::DataProcessor
};

pub struct BarChartPlot;

impl BarChartPlot {
    /// Process data for grouped bar charts
    async fn process_grouped_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<DataSeries>, String> {
        let _data_processor = DataProcessor::new();
        
        // Ensure we have a group column
        if config.group_column.is_none() || config.group_column.as_ref().unwrap().is_empty() {
            return Err("Group column is required for grouped bar charts".to_string());
        }
        
        let group_col = config.group_column.as_ref().unwrap();
        
        // Get unique categories from X column
        let categories = self.get_unique_categories(query_result, &config.x_column)?;
        
        // Get unique groups from group column
        let groups = self.get_unique_categories(query_result, group_col)?;
        
        // Get bar chart specific config
        let default_config;
        let bar_config = if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
            cfg
        } else {
            default_config = self.get_default_config();
            default_config.plot_specific.as_bar_chart()
        };
        
        // Create a series for each group
        let mut all_series = Vec::new();
        let colors = super::get_categorical_colors(&config.color_scheme);
        
        for (group_idx, group) in groups.iter().enumerate() {
            let color = colors[group_idx % colors.len()];
            let mut points = Vec::new();
            
            // For each category, get the value for this group
            for (cat_idx, category) in categories.iter().enumerate() {
                // Filter data for this category and group
                let filtered_rows: Vec<&Vec<String>> = query_result.rows.iter()
                    .filter(|row| {
                        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column).unwrap_or(0);
                        let group_idx = query_result.columns.iter().position(|c| c == group_col).unwrap_or(0);
                        
                        row.len() > x_idx && row.len() > group_idx && 
                        row[x_idx] == *category && row[group_idx] == *group
                    })
                    .collect();
                
                // Aggregate values
                let y_idx = query_result.columns.iter().position(|c| c == &config.y_column).unwrap_or(0);
                let sum: f64 = filtered_rows.iter()
                    .filter_map(|row| {
                        if row.len() > y_idx {
                            row[y_idx].parse::<f64>().ok()
                        } else {
                            None
                        }
                    })
                    .sum();
                
                // Calculate x position for grouped bars
                // Each category gets its own x position, and groups are offset within that position
                let base_x = cat_idx as f64;
                let group_count = groups.len() as f64;
                let bar_width = bar_config.bar_width as f64;
                let group_spacing = bar_config.group_spacing as f64;
                let total_width = group_count * bar_width + (group_count - 1.0) * group_spacing;
                let start_x = base_x - total_width / 2.0 + bar_width / 2.0;
                let x = start_x + (group_idx as f64) * (bar_width + group_spacing);
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Category".to_string(), category.clone());
                tooltip_data.insert("Group".to_string(), group.clone());
                tooltip_data.insert("Value".to_string(), format!("{:.2}", sum));
                
                points.push(super::PlotPoint {
                    x,
                    y: sum,
                    z: None,
                    label: Some(category.clone()),
                    color: Some(color),
                    size: None,
                    series_id: Some(group.clone()),
                    tooltip_data,
                });
            }
            
            // Create series for this group
            let series = DataSeries {
                id: group.clone(),
                name: group.clone(),
                points,
                color,
                visible: true,
                style: SeriesStyle::Bars { width: bar_config.bar_width },
            };
            
            all_series.push(series);
        }
        
        Ok(all_series)
    }
    
    /// Process data for stacked bar charts
    async fn process_stacked_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<DataSeries>, String> {
        let _data_processor = DataProcessor::new();
        
        // Ensure we have a group column
        if config.group_column.is_none() || config.group_column.as_ref().unwrap().is_empty() {
            return Err("Group column is required for stacked bar charts".to_string());
        }
        
        let group_col = config.group_column.as_ref().unwrap();
        
        // Get unique categories from X column
        let categories = self.get_unique_categories(query_result, &config.x_column)?;
        
        // Get unique groups from group column
        let groups = self.get_unique_categories(query_result, group_col)?;
        
        // Get bar chart specific config
        let default_config;
        let bar_config = if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
            cfg
        } else {
            default_config = self.get_default_config();
            default_config.plot_specific.as_bar_chart()
        };
        
        // Create a series for each group
        let mut all_series = Vec::new();
        let colors = super::get_categorical_colors(&config.color_scheme);
        
        // For stacked bars, we need to track the cumulative height for each category
        let mut cumulative_heights: HashMap<String, f64> = HashMap::new();
        for category in &categories {
            cumulative_heights.insert(category.clone(), 0.0);
        }
        
        // Process each group
        for (group_idx, group) in groups.iter().enumerate() {
            let color = colors[group_idx % colors.len()];
            let mut points = Vec::new();
            
            // For each category, get the value for this group
            for (cat_idx, category) in categories.iter().enumerate() {
                // Filter data for this category and group
                let filtered_rows: Vec<&Vec<String>> = query_result.rows.iter()
                    .filter(|row| {
                        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column).unwrap_or(0);
                        let group_idx = query_result.columns.iter().position(|c| c == group_col).unwrap_or(0);
                        
                        row.len() > x_idx && row.len() > group_idx && 
                        row[x_idx] == *category && row[group_idx] == *group
                    })
                    .collect();
                
                // Aggregate values
                let y_idx = query_result.columns.iter().position(|c| c == &config.y_column).unwrap_or(0);
                let sum: f64 = filtered_rows.iter()
                    .filter_map(|row| {
                        if row.len() > y_idx {
                            row[y_idx].parse::<f64>().ok()
                        } else {
                            None
                        }
                    })
                    .sum();
                
                // Get current cumulative height for this category
                let base_height = *cumulative_heights.get(category).unwrap_or(&0.0);
                let total_height = base_height + sum;
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Category".to_string(), category.clone());
                tooltip_data.insert("Group".to_string(), group.clone());
                tooltip_data.insert("Value".to_string(), format!("{:.2}", sum));
                tooltip_data.insert("Cumulative".to_string(), format!("{:.2}", total_height));
                
                points.push(super::PlotPoint {
                    x: cat_idx as f64,
                    y: sum,
                    z: Some(base_height), // Use z to store base height
                    label: Some(category.clone()),
                    color: Some(color),
                    size: None,
                    series_id: Some(group.clone()),
                    tooltip_data,
                });
                
                // Update cumulative height
                cumulative_heights.insert(category.clone(), total_height);
            }
            
            // Create series for this group
            let series = DataSeries {
                id: group.clone(),
                name: group.clone(),
                points,
                color,
                visible: true,
                style: SeriesStyle::Bars { width: bar_config.bar_width },
            };
            
            all_series.push(series);
        }
        
        Ok(all_series)
    }
    
    /// Process data for percent stacked bar charts
    async fn process_percent_stacked_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<DataSeries>, String> {
        let _data_processor = DataProcessor::new();
        
        // Ensure we have a group column
        if config.group_column.is_none() || config.group_column.as_ref().unwrap().is_empty() {
            return Err("Group column is required for percent stacked bar charts".to_string());
        }
        
        let group_col = config.group_column.as_ref().unwrap();
        
        // Get unique categories from X column
        let categories = self.get_unique_categories(query_result, &config.x_column)?;
        
        // Get unique groups from group column
        let groups = self.get_unique_categories(query_result, group_col)?;
        
        // Get bar chart specific config
        let default_config;
        let bar_config = if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
            cfg
        } else {
            default_config = self.get_default_config();
            default_config.plot_specific.as_bar_chart()
        };
        
        // First pass: calculate total for each category
        let mut category_totals: HashMap<String, f64> = HashMap::new();
        for category in &categories {
            let filtered_rows: Vec<&Vec<String>> = query_result.rows.iter()
                .filter(|row| {
                    let x_idx = query_result.columns.iter().position(|c| c == &config.x_column).unwrap_or(0);
                    row.len() > x_idx && row[x_idx] == *category
                })
                .collect();
            
            let y_idx = query_result.columns.iter().position(|c| c == &config.y_column).unwrap_or(0);
            let total: f64 = filtered_rows.iter()
                .filter_map(|row| {
                    if row.len() > y_idx {
                        row[y_idx].parse::<f64>().ok()
                    } else {
                        None
                    }
                })
                .sum();
            
            category_totals.insert(category.clone(), total);
        }
        
        // Create a series for each group
        let mut all_series = Vec::new();
        let colors = super::get_categorical_colors(&config.color_scheme);
        
        // For percent stacked bars, we need to track the cumulative percentage for each category
        let mut cumulative_percentages: HashMap<String, f64> = HashMap::new();
        for category in &categories {
            cumulative_percentages.insert(category.clone(), 0.0);
        }
        
        // Process each group
        for (group_idx, group) in groups.iter().enumerate() {
            let color = colors[group_idx % colors.len()];
            let mut points = Vec::new();
            
            // For each category, get the value for this group
            for (cat_idx, category) in categories.iter().enumerate() {
                // Filter data for this category and group
                let filtered_rows: Vec<&Vec<String>> = query_result.rows.iter()
                    .filter(|row| {
                        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column).unwrap_or(0);
                        let group_idx = query_result.columns.iter().position(|c| c == group_col).unwrap_or(0);
                        
                        row.len() > x_idx && row.len() > group_idx && 
                        row[x_idx] == *category && row[group_idx] == *group
                    })
                    .collect();
                
                // Aggregate values
                let y_idx = query_result.columns.iter().position(|c| c == &config.y_column).unwrap_or(0);
                let sum: f64 = filtered_rows.iter()
                    .filter_map(|row| {
                        if row.len() > y_idx {
                            row[y_idx].parse::<f64>().ok()
                        } else {
                            None
                        }
                    })
                    .sum();
                
                // Calculate percentage
                let total = category_totals.get(category).unwrap_or(&1.0);
                let percentage = if *total > 0.0 { (sum / total) * 100.0 } else { 0.0 };
                
                // Get current cumulative percentage for this category
                let base_percentage = *cumulative_percentages.get(category).unwrap_or(&0.0);
                let total_percentage = base_percentage + percentage;
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Category".to_string(), category.clone());
                tooltip_data.insert("Group".to_string(), group.clone());
                tooltip_data.insert("Value".to_string(), format!("{:.2}", sum));
                tooltip_data.insert("Percentage".to_string(), format!("{:.1}%", percentage));
                tooltip_data.insert("Cumulative %".to_string(), format!("{:.1}%", total_percentage));
                
                points.push(super::PlotPoint {
                    x: cat_idx as f64,
                    y: percentage,
                    z: Some(base_percentage), // Use z to store base percentage
                    label: Some(category.clone()),
                    color: Some(color),
                    size: None,
                    series_id: Some(group.clone()),
                    tooltip_data,
                });
                
                // Update cumulative percentage
                cumulative_percentages.insert(category.clone(), total_percentage);
            }
            
            // Create series for this group
            let series = DataSeries {
                id: group.clone(),
                name: group.clone(),
                points,
                color,
                visible: true,
                style: SeriesStyle::Bars { width: bar_config.bar_width },
            };
            
            all_series.push(series);
        }
        
        Ok(all_series)
    }
    
    /// Get unique categories from a column
    fn get_unique_categories(&self, query_result: &QueryResult, column: &str) -> Result<Vec<String>, String> {
        let col_idx = query_result.columns.iter().position(|c| c == column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;
        
        let mut categories = std::collections::HashSet::new();
        for row in &query_result.rows {
            if row.len() > col_idx {
                categories.insert(row[col_idx].clone());
            }
        }
        
        let mut categories_vec: Vec<String> = categories.into_iter().collect();
        
        // Try to sort categories intelligently
        if categories_vec.iter().any(|c| c.parse::<f64>().is_ok()) {
            // If all categories are numeric, sort numerically
            categories_vec.sort_by(|a, b| {
                a.parse::<f64>().unwrap_or(0.0)
                    .partial_cmp(&b.parse::<f64>().unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        } else if categories_vec.iter().any(|c| c.contains('-') && c.contains(':')) {
            // If categories look like timestamps, try to sort chronologically
            categories_vec.sort_by(|a, b| {
                if let (Ok(ts_a), Ok(ts_b)) = (
                    chrono::NaiveDateTime::parse_from_str(a, "%Y-%m-%d %H:%M:%S"),
                    chrono::NaiveDateTime::parse_from_str(b, "%Y-%m-%d %H:%M:%S")
                ) {
                    ts_a.cmp(&ts_b)
                } else {
                    a.cmp(b)
                }
            });
        } else {
            // Default alphabetical sort
            categories_vec.sort();
        }
        
        Ok(categories_vec)
    }
    
    /// Enhanced tooltip handling for bar charts
    fn handle_tooltips(&self, plot_ui: &PlotUi, data: &PlotData) {
        if let Some(pointer_coord) = plot_ui.pointer_coordinate() {
            // Find the bar under the cursor
            for series in &data.series {
                for point in &series.points {
                    let bar_width = if let SeriesStyle::Bars { width } = series.style {
                        width as f64
                    } else {
                        0.8
                    };
                    
                    let half_width = bar_width / 2.0;
                    if pointer_coord.x >= point.x - half_width && 
                       pointer_coord.x <= point.x + half_width &&
                       pointer_coord.y >= 0.0 && pointer_coord.y <= point.y {
                        
                        // Create tooltip text
                        let mut tooltip_text = String::new();
                        tooltip_text.push_str(&format!("Series: {}\n", series.name));
                        tooltip_text.push_str(&format!("Value: {:.2}\n", point.y));
                        
                        if let Some(label) = &point.label {
                            tooltip_text.push_str(&format!("Category: {}\n", label));
                        }
                        
                        // Add additional tooltip data
                        for (key, value) in &point.tooltip_data {
                            if key != "Value" && key != "Category" {
                                tooltip_text.push_str(&format!("{}: {}\n", key, value));
                            }
                        }
                        
                        // Note: In a real implementation, you would show this tooltip
                        // using egui::show_tooltip_at_pointer
                        break;
                    }
                }
            }
        }
    }
    
    /// Process data based on stacking mode
    async fn process_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<DataSeries>, String> {
        let _data_processor = DataProcessor::new();
        
        // Get bar chart specific config
        let default_config;
        let bar_config = if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
            cfg
        } else {
            default_config = self.get_default_config();
            default_config.plot_specific.as_bar_chart()
        };
        
        // Process based on stacking mode
        match bar_config.stacking_mode {
            StackingMode::None => {
                if config.group_column.is_some() && !config.group_column.as_ref().unwrap().is_empty() {
                    self.process_grouped_data(query_result, config).await
                } else {
                    // Simple bar chart
                    self.process_simple_data(query_result, config).await
                }
            },
            StackingMode::Stacked => {
                self.process_stacked_data(query_result, config).await
            },
            StackingMode::Percent => {
                self.process_percent_stacked_data(query_result, config).await
            },
        }
    }
    
    /// Process data for simple bar charts
    async fn process_simple_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<DataSeries>, String> {
        let _data_processor = DataProcessor::new();
        
        // Get unique categories from X column
        let categories = self.get_unique_categories(query_result, &config.x_column)?;
        
        // Get bar chart specific config
        let default_config;
        let bar_config = if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
            cfg
        } else {
            default_config = self.get_default_config();
            default_config.plot_specific.as_bar_chart()
        };
        
        // Create a single series
        let mut points = Vec::new();
        let colors = super::get_categorical_colors(&config.color_scheme);
        let series_color = colors[0]; // Use first color for the series
        
        for (cat_idx, category) in categories.iter().enumerate() {
            // Filter data for this category
            let filtered_rows: Vec<&Vec<String>> = query_result.rows.iter()
                .filter(|row| {
                    let x_idx = query_result.columns.iter().position(|c| c == &config.x_column).unwrap_or(0);
                    row.len() > x_idx && row[x_idx] == *category
                })
                .collect();
            
            // Aggregate values
            let y_idx = query_result.columns.iter().position(|c| c == &config.y_column).unwrap_or(0);
            let sum: f64 = filtered_rows.iter()
                .filter_map(|row| {
                    if row.len() > y_idx {
                        row[y_idx].parse::<f64>().ok()
                    } else {
                        None
                    }
                })
                .sum();
            
            // Create tooltip data
            let mut tooltip_data = HashMap::new();
            tooltip_data.insert("Category".to_string(), category.clone());
            tooltip_data.insert("Value".to_string(), format!("{:.2}", sum));
            
            points.push(super::PlotPoint {
                x: cat_idx as f64,
                y: sum,
                z: None,
                label: Some(category.clone()),
                color: Some(series_color), // Use consistent color for all bars
                size: None,
                series_id: Some("main".to_string()),
                tooltip_data,
            });
        }
        
        // Create series with meaningful name based on the data
        let series_name = if config.group_column.is_some() && !config.group_column.as_ref().unwrap().is_empty() {
            format!("{} by {}", config.y_column, config.group_column.as_ref().unwrap())
        } else {
            format!("{} by {}", config.y_column, config.x_column)
        };
        
        let series = DataSeries {
            id: "main".to_string(),
            name: series_name,
            points,
            color: series_color, // Set series color
            visible: true,
            style: SeriesStyle::Bars { width: bar_config.bar_width },
        };
        
        Ok(vec![series])
    }
    
    /// Helper method to get bar config
    fn as_bar_config(config: &PlotConfiguration) -> &BarChartConfig {
        if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
            cfg
        } else {
            // Use a simple default instead of static
            &BarChartConfig {
                bar_width: 0.8,
                group_spacing: 0.2,
                stacking_mode: StackingMode::None,
                sort_order: SortOrder::None,
                orientation: BarOrientation::Vertical,
            }
        }
    }
}

impl PlotTrait for BarChartPlot {
    fn name(&self) -> &'static str {
        "Bar Chart"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        // Bar charts can have categorical or numeric X axis
        Some(vec![
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
            DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64,
            DataType::Float32, DataType::Float64,
        ])
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        // Y axis must be numeric
        vec![
            DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
            DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64,
            DataType::Float16, DataType::Float32, DataType::Float64,
            DataType::Decimal128(38, 10), DataType::Decimal256(76, 10),
        ]
    }
    
    fn supports_multiple_series(&self) -> bool {
        true
    }
    
    fn supports_color_mapping(&self) -> bool {
        true
    }
    
    fn get_default_config(&self) -> PlotConfiguration {
        let mut config = PlotConfiguration::default();
        config.plot_specific = PlotSpecificConfig::BarChart(BarChartConfig {
            bar_width: 0.8,
            group_spacing: 0.2,
            stacking_mode: StackingMode::None,
            sort_order: SortOrder::None,
            orientation: BarOrientation::Vertical,
        });
        config
    }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        // Use tokio runtime to run async data processing
        let rt = tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?;
        
        // Get bar chart specific config
        let default_config;
        let bar_config = if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
            cfg
        } else {
            default_config = self.get_default_config();
            default_config.plot_specific.as_bar_chart()
        };
        
        // Process data based on stacking mode
        let series = match bar_config.stacking_mode {
            StackingMode::None => {
                // If we have a group column, use grouped bars
                if config.group_column.is_some() && !config.group_column.as_ref().unwrap().is_empty() {
                    rt.block_on(self.process_grouped_data(query_result, config))?
                } else {
                    // Otherwise use simple bars
                    rt.block_on(self.process_data(query_result, config))?
                }
            },
            StackingMode::Stacked => {
                rt.block_on(self.process_stacked_data(query_result, config))?
            },
            StackingMode::Percent => {
                rt.block_on(self.process_percent_stacked_data(query_result, config))?
            },
        };
        
        // Create plot metadata with proper axis labels
        let y_label = match bar_config.stacking_mode {
            StackingMode::Percent => format!("{} (%)", config.y_column),
            _ => config.y_column.clone(),
        };
        
        let metadata = super::PlotMetadata {
            title: config.title.clone(),
            x_label: config.x_column.clone(),
            y_label,
            show_legend: config.show_legend,
            show_grid: config.show_grid,
            color_scheme: config.color_scheme.clone(),
            extra_data: None,
        };
        
        // Flatten points for backward compatibility
        let points = series.iter().flat_map(|s| s.points.clone()).collect();
        
        Ok(PlotData {
            points,
            series,
            metadata,
            statistics: None,
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        let bar_config = if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
            cfg
        } else {
            return;
        };

        // Performance optimization: limit points for large datasets
        let max_points = 10000; // Limit for performance
        let mut total_points = 0;
        for series in &data.series {
            total_points += series.points.len();
        }
        
        if total_points > max_points {
            ui.colored_label(egui::Color32::YELLOW, 
                format!("⚠ Large dataset detected ({} points). Consider filtering data for better performance.", total_points));
        }

        // Create plot with proper configuration
        let mut plot = Plot::new("bar_chart")
            .allow_zoom(config.allow_zoom)
            .allow_drag(config.allow_pan)
            .show_grid(config.show_grid);
        
        // Only show built-in legend if we're not using custom legend
        if !config.show_legend {
            plot = plot.legend(Legend::default()
                .position(egui_plot::Corner::RightBottom)
                .background_alpha(0.8)
                .text_style(egui::TextStyle::Small));
        }

        // Add axis labels if enabled
        if config.show_axes_labels {
            plot = plot
                .x_axis_label(config.x_column.clone())
                .y_axis_label(config.y_column.clone());
        }
        
        // Note: Custom X-axis labels for categories would require more complex handling
        // For now, we'll use the default numeric labels

        // Track hover state for highlighting
        let mut hovered_bar: Option<(usize, usize)> = None; // (series_idx, point_idx)
        
        plot.show(ui, |plot_ui| {
            // Render each series as a separate BarChart
            for (series_idx, series) in data.series.iter().enumerate() {
                if !series.visible {
                    continue;
                }
                
                // Create bars for this series
                let mut bars = Vec::new();
                for (point_idx, point) in series.points.iter().enumerate() {
                    let bar_width = if let SeriesStyle::Bars { width } = &series.style {
                        *width as f64
                    } else {
                        bar_config.bar_width as f64
                    };
                    
                    // Check if this bar is being hovered
                    let is_hovered = if let Some(pointer_coord) = plot_ui.pointer_coordinate() {
                        let half_width = bar_width / 2.0;
                        pointer_coord.x >= point.x - half_width && 
                        pointer_coord.x <= point.x + half_width &&
                        pointer_coord.y >= 0.0 && pointer_coord.y <= point.y
                    } else {
                        false
                    };
                    
                    // Set hover state
                    if is_hovered {
                        hovered_bar = Some((series_idx, point_idx));
                    }
                    
                    // Use original color for consistency with legend
                    let bar_color = point.color.unwrap_or(series.color);
                    
                    let mut bar = Bar::new(point.x, point.y)
                        .width(bar_width)
                        .fill(bar_color);
                    
                    // Enhanced highlighting effect
                    if is_hovered {
                        // Use a subtle glow effect instead of harsh white outline
                        let glow_color = egui::Color32::from_rgba_unmultiplied(255, 255, 255, 60);
                        bar = bar.stroke(egui::Stroke::new(2.0, glow_color));
                        
                        // Add a subtle brightness increase
                        let brightened_color = brighten_color(bar_color, 0.2);
                        bar = bar.fill(brightened_color);
                    }
                    
                    // Handle stacked bars
                    if let Some(base_value) = point.z {
                        bar = bar.base_offset(base_value);
                    }
                    
                    bars.push(bar);
                }
                
                // Create BarChart for this series
                let chart = BarChart::new(bars)
                    .name(&series.name);
                
                plot_ui.bar_chart(chart);
            }
        });
        
        // Enhanced tooltip handling
        if config.show_tooltips {
            if let Some((series_idx, point_idx)) = hovered_bar {
                if let Some(series) = data.series.get(series_idx) {
                    if let Some(point) = series.points.get(point_idx) {
                        // Create comprehensive tooltip with better formatting
                        let mut tooltip_text = String::new();
                        tooltip_text.push_str(&format!("**Series:** {}\n", series.name));
                        tooltip_text.push_str(&format!("**Value:** {:.2}\n", point.y));
                        
                        if let Some(label) = &point.label {
                            // Try to format timestamp if it looks like one
                            let formatted_label = format_timestamp_label(label);
                            tooltip_text.push_str(&format!("**Category:** {}\n", formatted_label));
                        }
                        
                        // Add additional tooltip data with better formatting
                        for (key, value) in &point.tooltip_data {
                            if key != "Value" && key != "Category" && key != "Series" {
                                tooltip_text.push_str(&format!("**{}:** {}\n", key, value));
                            }
                        }
                        
                        // Show enhanced tooltip at pointer position
                        if let Some(_pointer_pos) = ui.input(|i| i.pointer.hover_pos()) {
                            egui::show_tooltip_at_pointer(ui.ctx(), egui::LayerId::new(egui::Order::Tooltip, egui::Id::new("bar_tooltip")), egui::Id::new("bar_tooltip"), |ui| {
                                // Use a styled tooltip with better colors
                                ui.visuals_mut().override_text_color = Some(egui::Color32::WHITE);
                                
                                ui.label(egui::RichText::new(tooltip_text).monospace());
                            });
                        }
                    }
                }
            }
        }
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() {
            ui.group(|ui| {
                // Get bar chart specific config
                let default_config;
                let bar_config = if let PlotSpecificConfig::BarChart(cfg) = &config.plot_specific {
                    cfg
                } else {
                    default_config = self.get_default_config();
                    default_config.plot_specific.as_bar_chart()
                };
                
                // Show plot title
                if !config.title.is_empty() {
                    ui.label(RichText::new(&config.title).strong().size(16.0));
                    ui.separator();
                }
                
                // Show dataset info
                let total_points: usize = data.series.iter().map(|s| s.points.len()).sum();
                ui.label(RichText::new(format!("Dataset: {} points", total_points)).italics());
                
                // Performance warning for large datasets
                if total_points > 10000 {
                    ui.colored_label(egui::Color32::YELLOW, 
                        "⚠ Large dataset - consider filtering for better performance");
                }
                
                ui.separator();
                
                // Determine if we're showing series or categories in the legend
                let has_multiple_series = data.series.len() > 1;
                
                if has_multiple_series {
                    // For grouped or stacked bars, show series in legend
                    ui.label(RichText::new("Series:").strong());
                    ui.separator();
                    
                    // Sort series by name for consistent display
                    let mut sorted_series: Vec<_> = data.series.iter().collect();
                    sorted_series.sort_by(|a, b| a.name.cmp(&b.name));
                    
                    for series in &sorted_series {
                        let mut is_checked = series.visible;
                        if ui.checkbox(&mut is_checked, &series.name).changed() {
                            // TODO: Handle toggling series visibility
                            // This would require mutable access to data, which we don't have here
                            // We'll need to return a PlotInteraction to handle this
                        }
                        
                        ui.horizontal(|ui| {
                            ui.colored_label(series.color, "■■■");
                            ui.label(&series.name);
                            
                            // Show point count for this series
                            ui.label(RichText::new(format!("({} bars)", series.points.len())).weak());
                        });
                    }
                    
                    // Add stacking mode info if applicable
                    match bar_config.stacking_mode {
                        StackingMode::Stacked => {
                            ui.separator();
                            ui.label(RichText::new("Stacked Bar Chart").italics());
                            ui.label("Bars are stacked on top of each other");
                        },
                        StackingMode::Percent => {
                            ui.separator();
                            ui.label(RichText::new("Percentage Stacked Bar Chart").italics());
                            ui.label("Bars show percentage contribution");
                        },
                        _ => {
                            ui.separator();
                            ui.label(RichText::new("Grouped Bar Chart").italics());
                            ui.label("Bars are grouped side by side");
                        }
                    }
                } else {
                    // For simple bars, show categories or groups
                    ui.label(RichText::new("Categories:").strong());
                    ui.separator();
                    
                    // Get unique categories with their colors
                    let mut category_colors = std::collections::HashMap::new();
                    for series in &data.series {
                        if series.visible {
                            for point in &series.points {
                                // Use point label if available, otherwise use series name
                                let label = point.label.as_ref().unwrap_or(&series.name);
                                let color = point.color.unwrap_or(series.color);
                                category_colors.insert(label.clone(), color);
                            }
                        }
                    }
                    
                    // Sort categories alphabetically for consistent display
                    let mut categories: Vec<(String, Color32)> = category_colors
                        .into_iter()
                        .map(|(k, v)| (k, v))
                        .collect();
                    
                    categories.sort_by(|a, b| a.0.cmp(&b.0));
                    
                    // Limit display for large datasets
                    let max_categories = 20;
                    let display_categories = if categories.len() > max_categories {
                        &categories[..max_categories]
                    } else {
                        &categories
                    };
                    
                    // Display categories
                    for (label, color) in display_categories {
                        ui.horizontal(|ui| {
                            ui.colored_label(*color, "■■■");
                            ui.label(&*label);
                        });
                    }
                    
                    // Show warning if too many categories
                    if categories.len() > max_categories {
                        ui.colored_label(egui::Color32::YELLOW, 
                            format!("... and {} more categories", categories.len() - max_categories));
                    }
                    
                    // Add sorting info if applicable
                    match bar_config.sort_order {
                        SortOrder::Ascending => {
                            ui.separator();
                            ui.label(RichText::new("Sorted by value (ascending)").italics());
                        },
                        SortOrder::Descending => {
                            ui.separator();
                            ui.label(RichText::new("Sorted by value (descending)").italics());
                        },
                        SortOrder::ByValue => {
                            ui.separator();
                            ui.label(RichText::new("Sorted by category").italics());
                        },
                        _ => {}
                    }
                }
                
                // Show orientation info
                ui.separator();
                match bar_config.orientation {
                    BarOrientation::Vertical => {
                        ui.label(RichText::new("Orientation: Vertical").weak());
                    },
                    BarOrientation::Horizontal => {
                        ui.label(RichText::new("Orientation: Horizontal").weak());
                    }
                }
                
                // Show configuration details
                ui.label(RichText::new(format!("Bar Width: {:.1}", bar_config.bar_width)).weak());
                if bar_config.group_spacing > 0.0 {
                    ui.label(RichText::new(format!("Group Spacing: {:.1}", bar_config.group_spacing)).weak());
                }
            });
        }
    }
    
    fn handle_interaction(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) -> Option<PlotInteraction> {
        // Handle series toggling from legend clicks
        if data.metadata.show_legend && data.series.len() > 1 {
            ui.horizontal(|ui| {
                ui.label(RichText::new("Toggle series:").strong());
                
                for series in &data.series {
                    let mut is_visible = series.visible;
                    if ui.checkbox(&mut is_visible, &series.name).changed() {
                        // Note: Can't return from closure, handle this elsewhere
                        // return Some(PlotInteraction::SeriesToggled(series.id.clone()));
                    }
                }
            });
        }
        
        // Handle bar selection
        if config.allow_selection {
            // TODO: Implement bar selection interaction
            // This would return PlotInteraction::PointSelected with indices of selected bars
        }
        
        None
    }
}

/// Helper function to brighten a color
fn brighten_color(color: Color32, factor: f32) -> Color32 {
    let r = (color.r() as f32 * (1.0 + factor)).min(255.0) as u8;
    let g = (color.g() as f32 * (1.0 + factor)).min(255.0) as u8;
    let b = (color.b() as f32 * (1.0 + factor)).min(255.0) as u8;
    Color32::from_rgb(r, g, b)
}

/// Helper function to format timestamp labels
fn format_timestamp_label(label: &str) -> String {
    // Try to parse as timestamp and format nicely
    if label.contains('-') && label.contains(':') {
        // Looks like a timestamp, try to format it
        if let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(label, "%Y-%m-%d %H:%M:%S") {
            return timestamp.format("%Y-%m-%d %H:%M").to_string();
        } else if let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(label, "%Y-%m-%d %H:%M:%S%.f") {
            return timestamp.format("%Y-%m-%d %H:%M").to_string();
        }
    }
    
    // If it's a numeric timestamp, try to convert
    if let Ok(timestamp) = label.parse::<f64>() {
        // Assume it's a Unix timestamp
        if let Some(datetime) = chrono::DateTime::from_timestamp(timestamp as i64, 0) {
            return datetime.format("%Y-%m-%d %H:%M").to_string();
        }
    }
    
    // Return original if no formatting applies
    label.to_string()
}