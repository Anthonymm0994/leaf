use egui::{Ui, Color32, RichText, Rect, Stroke, Vec2, pos2};
use std::ops::Add;
use egui_plot::{Plot, PlotUi, Text, PlotPoint, Polygon, PlotPoints};
use datafusion::arrow::datatypes::DataType;
use std::collections::{HashMap, HashSet};

use super::{
    Plot as PlotTrait, 
    PlotData, 
    PlotConfiguration, 
    PlotSpecificConfig, 
    ColorScheme,
    DataSeries,
    SeriesStyle,
    PlotMetadata,
    DataStatistics,
    PlotInteraction,
    // Enhanced utilities
    categorical_color, viridis_color, plasma_color, diverging_color,
    calculate_statistics, extract_numeric_values, extract_string_values,
    get_categorical_colors
};

pub struct HeatmapPlot;

impl HeatmapPlot {
    /// Handle tooltips for heatmap
    fn handle_tooltips(&self, ui: &mut Ui, plot_ui: &mut PlotUi, _data: &PlotData, matrix: &[Vec<f64>], row_labels: &[String], col_labels: &[String]) {
        if let Some(pointer_coord) = plot_ui.pointer_coordinate() {
            // Find the cell under the cursor
            let x = pointer_coord.x.round() as usize;
            let y = pointer_coord.y.round() as usize;
            
            if x < col_labels.len() && y < row_labels.len() {
                if let Some(row) = matrix.get(y) {
                    if let Some(&value) = row.get(x) {
                        // Show tooltip with cell data
                        let _tooltip_text = format!(
                            "Row: {}\nColumn: {}\nValue: {:.3}",
                            row_labels[y], col_labels[x], value
                        );
                        
                        // Tooltip functionality - using a workaround since show_tooltip is not available
                        if let Some(_hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
                            // Tooltip functionality - disabled due to API compatibility issues
                            // TODO: Implement tooltip using a different approach
                        }
                        
                        // Highlight the cell
                        let highlight_color = Color32::from_rgb(255, 255, 255);
                        let cell_rect = [
                            [x as f64 - 0.5, y as f64 - 0.5],
                            [x as f64 + 0.5, y as f64 - 0.5],
                            [x as f64 + 0.5, y as f64 + 0.5],
                            [x as f64 - 0.5, y as f64 + 0.5],
                        ];
                        
                        plot_ui.add(Polygon::new(PlotPoints::from(cell_rect.to_vec())).stroke(Stroke::new(2.0, highlight_color)));
                    }
                }
            }
        }
    }
    
    /// Process data for heatmap
    async fn process_data(&self, query_result: &crate::core::QueryResult, config: &PlotConfiguration) -> Result<(Vec<Vec<f64>>, Vec<String>, Vec<String>), String> {
        // We need both X and Y columns for heatmap
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("Both X and Y columns are required for heatmap".to_string());
        }
        
        // Get column indices
        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column)
            .ok_or_else(|| format!("X column '{}' not found", config.x_column))?;
        
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or_else(|| format!("Y column '{}' not found", config.y_column))?;
        
        // Get value column index (if specified, otherwise use a constant value)
        let value_idx = if let Some(color_col) = &config.color_column {
            query_result.columns.iter().position(|c| c == color_col)
                .ok_or_else(|| format!("Value column '{}' not found", color_col))?
        } else {
            // If no value column is specified, we'll just count occurrences
            usize::MAX
        };
        
        // Extract unique row and column labels
        let mut row_labels = HashSet::new();
        let mut col_labels = HashSet::new();
        
        for row in &query_result.rows {
            if row.len() > x_idx && row.len() > y_idx {
                col_labels.insert(row[x_idx].clone());
                row_labels.insert(row[y_idx].clone());
            }
        }
        
        // Convert to sorted vectors
        let mut row_labels: Vec<String> = row_labels.into_iter().collect();
        let mut col_labels: Vec<String> = col_labels.into_iter().collect();
        row_labels.sort();
        col_labels.sort();
        
        // Create a mapping from label to index
        let row_map: HashMap<String, usize> = row_labels.iter()
            .enumerate()
            .map(|(i, label)| (label.clone(), i))
            .collect();
        
        let col_map: HashMap<String, usize> = col_labels.iter()
            .enumerate()
            .map(|(i, label)| (label.clone(), i))
            .collect();
        
        // Initialize matrix with zeros
        let mut matrix = vec![vec![0.0; col_labels.len()]; row_labels.len()];
        
        // Fill matrix with values
        for row in &query_result.rows {
            if row.len() > x_idx && row.len() > y_idx {
                let col_label = &row[x_idx];
                let row_label = &row[y_idx];
                
                if let (Some(&row_idx), Some(&col_idx)) = (row_map.get(row_label), col_map.get(col_label)) {
                    let value = if value_idx != usize::MAX && row.len() > value_idx {
                        row[value_idx].parse::<f64>().unwrap_or(1.0)
                    } else {
                        // If no value column, increment count
                        1.0
                    };
                    
                    matrix[row_idx][col_idx] += value;
                }
            }
        }
        
        Ok((matrix, row_labels, col_labels))
    }
    
    /// Get color for a value based on the color scheme and range
    fn get_color_for_value(&self, value: f64, min_val: f64, max_val: f64, color_scheme: &ColorScheme) -> Color32 {
        // Normalize value to 0-1 range
        let normalized = if max_val > min_val {
            (value - min_val) / (max_val - min_val)
        } else {
            0.5 // Default to middle if min == max
        };
        
        match color_scheme {
            ColorScheme::Viridis => {
                // Use viridis color scheme
                let colors = color_scheme.get_colors(256);
                let idx = (normalized * 255.0).round() as usize;
                colors[idx.min(255)]
            },
            ColorScheme::Plasma => {
                // Use plasma color scheme
                let colors = color_scheme.get_colors(256);
                let idx = (normalized * 255.0).round() as usize;
                colors[idx.min(255)]
            },
            _ => {
                // Default heatmap colors (blue to red)
                let r = (normalized * 255.0) as u8;
                let b = ((1.0 - normalized) * 255.0) as u8;
                let g = 0;
                Color32::from_rgb(r, g, b)
            }
        }
    }
    
    /// Render a color legend for the heatmap
    fn render_color_legend(&self, ui: &mut Ui, min_val: f64, max_val: f64, color_scheme: &ColorScheme) {
        ui.vertical(|ui| {
            ui.label(RichText::new("Color Scale:").strong());
            
            // Create a color gradient rectangle
            let (rect, _) = ui.allocate_exact_size(Vec2::new(150.0, 20.0), egui::Sense::hover());
            
            if ui.is_rect_visible(rect) {
                let painter = ui.painter();
                
                // Draw gradient
                for i in 0..150 {
                    let normalized = i as f64 / 149.0;
                    let value = min_val + normalized * (max_val - min_val);
                    let color = self.get_color_for_value(value, min_val, max_val, color_scheme);
                    
                    let segment_rect = Rect::from_min_size(
                        pos2(rect.min.x + i as f32, rect.min.y),
                        Vec2::new(1.0, rect.height()),
                    );
                    
                    painter.rect_filled(segment_rect, 0.0, color);
                }
                
                // Draw border
                painter.rect_stroke(rect, 0.0, Stroke::new(1.0, Color32::from_gray(200)));
                
                // Draw min/max labels
                ui.horizontal(|ui| {
                    ui.label(format!("{:.2}", min_val));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(format!("{:.2}", max_val));
                    });
                });
            }
        });
    }
}

impl PlotTrait for HeatmapPlot {
    fn name(&self) -> &'static str {
        "Heatmap"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        // Heatmap X axis can be categorical or numeric
        Some(vec![
            DataType::Utf8, DataType::LargeUtf8,
            DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
            DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64,
            DataType::Float32, DataType::Float64,
        ])
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        // Heatmap Y axis can be categorical or numeric
        vec![
            DataType::Utf8, DataType::LargeUtf8,
            DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
            DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64,
            DataType::Float32, DataType::Float64,
        ]
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("Value", vec![
                DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
                DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64,
                DataType::Float32, DataType::Float64,
            ]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool {
        true
    }
    
    fn get_default_config(&self) -> PlotConfiguration {
        let mut config = PlotConfiguration::default();
        config.color_scheme = ColorScheme::Viridis;
        config
    }
    
    fn prepare_data(&self, query_result: &crate::core::QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        // Use tokio runtime to run async data processing
        let rt = tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?;
        
        let (matrix, row_labels, col_labels) = rt.block_on(self.process_data(query_result, config))?;
        
        // Find min and max values for color scaling
        let mut min_val = f64::MAX;
        let mut max_val = f64::MIN;
        
        for row in &matrix {
            for &value in row {
                min_val = min_val.min(value);
                max_val = max_val.max(value);
            }
        }
        
        // Create points for the heatmap cells
        let mut points = Vec::new();
        
        for (y, row) in matrix.iter().enumerate() {
            for (x, &value) in row.iter().enumerate() {
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Row".to_string(), row_labels[y].clone());
                tooltip_data.insert("Column".to_string(), col_labels[x].clone());
                tooltip_data.insert("Value".to_string(), format!("{:.3}", value));
                
                // Get color based on value
                let color = self.get_color_for_value(value, min_val, max_val, &config.color_scheme);
                
                points.push(super::PlotPoint {
                    x: x as f64,
                    y: y as f64,
                    z: None,
                    label: Some(format!("{:.3}", value)),
                    color: Some(color),
                    size: None,
                    series_id: Some("heatmap".to_string()),
                    tooltip_data,
                });
            }
        }
        
        // Create series for the heatmap
        let series = DataSeries {
            id: "heatmap".to_string(),
            name: "Heatmap".to_string(),
            points,
            color: Color32::from_rgb(100, 100, 255),
            visible: true,
            style: SeriesStyle::Bars { width: 1.0 },
        };
        
        // Create plot metadata
        let metadata = super::PlotMetadata {
            title: config.title.clone(),
            x_label: config.x_column.clone(),
            y_label: config.y_column.clone(),
            show_legend: config.show_legend,
            show_grid: config.show_grid,
            color_scheme: config.color_scheme.clone(),
            extra_data: None,
        };
        
        // Store matrix and labels in the first point's tooltip_data for later use
        let mut plot_data = PlotData {
            points: vec![series.points.clone()].concat(),
            series: vec![series],
            metadata,
            statistics: None,
        };
        
        // Store matrix dimensions and min/max values in a special field for rendering
        if let Some(first_point) = plot_data.points.first_mut() {
            first_point.tooltip_data.insert("__matrix_rows__".to_string(), row_labels.len().to_string());
            first_point.tooltip_data.insert("__matrix_cols__".to_string(), col_labels.len().to_string());
            first_point.tooltip_data.insert("__min_val__".to_string(), min_val.to_string());
            first_point.tooltip_data.insert("__max_val__".to_string(), max_val.to_string());
            
            // Store row and column labels (limited to first 10 for tooltip data size)
            for (i, label) in row_labels.iter().take(10).enumerate() {
                first_point.tooltip_data.insert(format!("__row_{}", i), label.clone());
            }
            
            for (i, label) in col_labels.iter().take(10).enumerate() {
                first_point.tooltip_data.insert(format!("__col_{}", i), label.clone());
            }
        }
        
        Ok(plot_data)
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label("No data points to display");
                ui.label(RichText::new("Configure X and Y columns").weak());
            });
            return;
        }
        
        // Extract matrix dimensions and min/max values
        let mut matrix_rows = 0;
        let mut matrix_cols = 0;
        let mut min_val = 0.0;
        let mut max_val = 1.0;
        let mut row_labels = Vec::new();
        let mut col_labels = Vec::new();
        
        if let Some(first_point) = data.points.first() {
            if let Some(rows_str) = first_point.tooltip_data.get("__matrix_rows__") {
                matrix_rows = rows_str.parse::<usize>().unwrap_or(0);
            }
            
            if let Some(cols_str) = first_point.tooltip_data.get("__matrix_cols__") {
                matrix_cols = cols_str.parse::<usize>().unwrap_or(0);
            }
            
            if let Some(min_str) = first_point.tooltip_data.get("__min_val__") {
                min_val = min_str.parse::<f64>().unwrap_or(0.0);
            }
            
            if let Some(max_str) = first_point.tooltip_data.get("__max_val__") {
                max_val = max_str.parse::<f64>().unwrap_or(1.0);
            }
            
            // Extract row and column labels
            for i in 0..10 {
                if let Some(label) = first_point.tooltip_data.get(&format!("__row_{}", i)) {
                    row_labels.push(label.clone());
                }
                
                if let Some(label) = first_point.tooltip_data.get(&format!("__col_{}", i)) {
                    col_labels.push(label.clone());
                }
            }
        }
        
        // Reconstruct matrix from points
        let mut matrix = vec![vec![0.0; matrix_cols.max(1)]; matrix_rows.max(1)];
        
        for point in &data.points {
            let x = point.x.round() as usize;
            let y = point.y.round() as usize;
            
            if x < matrix_cols && y < matrix_rows {
                if let Some(value_str) = point.tooltip_data.get("Value") {
                    if let Ok(value) = value_str.parse::<f64>() {
                        matrix[y][x] = value;
                    }
                }
            }
        }
        
        // Create plot
        let plot = Plot::new("heatmap")
            .x_axis_label(&data.metadata.x_label)
            .y_axis_label(&data.metadata.y_label)
            .show_grid(data.metadata.show_grid)
            .allow_zoom(config.allow_zoom)
            .allow_drag(config.allow_pan)
            .allow_boxed_zoom(config.allow_zoom)
            .data_aspect(1.0);
        
        // Store pointer coordinates for tooltip
        let mut hover_cell: Option<(usize, usize, f64)> = None;
        
        plot.show(ui, |plot_ui| {
            // Render heatmap cells
            for point in &data.points {
                let x = point.x.round() as f64;
                let y = point.y.round() as f64;
                
                // Create cell rectangle
                let cell_rect = [
                    [x - 0.5, y - 0.5],
                    [x + 0.5, y - 0.5],
                    [x + 0.5, y + 0.5],
                    [x - 0.5, y + 0.5],
                ];
                
                // Fill cell with color
                if let Some(color) = point.color {
                    let polygon = Polygon::new(PlotPoints::from(cell_rect.to_vec())).fill_color(color);
                    plot_ui.add(polygon);
                }
                
                // Add cell border
                let border = Polygon::new(PlotPoints::from(cell_rect.to_vec())).stroke(egui::Stroke::new(0.5, Color32::from_gray(50)));
                plot_ui.add(border);
                
                // Add value text for larger cells when zoomed in
                if matrix_cols > 0 && plot_ui.plot_bounds().width() / matrix_cols as f64 > 30.0 {
                    if let Some(value_str) = point.tooltip_data.get("Value") {
                        plot_ui.text(
                            Text::new(
                                PlotPoint::new(x, y),
                                value_str
                            )
                            .color(Color32::WHITE)
                            .anchor(egui::Align2::CENTER_CENTER)
                        );
                    }
                }
            }
            
            // Add row labels
            if matrix_cols > 0 && plot_ui.plot_bounds().width() / matrix_cols as f64 > 30.0 {
                for (y, label) in row_labels.iter().enumerate() {
                    if y < matrix_rows {
                        plot_ui.text(
                            Text::new(
                                PlotPoint::new(-1.0, y as f64),
                                label
                            )
                            .color(Color32::WHITE)
                            .anchor(egui::Align2::RIGHT_CENTER)
                        );
                    }
                }
                
                // Add column labels
                for (x, label) in col_labels.iter().enumerate() {
                    if x < matrix_cols {
                        plot_ui.text(
                            Text::new(
                                PlotPoint::new(x as f64, -1.0),
                                label
                            )
                            .color(Color32::WHITE)
                            .anchor(egui::Align2::CENTER_TOP)
                        );
                    }
                }
            }
            
            // Handle highlighting for cells and store hover cell for tooltip
            if config.show_tooltips && matrix_rows > 0 && matrix_cols > 0 {
                if let Some(pointer_coord) = plot_ui.pointer_coordinate() {
                    // Find the cell under the cursor
                    let x = pointer_coord.x.round() as usize;
                    let y = pointer_coord.y.round() as usize;
                    
                    if x < col_labels.len() && y < row_labels.len() {
                        if let Some(row) = matrix.get(y) {
                            if let Some(&value) = row.get(x) {
                                // Store hover cell for tooltip
                                hover_cell = Some((x, y, value));
                                
                                // Highlight the cell
                                let highlight_color = Color32::from_rgb(255, 255, 255);
                                let cell_rect = [
                                    [x as f64 - 0.5, y as f64 - 0.5],
                                    [x as f64 + 0.5, y as f64 - 0.5],
                                    [x as f64 + 0.5, y as f64 + 0.5],
                                    [x as f64 - 0.5, y as f64 + 0.5],
                                ];
                                
                                let highlight = Polygon::new(PlotPoints::from(cell_rect.to_vec()))
                                    .stroke(Stroke::new(2.0, highlight_color));
                                plot_ui.add(highlight);
                            }
                        }
                    }
                }
            }
        });
        
        // Show tooltip outside the plot closure
        if let Some((x, y, value)) = hover_cell {
            if x < col_labels.len() && y < row_labels.len() {
                let tooltip_text = format!(
                    "Row: {}\nColumn: {}\nValue: {:.3}",
                    row_labels[y], col_labels[x], value
                );
                
                if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
                    ui.ctx().debug_painter().text(
                        hover_pos.add(egui::vec2(15.0, 15.0)),
                        egui::Align2::LEFT_TOP,
                        tooltip_text,
                        egui::TextStyle::Body.resolve(ui.style()),
                        Color32::WHITE
                    );
                }
            }
        }
        
        // Render color legend
        self.render_color_legend(ui, min_val, max_val, &data.metadata.color_scheme);
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration) {
        // Extract min/max values
        let mut min_val = 0.0;
        let mut max_val = 1.0;
        
        if let Some(first_point) = data.points.first() {
            if let Some(min_str) = first_point.tooltip_data.get("__min_val__") {
                min_val = min_str.parse::<f64>().unwrap_or(0.0);
            }
            
            if let Some(max_str) = first_point.tooltip_data.get("__max_val__") {
                max_val = max_str.parse::<f64>().unwrap_or(1.0);
            }
        }
        
        // Render color legend
        self.render_color_legend(ui, min_val, max_val, &data.metadata.color_scheme);
    }
    
    fn handle_interaction(&self, _ui: &mut Ui, _data: &PlotData, _config: &PlotConfiguration) -> Option<PlotInteraction> {
        None
    }
}
