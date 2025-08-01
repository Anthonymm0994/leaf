use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, DataSeries, PlotMetadata, ColorScheme};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke, Sense, FontId, Align2, Shape};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;
use std::f32::consts::PI;

pub struct SunburstPlot;

/// Sunburst configuration  
#[derive(Debug, Clone)]
pub struct SunburstConfig {
    pub hierarchy_columns: Vec<String>,
    pub value_column: Option<String>,
    
    // Visual options
    pub inner_radius_ratio: f32,
    pub color_scheme: ColorScheme,
    pub show_labels: bool,
    pub label_threshold: f32,
    pub show_values: bool,
    pub show_tooltip: bool,
    
    // Interaction
    pub enable_zoom: bool,
    pub animate_transitions: bool,
    pub highlight_on_hover: bool,
}

impl Default for SunburstConfig {
    fn default() -> Self {
        Self {
            hierarchy_columns: Vec::new(),
            value_column: None,
            inner_radius_ratio: 0.3,
            color_scheme: ColorScheme::Viridis,
            show_labels: true,
            label_threshold: 0.03,
            show_values: false,
            show_tooltip: true,
            enable_zoom: true,
            animate_transitions: true,
            highlight_on_hover: true,
        }
    }
}

#[derive(Clone, Debug)]
struct SunburstNode {
    name: String,
    value: f64,
    children: Vec<SunburstNode>,
    color: Color32,
    depth: usize,
    angle_start: f64,
    angle_end: f64,
    parent_path: Vec<String>,
}

impl SunburstNode {
    fn new(name: String, depth: usize, parent_path: Vec<String>) -> Self {
        Self {
            name,
            value: 0.0,
            children: Vec::new(),
            color: Color32::WHITE,
            depth,
            angle_start: 0.0,
            angle_end: 0.0,
            parent_path,
        }
    }
    
    fn add_child(&mut self, child: SunburstNode) {
        self.children.push(child);
    }
    
    fn calculate_value(&mut self) {
        if self.children.is_empty() {
            // Leaf node - value already set
            return;
        }
        
        // Internal node - sum children
        self.value = 0.0;
        for child in &mut self.children {
            child.calculate_value();
            self.value += child.value;
        }
    }
    
    fn assign_angles(&mut self, start_angle: f64, end_angle: f64) {
        self.angle_start = start_angle;
        self.angle_end = end_angle;
        
        if self.children.is_empty() || self.value == 0.0 {
            return;
        }
        
        let angle_range = end_angle - start_angle;
        let mut current_angle = start_angle;
        
        for child in &mut self.children {
            let child_angle_range = (child.value / self.value) * angle_range;
            child.assign_angles(current_angle, current_angle + child_angle_range);
            current_angle += child_angle_range;
        }
    }
    
    fn assign_colors(&mut self, color_scheme: &ColorScheme, index: &mut usize) {
        match color_scheme {
            ColorScheme::Viridis => {
                if self.depth == 1 {
                    // Top level gets distinct colors
                    self.color = super::categorical_color(*index);
                    *index += 1;
                } else if !self.children.is_empty() {
                    // Inherit parent color with slight variation
                    self.color = self.parent_color_variation();
                }
            }
            ColorScheme::Plasma => {
                let normalized = (*index as f32) / 20.0;
                self.color = super::viridis_color(normalized.into());
                *index += 1;
            }
            _ => {
                self.color = super::categorical_color(*index);
                *index += 1;
            }
        }
        
        for child in &mut self.children {
            child.assign_colors(color_scheme, index);
        }
    }
    
    fn parent_color_variation(&self) -> Color32 {
        // Lighten or darken parent color based on depth
        let factor = 1.0 + (self.depth as f32 - 1.0) * 0.1;
        Color32::from_rgba_unmultiplied(
            (self.color.r() as f32 * factor).min(255.0) as u8,
            (self.color.g() as f32 * factor).min(255.0) as u8,
            (self.color.b() as f32 * factor).min(255.0) as u8,
            self.color.a(),
        )
    }
    
    fn find_node_at_angle(&self, angle: f64, radius_ratio: f32, depth: usize) -> Option<&SunburstNode> {
        if angle >= self.angle_start && angle <= self.angle_end {
            if depth == self.depth {
                return Some(self);
            }
            
            for child in &self.children {
                if let Some(node) = child.find_node_at_angle(angle, radius_ratio, depth) {
                    return Some(node);
                }
            }
        }
        None
    }
}

/// Sunburst chart view
pub struct SunburstChart {
    pub config: SunburstConfig,
    
    // State
    root: Option<SunburstNode>,
    
    // Interaction state
    hovered_node: Option<Vec<String>>,
    selected_path: Vec<String>,
    zoom_level: usize,
    animation_progress: f32,
}

impl SunburstChart {
    pub fn new() -> Self {
        Self {
            config: SunburstConfig::default(),
            root: None,
            hovered_node: None,
            selected_path: Vec::new(),
            zoom_level: 0,
            animation_progress: 1.0,
        }
    }
    
    fn build_hierarchy(&mut self, query_result: &QueryResult) {
        if self.config.hierarchy_columns.is_empty() {
            return;
        }
        
        // Find column indices
        let mut column_indices = Vec::new();
        for col_name in &self.config.hierarchy_columns {
            if let Some(idx) = query_result.columns.iter().position(|c| c == col_name) {
                column_indices.push(idx);
            }
        }
        
        let value_idx = self.config.value_column.as_ref()
            .and_then(|col_name| query_result.columns.iter().position(|c| c == col_name));
        
        if column_indices.is_empty() {
            return;
        }
        
        // Build tree
        let mut root = SunburstNode::new("root".to_string(), 0, Vec::new());
        
        for row in &query_result.rows {
            if row.len() >= column_indices.iter().max().unwrap_or(&0) + 1 {
                let mut current_node = &mut root;
                let mut current_path = Vec::new();
                
                // Build path through hierarchy
                for (depth, &col_idx) in column_indices.iter().enumerate() {
                    if row.len() > col_idx {
                        let name = &row[col_idx];
                        current_path.push(name.clone());
                        
                        // Find or create child node
                        let child_idx = current_node.children.iter()
                            .position(|child| child.name == *name);
                        
                        if let Some(idx) = child_idx {
                            current_node = &mut current_node.children[idx];
                        } else {
                            let mut new_node = SunburstNode::new(
                                name.clone(),
                                depth + 1,
                                current_path.clone(),
                            );
                            
                            // Set value if this is a leaf node
                            if depth == column_indices.len() - 1 {
                                if let Some(value_idx) = value_idx {
                                    if row.len() > value_idx {
                                        new_node.value = row[value_idx].parse::<f64>().unwrap_or(1.0);
                                    } else {
                                        new_node.value = 1.0;
                                    }
                                } else {
                                    new_node.value = 1.0;
                                }
                            }
                            
                            current_node.children.push(new_node);
                            let last_idx = current_node.children.len() - 1;
                            current_node = &mut current_node.children[last_idx];
                        }
                    }
                }
            }
        }
        
        // Calculate values and assign colors
        root.calculate_value();
        let mut color_index = 0;
        root.assign_colors(&self.config.color_scheme, &mut color_index);
        
        self.root = Some(root);
    }
    
    fn draw_sunburst(&self, ui: &mut Ui, rect: Rect) {
        if let Some(ref root) = self.root {
            let painter = ui.painter();
            let center = rect.center();
            let max_radius = rect.width().min(rect.height()) * 0.4;
            let inner_radius = max_radius * self.config.inner_radius_ratio;
            
            // Assign angles to all nodes
            let mut root_mut = root.clone();
            root_mut.assign_angles(0.0, 2.0 * PI as f64);
            
            // Draw sunburst
            self.draw_node_recursive(&root_mut, center, inner_radius, max_radius, painter);
        }
    }
    
    fn draw_node_recursive(&self, node: &SunburstNode, center: Pos2, inner_radius: f32, outer_radius: f32, painter: &egui::Painter) {
        if node.children.is_empty() {
            // Draw leaf node
            self.draw_arc(
                painter,
                center,
                inner_radius,
                outer_radius,
                node.angle_start,
                node.angle_end,
                node.color,
                node.name.clone(),
                node.value,
                &node.parent_path,
            );
        } else {
            // Draw internal node
            self.draw_arc(
                painter,
                center,
                inner_radius,
                outer_radius,
                node.angle_start,
                node.angle_end,
                node.color,
                node.name.clone(),
                node.value,
                &node.parent_path,
            );
            
            // Draw children
            let child_inner_radius = inner_radius + (outer_radius - inner_radius) * 0.2;
            let child_outer_radius = outer_radius;
            
            for child in &node.children {
                self.draw_node_recursive(child, center, child_inner_radius, child_outer_radius, painter);
            }
        }
    }
    
    fn draw_arc(
        &self,
        painter: &egui::Painter,
        center: Pos2,
        inner_radius: f32,
        outer_radius: f32,
        start_angle: f64,
        end_angle: f64,
        color: Color32,
        label: String,
        value: f64,
        _path: &[String],
    ) {
        // Convert angles to radians
        let start_rad = start_angle as f32;
        let end_rad = end_angle as f32;
        
        // Create arc points
        let num_segments = 32;
        let mut points = Vec::new();
        
        for i in 0..=num_segments {
            let t = i as f32 / num_segments as f32;
            let angle = start_rad + t * (end_rad - start_rad);
            
            // Outer arc
            let outer_x = center.x + outer_radius * angle.cos();
            let outer_y = center.y + outer_radius * angle.sin();
            points.push(Pos2::new(outer_x, outer_y));
        }
        
        // Add inner arc points in reverse
        for i in (0..=num_segments).rev() {
            let t = i as f32 / num_segments as f32;
            let angle = start_rad + t * (end_rad - start_rad);
            
            // Inner arc
            let inner_x = center.x + inner_radius * angle.cos();
            let inner_y = center.y + inner_radius * angle.sin();
            points.push(Pos2::new(inner_x, inner_y));
        }
        
        // Draw filled arc
        if points.len() > 2 {
            let shape = Shape::convex_polygon(points, color, Stroke::new(1.0, Color32::BLACK));
            painter.add(shape);
        }
        
        // Draw label if large enough
        let angle_range = end_rad - start_rad;
        if angle_range > self.config.label_threshold && outer_radius - inner_radius > 20.0 {
            let mid_angle = start_rad + angle_range * 0.5;
            let label_radius = inner_radius + (outer_radius - inner_radius) * 0.5;
            let label_x = center.x + label_radius * mid_angle.cos();
            let label_y = center.y + label_radius * mid_angle.sin();
            let label_pos = Pos2::new(label_x, label_y);
            
            painter.text(
                label_pos,
                Align2::CENTER_CENTER,
                &label,
                FontId::proportional(10.0),
                Color32::WHITE,
            );
            
            // Draw value if enabled
            if self.config.show_values {
                let value_text = format!("{:.0}", value);
                painter.text(
                    label_pos + Vec2::new(0.0, 12.0),
                    Align2::CENTER_CENTER,
                    &value_text,
                    FontId::proportional(8.0),
                    Color32::WHITE,
                );
            }
        }
    }
    
    fn get_max_depth(&self, node: &SunburstNode) -> usize {
        let mut max_depth = node.depth;
        for child in &node.children {
            max_depth = max_depth.max(self.get_max_depth(child));
        }
        max_depth
    }
    
    fn handle_interaction(&mut self, ui: &mut Ui, rect: Rect) -> Response {
        let response = ui.allocate_response(rect.size(), Sense::click_and_drag());
        
        if response.clicked() {
            if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
                if let Some(ref root) = self.root {
                    let center = rect.center();
                    let max_radius = rect.width().min(rect.height()) * 0.4;
                    
                    // Calculate angle and radius from click position
                    let dx = hover_pos.x - center.x;
                    let dy = hover_pos.y - center.y;
                    let distance = (dx * dx + dy * dy).sqrt();
                    let angle = dy.atan2(dx);
                    
                    if distance <= max_radius {
                        // Find node at this position
                        let depth = ((distance / max_radius) * 5.0) as usize;
                        if let Some(node) = root.find_node_at_angle(angle as f64, 0.5, depth) {
                            self.selected_path = node.parent_path.clone();
                            self.selected_path.push(node.name.clone());
                        }
                    }
                }
            }
        }
        
        response
    }
    
    fn draw_breadcrumb(&self, ui: &mut Ui) {
        if !self.selected_path.is_empty() {
            ui.horizontal(|ui| {
                ui.label("Path:");
                for (i, path) in self.selected_path.iter().enumerate() {
                    if i > 0 {
                        ui.label(">");
                    }
                    ui.label(path);
                }
            });
        }
    }
}

impl PlotTrait for SunburstPlot {
    fn name(&self) -> &'static str { 
        "Sunburst Chart" 
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> { 
        Some(vec![DataType::Utf8]) // Hierarchy columns
    }
    
    fn required_y_types(&self) -> Vec<DataType> { 
        vec![DataType::Float64, DataType::Int64] // Value column
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("Value", vec![DataType::Float64, DataType::Int64]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    fn supports_multiple_series(&self) -> bool { false }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() {
            return Err("Hierarchy column is required for sunburst chart".to_string());
        }
        
        // For large datasets, sample the data
        let max_points = 3000; // Limit for performance
        let sample_size = query_result.rows.len().min(max_points);
        let step = if query_result.rows.len() > max_points {
            query_result.rows.len() / max_points
        } else {
            1
        };
        
        // Create sunburst instance for data processing
        let mut sunburst = SunburstChart::new();
        sunburst.config.hierarchy_columns = vec![config.x_column.clone()];
        
        if !config.y_column.is_empty() {
            sunburst.config.value_column = Some(config.y_column.clone());
        }
        
        // Build hierarchy with sampling
        let mut sampled_rows = Vec::new();
        for (i, row) in query_result.rows.iter().enumerate().step_by(step) {
            sampled_rows.push(row.clone());
        }
        
        // Create a modified query result with sampled data
        let sampled_rows_len = sampled_rows.len();
        let sampled_result = QueryResult {
            columns: query_result.columns.clone(),
            rows: sampled_rows,
            column_types: query_result.column_types.clone(),
            total_rows: Some(sampled_rows_len),
        };
        
        sunburst.build_hierarchy(&sampled_result);
        
        // Convert to plot data
        let mut points = Vec::new();
        let mut series = Vec::new();
        
        if let Some(root) = &sunburst.root {
            // Create a simple representation for the plot data
            points.push(PlotPoint {
                x: 0.0,
                y: 0.0,
                z: None,
                label: Some("Sunburst".to_string()),
                color: Some(Color32::BLUE),
                size: Some(10.0),
                series_id: Some("sunburst".to_string()),
                tooltip_data: HashMap::new(),
            });
            
            series.push(DataSeries {
                id: "sunburst".to_string(),
                name: "Sunburst".to_string(),
                points: points.clone(),
                color: Color32::BLUE,
                visible: true,
                style: super::SeriesStyle::Points { size: 10.0, shape: super::MarkerShape::Circle },
            });
        }
        
        // Calculate statistics
        let statistics = if let Some(root) = &sunburst.root {
            let total_value = root.value;
            let node_count = count_sunburst_nodes(root);
            let max_depth = get_sunburst_max_depth(root);
            
            super::DataStatistics {
                mean_x: 0.0, // Not applicable for sunburst
                mean_y: total_value / node_count as f64,
                std_x: 0.0,
                std_y: calculate_sunburst_value_std(root),
                correlation: None,
                count: node_count,
            }
        } else {
            super::DataStatistics {
                mean_x: 0.0,
                mean_y: 0.0,
                std_x: 0.0,
                std_y: 0.0,
                correlation: None,
                count: 0,
            }
        };
        
        Ok(PlotData {
            points,
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
            statistics: Some(statistics),
        })
    }
    
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if data.points.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("No data available for sunburst chart").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Sunburst Chart").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Data Points:");
                ui.label(format!("{}", data.points.len()));
            });
            
            // Show statistics
            if let Some(stats) = &data.statistics {
                ui.horizontal(|ui| {
                    ui.label("Total Value:");
                    ui.label(format!("{:.2}", stats.mean_y * stats.count as f64));
                });
                ui.horizontal(|ui| {
                    ui.label("Average Value:");
                    ui.label(format!("{:.3}", stats.mean_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Value Std Dev:");
                    ui.label(format!("{:.3}", stats.std_y));
                });
            }
            
            ui.separator();
            
            // Sunburst visualization area
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(400.0));
            
            ui.allocate_ui(plot_size, |ui| {
                // Create sunburst instance for rendering
                let mut sunburst = SunburstChart::new();
                sunburst.config.hierarchy_columns = vec![config.x_column.clone()];
                
                if !config.y_column.is_empty() {
                    sunburst.config.value_column = Some(config.y_column.clone());
                }
                
                // Note: In a real implementation, we would rebuild the hierarchy here
                // For now, we'll just show a placeholder visualization
                
                // Handle interaction
                let response = sunburst.handle_interaction(ui, plot_rect);
                
                // Draw sunburst
                sunburst.draw_sunburst(ui, plot_rect);
                
                // Draw breadcrumb
                sunburst.draw_breadcrumb(ui);
                
                // Show configuration options
                ui.separator();
                ui.label(RichText::new("Configuration").strong());
                ui.horizontal(|ui| {
                    ui.label("Inner Radius:");
                    ui.add(egui::Slider::new(&mut sunburst.config.inner_radius_ratio, 0.1..=0.8));
                });
                
                ui.horizontal(|ui| {
                    ui.label("Show Labels:");
                    ui.checkbox(&mut sunburst.config.show_labels, "");
                });
                
                ui.horizontal(|ui| {
                    ui.label("Show Values:");
                    ui.checkbox(&mut sunburst.config.show_values, "");
                });
                
                ui.horizontal(|ui| {
                    ui.label("Enable Zoom:");
                    ui.checkbox(&mut sunburst.config.enable_zoom, "");
                });
            });
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() && config.show_legend {
            ui.group(|ui| {
                ui.label(RichText::new("Sunburst Segments:").strong());
                ui.separator();
                
                for (i, point) in data.points.iter().take(10).enumerate() {
                    ui.horizontal(|ui| {
                        if let Some(color) = point.color {
                            ui.colored_label(color, "●");
                        }
                        ui.label(format!("Segment {}", i + 1));
                    });
                }
                
                if data.points.len() > 10 {
                    ui.label(format!("... and {} more segments", data.points.len() - 10));
                }
            });
        }
    }
    
    fn handle_interaction(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) -> Option<super::PlotInteraction> {
        // Handle hover and selection for sunburst
        if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
            for point in &data.points {
                let point_pos = Pos2::new(point.x as f32, point.y as f32);
                if (hover_pos - point_pos).length() < 10.0 {
                    // Show tooltip
                    ui.label(format!("Segment: {} | Value: {}", 
                        point.label.as_ref().unwrap_or(&"Unknown".to_string()),
                        point.size.unwrap_or(0.0)));
                    break;
                }
            }
        }
        
        None
    }
}

// Helper functions for statistics
fn count_sunburst_nodes(node: &SunburstNode) -> usize {
    let mut count = 1;
    for child in &node.children {
        count += count_sunburst_nodes(child);
    }
    count
}

fn get_sunburst_max_depth(node: &SunburstNode) -> usize {
    let mut max_depth = node.depth;
    for child in &node.children {
        max_depth = max_depth.max(get_sunburst_max_depth(child));
    }
    max_depth
}

fn calculate_sunburst_value_std(node: &SunburstNode) -> f64 {
    let values = collect_sunburst_values(node);
    if values.is_empty() {
        return 0.0;
    }
    
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter()
        .map(|v| (v - mean).powi(2))
        .sum::<f64>() / values.len() as f64;
    variance.sqrt()
}

fn collect_sunburst_values(node: &SunburstNode) -> Vec<f64> {
    let mut values = vec![node.value];
    for child in &node.children {
        values.extend(collect_sunburst_values(child));
    }
    values
}
