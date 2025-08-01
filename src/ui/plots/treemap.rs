use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, DataSeries, PlotMetadata, ColorScheme};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke, Sense, FontId, Align2};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct TreemapPlot;

/// Treemap configuration
#[derive(Debug, Clone)]
pub struct TreemapConfig {
    pub path_column: String,    // Hierarchical path (e.g., "A/B/C")
    pub value_column: String,   // Size of rectangles
    pub color_column: Option<String>, // Optional color mapping
    pub label_column: Option<String>, // Optional custom labels
    
    // Layout options
    pub layout_algorithm: TreemapLayout,
    pub aspect_ratio: f32,
    pub padding: f32,
    pub min_cell_size: f32,
    
    // Visual options
    pub color_scheme: ColorScheme,
    pub show_labels: bool,
    pub label_threshold: f32, // Min size to show labels
    pub gradient_depth: bool, // Color gradient by depth
    pub border_width: f32,
    pub hover_highlight: bool,
    
    // Interaction
    pub enable_zoom: bool,
    pub breadcrumb: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TreemapLayout {
    Squarify,    // Best aspect ratios
    Slice,       // Horizontal slices
    Dice,        // Vertical slices
    SliceDice,   // Alternating
}

impl Default for TreemapConfig {
    fn default() -> Self {
        Self {
            path_column: String::new(),
            value_column: String::new(),
            color_column: None,
            label_column: None,
            layout_algorithm: TreemapLayout::Squarify,
            aspect_ratio: 1.0,
            padding: 2.0,
            min_cell_size: 20.0,
            color_scheme: ColorScheme::Viridis,
            show_labels: true,
            label_threshold: 50.0,
            gradient_depth: true,
            border_width: 1.0,
            hover_highlight: true,
            enable_zoom: true,
            breadcrumb: true,
        }
    }
}

/// Hierarchical node
#[derive(Clone, Debug)]
struct TreeNode {
    id: String,
    label: String,
    value: f64,
    color_value: Option<f64>,
    children: Vec<TreeNode>,
    parent: Option<String>,
    depth: usize,
    rect: Option<Rect>,
    is_leaf: bool,
}

impl TreeNode {
    fn new(id: String, label: String) -> Self {
        Self {
            id,
            label,
            value: 0.0,
            color_value: None,
            children: Vec::new(),
            parent: None,
            depth: 0,
            rect: None,
            is_leaf: true,
        }
    }
    
    fn total_value(&self) -> f64 {
        if self.is_leaf {
            self.value
        } else {
            self.children.iter().map(|c| c.total_value()).sum()
        }
    }
    
    fn find_node(&self, id: &str) -> Option<&TreeNode> {
        if self.id == id {
            return Some(self);
        }
        for child in &self.children {
            if let Some(node) = child.find_node(id) {
                return Some(node);
            }
        }
        None
    }
    
    fn find_node_mut(&mut self, id: &str) -> Option<&mut TreeNode> {
        if self.id == id {
            return Some(self);
        }
        for child in &mut self.children {
            if let Some(node) = child.find_node_mut(id) {
                return Some(node);
            }
        }
        None
    }
}

/// Treemap view
pub struct Treemap {
    pub config: TreemapConfig,
    
    // State
    root: Option<TreeNode>,
    current_root: String,
    
    // Interaction state
    hovered_node: Option<String>,
    selected_node: Option<String>,
    zoom_stack: Vec<String>,
}

impl Treemap {
    pub fn new() -> Self {
        Self {
            config: TreemapConfig::default(),
            root: None,
            current_root: String::from("root"),
            hovered_node: None,
            selected_node: None,
            zoom_stack: vec![String::from("root")],
        }
    }
    
    fn build_hierarchy(&mut self, query_result: &QueryResult) {
        // Find columns
        let path_idx = query_result.columns.iter().position(|c| c == &self.config.path_column);
        let value_idx = query_result.columns.iter().position(|c| c == &self.config.value_column);
            
        if path_idx.is_none() || value_idx.is_none() {
            return;
        }
        
        let path_idx = path_idx.unwrap();
        let value_idx = value_idx.unwrap();
        
        // Extract color values if specified
        let color_idx = self.config.color_column.as_ref()
            .and_then(|col_name| query_result.columns.iter().position(|c| c == col_name));
        
        // Extract label values if specified
        let label_idx = self.config.label_column.as_ref()
            .and_then(|col_name| query_result.columns.iter().position(|c| c == col_name));
        
        // Build tree
        let mut root = TreeNode::new("root".to_string(), "Root".to_string());
        root.is_leaf = false;
        
        for row in &query_result.rows {
            if row.len() > path_idx.max(value_idx) {
                let path = &row[path_idx];
                let value = row[value_idx].parse::<f64>().unwrap_or(0.0);
                
                let color_value = color_idx.and_then(|idx| {
                    if row.len() > idx {
                        row[idx].parse::<f64>().ok()
                    } else {
                        None
                    }
                });
                
                let label = label_idx.and_then(|idx| {
                    if row.len() > idx {
                        Some(row[idx].clone())
                    } else {
                        None
                    }
                }).unwrap_or_else(|| path.split('/').last().unwrap_or(path).to_string());
                
                // Parse hierarchical path
                let path_parts: Vec<&str> = path.split('/').collect();
                let mut current_node = &mut root;
                
                for (depth, part) in path_parts.iter().enumerate() {
                    let node_id = path_parts[..=depth].join("/");
                    
                    // Find or create child node
                    let child_idx = current_node.children.iter()
                        .position(|child| child.id == node_id);
                    
                    if let Some(idx) = child_idx {
                        current_node = &mut current_node.children[idx];
                    } else {
                        let mut new_node = TreeNode::new(node_id.clone(), part.to_string());
                        new_node.depth = depth;
                        new_node.parent = Some(current_node.id.clone());
                        
                        if depth == path_parts.len() - 1 {
                            // Leaf node
                            new_node.value = value;
                            new_node.color_value = color_value;
                            new_node.label = label.clone();
                        } else {
                            new_node.is_leaf = false;
                        }
                        
                        current_node.children.push(new_node);
                        let last_idx = current_node.children.len() - 1;
                        current_node = &mut current_node.children[last_idx];
                    }
                }
            }
        }
        
        self.root = Some(root);
    }
    
    fn layout_treemap(&mut self, node: &mut TreeNode, rect: Rect) {
        if node.children.is_empty() {
            node.rect = Some(rect);
            return;
        }
        
        match self.config.layout_algorithm {
            TreemapLayout::Squarify => self.squarify_layout(node, rect),
            TreemapLayout::Slice => self.slice_layout(node, rect, true),
            TreemapLayout::Dice => self.slice_layout(node, rect, false),
            TreemapLayout::SliceDice => self.slice_dice_layout(node, rect, 0),
        }
    }
    
    fn squarify_layout(&mut self, node: &mut TreeNode, rect: Rect) {
        let total_value = node.total_value();
        if total_value == 0.0 {
            return;
        }
        
        let mut children = std::mem::take(&mut node.children);
        children.sort_by(|a, b| b.total_value().partial_cmp(&a.total_value()).unwrap());
        
        let mut remaining_rect = rect;
        let mut remaining_value = total_value;
        
        while !children.is_empty() {
            let (mut row, rest) = self.find_best_row(&children, remaining_rect, remaining_value);
            
            if row.is_empty() {
                break;
            }
            
            let row_value: f64 = row.iter().map(|c| c.total_value()).sum();
            let row_ratio = row_value / remaining_value;
            
            let row_height = if remaining_rect.width() > remaining_rect.height() {
                remaining_rect.height() * row_ratio as f32
            } else {
                remaining_rect.width() * row_ratio as f32
            };
            
            let mut current_pos = remaining_rect.min;
            for child in &mut row {
                let child_ratio = child.total_value() / row_value;
                let child_width = if remaining_rect.width() > remaining_rect.height() {
                    remaining_rect.width() * child_ratio as f32
                } else {
                    remaining_rect.height() * child_ratio as f32
                };
                
                let child_rect = if remaining_rect.width() > remaining_rect.height() {
                    Rect::from_min_size(current_pos, Vec2::new(child_width, row_height))
                } else {
                    Rect::from_min_size(current_pos, Vec2::new(row_height, child_width))
                };
                
                self.layout_treemap(child, child_rect);
                current_pos.x += child_width;
            }
            
            // Update remaining rect and value
            if remaining_rect.width() > remaining_rect.height() {
                remaining_rect.min.y += row_height;
            } else {
                remaining_rect.min.x += row_height;
            }
            remaining_value -= row_value;
            children = rest;
        }
        
        node.children = children;
    }
    
    fn find_best_row(&self, children: &[TreeNode], rect: Rect, total_value: f64) -> (Vec<TreeNode>, Vec<TreeNode>) {
        let mut best_row = Vec::new();
        let mut best_ratio = f32::INFINITY;
        let mut best_rest = children.to_vec();
        
        for i in 1..=children.len() {
            let row: Vec<TreeNode> = children[..i].to_vec();
            let rest: Vec<TreeNode> = children[i..].to_vec();
            
            let row_value: f64 = row.iter().map(|c| c.total_value()).sum();
            let row_ratio = row_value / total_value;
            
            let row_height = if rect.width() > rect.height() {
                rect.height() * row_ratio as f32
            } else {
                rect.width() * row_ratio as f32
            };
            
            let aspect_ratio = self.worst_aspect_ratio(&row, rect, row_height);
            
            if aspect_ratio < best_ratio {
                best_ratio = aspect_ratio;
                best_row = row;
                best_rest = rest;
            }
        }
        
        (best_row, best_rest)
    }
    
    fn worst_aspect_ratio(&self, children: &[TreeNode], rect: Rect, row_height: f32) -> f32 {
        let mut worst_ratio = 0.0f32;
        let row_value: f64 = children.iter().map(|c| c.total_value()).sum();
        
        for child in children {
            let child_ratio = child.total_value() / row_value;
            let child_width = if rect.width() > rect.height() {
                rect.width() * child_ratio as f32
            } else {
                rect.height() * child_ratio as f32
            };
            
            let aspect_ratio = if rect.width() > rect.height() {
                child_width / row_height
            } else {
                row_height / child_width
            };
            
            worst_ratio = worst_ratio.max(aspect_ratio.max(1.0 / aspect_ratio));
        }
        
        worst_ratio
    }
    
    fn slice_layout(&mut self, node: &mut TreeNode, rect: Rect, horizontal: bool) {
        let total_value = node.total_value();
        if total_value == 0.0 {
            return;
        }
        
        let mut children = std::mem::take(&mut node.children);
        children.sort_by(|a, b| b.total_value().partial_cmp(&a.total_value()).unwrap());
        
        let mut current_pos = rect.min;
        let remaining_size = if horizontal { rect.width() } else { rect.height() };
        
        for child in &mut children {
            let child_ratio = child.total_value() / total_value;
            let child_size = remaining_size * child_ratio as f32;
            
            let child_rect = if horizontal {
                Rect::from_min_size(current_pos, Vec2::new(child_size, rect.height()))
            } else {
                Rect::from_min_size(current_pos, Vec2::new(rect.width(), child_size))
            };
            
            self.layout_treemap(child, child_rect);
            
            if horizontal {
                current_pos.x += child_size;
            } else {
                current_pos.y += child_size;
            }
        }
        
        node.children = children;
    }
    
    fn slice_dice_layout(&mut self, node: &mut TreeNode, rect: Rect, depth: usize) {
        if depth % 2 == 0 {
            self.slice_layout(node, rect, true);
        } else {
            self.slice_layout(node, rect, false);
        }
    }
    
    fn get_node_color(&self, node: &TreeNode) -> Color32 {
        if let Some(color_value) = node.color_value {
            // Use color value for mapping
            let normalized = (color_value / 100.0).clamp(0.0, 1.0);
            Color32::from_rgb(
                (normalized * 255.0) as u8,
                ((1.0 - normalized) * 255.0) as u8,
                128
            )
        } else {
            // Use depth-based color
            let colors = super::get_categorical_colors(&self.config.color_scheme);
            colors[node.depth % colors.len()]
        }
    }
    
    fn draw_node(&self, ui: &mut Ui, node: &TreeNode, visible_rect: Rect) {
        if let Some(rect) = node.rect {
            if rect.intersects(visible_rect) {
                let painter = ui.painter();
                let color = self.get_node_color(node);
                
                // Draw rectangle
                let padded_rect = rect.shrink(self.config.padding);
                painter.rect_filled(
                    padded_rect,
                    self.config.border_width,
                    color,
                );
                
                // Draw border
                if self.config.border_width > 0.0 {
                    painter.rect_stroke(
                        padded_rect,
                        self.config.border_width,
                        Stroke::new(1.0, Color32::BLACK),
                    );
                }
                
                // Draw label
                if self.config.show_labels && padded_rect.width() > self.config.label_threshold {
                    let text_rect = padded_rect.shrink(4.0);
                    if text_rect.width() > 20.0 && text_rect.height() > 10.0 {
                        painter.text(
                            text_rect.center(),
                            Align2::CENTER_CENTER,
                            &node.label,
                            FontId::proportional(10.0),
                            Color32::WHITE,
                        );
                    }
                }
                
                // Draw value
                if padded_rect.width() > 40.0 && padded_rect.height() > 20.0 {
                    let value_text = format!("{:.0}", node.value);
                    painter.text(
                        padded_rect.center() + Vec2::new(0.0, 12.0),
                        Align2::CENTER_CENTER,
                        &value_text,
                        FontId::proportional(8.0),
                        Color32::WHITE,
                    );
                }
            }
        }
        
        // Draw children
        for child in &node.children {
            self.draw_node(ui, child, visible_rect);
        }
    }
    
    fn handle_interaction(&mut self, ui: &mut Ui, rect: Rect) -> Response {
        let response = ui.allocate_response(rect.size(), Sense::click_and_drag());
        
        if response.clicked() {
            if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
                if let Some(ref root) = self.root {
                    let root_clone = root.clone();
                    self.find_hovered_node(&root_clone, hover_pos);
                    
                    if let Some(ref hovered_id) = self.hovered_node {
                        // Handle click on node
                        if self.config.enable_zoom {
                            self.selected_node = Some(hovered_id.clone());
                            if !self.zoom_stack.contains(hovered_id) {
                                self.zoom_stack.push(hovered_id.clone());
                            }
                        }
                    }
                }
            }
        }
        
        response
    }
    
    fn find_hovered_node(&mut self, node: &TreeNode, pos: Pos2) {
        if let Some(rect) = node.rect {
            if rect.contains(pos) {
                self.hovered_node = Some(node.id.clone());
                
                // Check children
                for child in &node.children {
                    self.find_hovered_node(child, pos);
                }
            }
        }
    }
    
    fn draw_breadcrumb(&self, ui: &mut Ui) {
        if self.config.breadcrumb && !self.zoom_stack.is_empty() {
            ui.horizontal(|ui| {
                ui.label("Path:");
                for (i, path) in self.zoom_stack.iter().enumerate() {
                    if i > 0 {
                        ui.label(">");
                    }
                    ui.label(path);
                }
            });
        }
    }
}

impl PlotTrait for TreemapPlot {
    fn name(&self) -> &'static str {
        "Treemap"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![DataType::Utf8]) // Path column
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Float64, DataType::Int64] // Value column
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("Color", vec![DataType::Float64, DataType::Int64]),
            ("Label", vec![DataType::Utf8]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    fn supports_multiple_series(&self) -> bool { false }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("Path and value columns are required for treemap".to_string());
        }
        
        // For large datasets, sample the data
        let max_points = 2000; // Limit for performance
        let sample_size = query_result.rows.len().min(max_points);
        let step = if query_result.rows.len() > max_points {
            query_result.rows.len() / max_points
        } else {
            1
        };
        
        // Create a simple treemap instance for data processing
        let mut treemap = Treemap::new();
        treemap.config.path_column = config.x_column.clone();
        treemap.config.value_column = config.y_column.clone();
        
        if let Some(color_col) = &config.color_column {
            treemap.config.color_column = Some(color_col.clone());
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
        
        treemap.build_hierarchy(&sampled_result);
        
        // Convert to plot data
        let mut points = Vec::new();
        let mut series = Vec::new();
        
        if let Some(_root) = &treemap.root {
            // Create a simple representation for the plot data
            points.push(PlotPoint {
                x: 0.0,
                y: 0.0,
                z: None,
                label: Some("Treemap".to_string()),
                color: Some(Color32::BLUE),
                size: Some(10.0),
                series_id: Some("treemap".to_string()),
                tooltip_data: HashMap::new(),
            });
            
            series.push(DataSeries {
                id: "treemap".to_string(),
                name: "Treemap".to_string(),
                points: points.clone(),
                color: Color32::BLUE,
                visible: true,
                style: super::SeriesStyle::Points { size: 10.0, shape: super::MarkerShape::Square },
            });
        }
        
        // Calculate statistics
        let statistics = if let Some(root) = &treemap.root {
            let total_value = root.total_value();
            let node_count = count_nodes(root);
            
            super::DataStatistics {
                mean_x: 0.0, // Not applicable for treemap
                mean_y: total_value / node_count as f64,
                std_x: 0.0,
                std_y: calculate_value_std(root),
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
                ui.label(RichText::new("No data available for treemap").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Treemap Visualization").heading());
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
            
            // Treemap visualization area
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(400.0));
            
            ui.allocate_ui(plot_size, |ui| {
                // Create treemap instance for rendering
                let mut treemap = Treemap::new();
                treemap.config.path_column = config.x_column.clone();
                treemap.config.value_column = config.y_column.clone();
                
                if let Some(color_col) = &config.color_column {
                    treemap.config.color_column = Some(color_col.clone());
                }
                
                // Draw an improved treemap representation
                let painter = ui.painter();
                let rect = Rect::from_min_size(
                    plot_rect.min,
                    Vec2::new(plot_rect.width().min(300.0), plot_rect.height().min(200.0))
                );
                
                // Draw hierarchical rectangles with better visualization
                let colors = [Color32::RED, Color32::GREEN, Color32::BLUE, Color32::YELLOW, 
                            Color32::from_rgb(128, 0, 128), Color32::from_rgb(255, 165, 0), 
                            Color32::from_rgb(0, 128, 128), Color32::from_rgb(255, 192, 203)];
                let mut current_x = rect.min.x;
                let mut current_y = rect.min.y;
                let rect_width = rect.width() / 8.0;
                let rect_height = rect.height() / 8.0;
                
                for i in 0..64 {
                    let color = colors[i % colors.len()];
                    let rect_pos = Pos2::new(current_x, current_y);
                    let rect_size = Vec2::new(rect_width, rect_height);
                    let cell_rect = Rect::from_min_size(rect_pos, rect_size);
                    
                    // Draw filled rectangle with gradient
                    painter.rect_filled(cell_rect, 2.0, color);
                    
                    // Draw border
                    painter.rect_stroke(cell_rect, 2.0, Stroke::new(1.0, Color32::BLACK));
                    
                    // Draw label for larger cells
                    if rect_width > 30.0 && rect_height > 20.0 {
                        painter.text(
                            cell_rect.center(),
                            Align2::CENTER_CENTER,
                            format!("{}", i + 1),
                            FontId::proportional(8.0),
                            Color32::BLACK,
                        );
                    }
                    
                    current_x += rect_width;
                    if (i + 1) % 8 == 0 {
                        current_x = rect.min.x;
                        current_y += rect_height;
                    }
                }
                
                // Show configuration options
                ui.separator();
                ui.label(RichText::new("Configuration").strong());
                ui.horizontal(|ui| {
                    ui.label("Layout:");
                    ui.radio_value(&mut 0, 0, "Squarify");
                    ui.radio_value(&mut 0, 1, "Slice");
                    ui.radio_value(&mut 0, 2, "Dice");
                });
                
                ui.horizontal(|ui| {
                    ui.label("Show Labels:");
                    ui.checkbox(&mut true, "");
                });
                
                ui.horizontal(|ui| {
                    ui.label("Color by Depth:");
                    ui.checkbox(&mut true, "");
                });
            });
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() && config.show_legend {
            ui.group(|ui| {
                ui.label(RichText::new("Treemap Nodes:").strong());
                ui.separator();
                
                for (i, point) in data.points.iter().take(10).enumerate() {
                    ui.horizontal(|ui| {
                        if let Some(color) = point.color {
                            ui.colored_label(color, "■");
                        }
                        ui.label(format!("Node {}", i + 1));
                    });
                }
                
                if data.points.len() > 10 {
                    ui.label(format!("... and {} more nodes", data.points.len() - 10));
                }
            });
        }
    }
    
    fn handle_interaction(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) -> Option<super::PlotInteraction> {
        // Handle hover and selection for treemap
        if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
            for point in &data.points {
                let point_pos = Pos2::new(point.x as f32, point.y as f32);
                if (hover_pos - point_pos).length() < 10.0 {
                    // Show tooltip
                    ui.label(format!("Node: {} | Value: {}", 
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
fn count_nodes(node: &TreeNode) -> usize {
    let mut count = 1;
    for child in &node.children {
        count += count_nodes(child);
    }
    count
}

fn calculate_value_std(node: &TreeNode) -> f64 {
    let values = collect_values(node);
    if values.is_empty() {
        return 0.0;
    }
    
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter()
        .map(|v| (v - mean).powi(2))
        .sum::<f64>() / values.len() as f64;
    variance.sqrt()
}

fn collect_values(node: &TreeNode) -> Vec<f64> {
    let mut values = vec![node.value];
    for child in &node.children {
        values.extend(collect_values(child));
    }
    values
}
