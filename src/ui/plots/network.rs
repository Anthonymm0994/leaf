use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct NetworkPlot;

impl PlotTrait for NetworkPlot {
    fn name(&self) -> &'static str {
        "Network Graph"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![DataType::Float64, DataType::Int64, DataType::Utf8])
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Float64, DataType::Int64, DataType::Utf8]
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("Color", vec![DataType::Float64, DataType::Int64, DataType::Utf8]),
            ("Size", vec![DataType::Float64, DataType::Int64]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for network graphs".to_string());
        }
        
        // For large datasets, sample the data
        let max_points = 1500; // Limit for performance
        let sample_size = query_result.rows.len().min(max_points);
        let step = if query_result.rows.len() > max_points {
            query_result.rows.len() / max_points
        } else {
            1
        };
        
        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column)
            .ok_or("X column not found")?;
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or("Y column not found")?;
        
        // Find color column
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
        let mut nodes = HashMap::new();
        let mut edges = Vec::new();
        
        for (row_idx, row) in query_result.rows.iter().enumerate().step_by(step) {
            if row.len() > x_idx && row.len() > y_idx {
                let source = row[x_idx].clone();
                let target = row[y_idx].clone();
                
                // Create color mapping
                let color = if let Some(color_idx) = color_idx {
                    if row.len() > color_idx {
                        let color_value = &row[color_idx];
                        if let Ok(num_val) = color_value.parse::<f64>() {
                            let normalized = (num_val - 0.0).max(0.0).min(1.0);
                            Color32::from_rgb(
                                (normalized * 255.0) as u8,
                                ((1.0 - normalized) * 255.0) as u8,
                                128
                            )
                        } else {
                            // Categorical color
                            let hash = color_value.chars().map(|c| c as u32).sum::<u32>();
                            Color32::from_rgb(
                                (hash % 256) as u8,
                                ((hash >> 8) % 256) as u8,
                                ((hash >> 16) % 256) as u8,
                            )
                        }
                    } else {
                        Color32::BLUE
                    }
                } else {
                    Color32::BLUE
                };
                
                // Add nodes if they don't exist
                if !nodes.contains_key(&source) {
                    nodes.insert(source.clone(), NodeData {
                        id: source.clone(),
                        x: (nodes.len() as f64 * 0.1) % 10.0,
                        y: (nodes.len() as f64 * 0.2) % 10.0,
                        color,
                    });
                }
                
                if !nodes.contains_key(&target) {
                    nodes.insert(target.clone(), NodeData {
                        id: target.clone(),
                        x: (nodes.len() as f64 * 0.3) % 10.0,
                        y: (nodes.len() as f64 * 0.4) % 10.0,
                        color,
                    });
                }
                
                // Add edge
                edges.push(EdgeData {
                    source: source.clone(),
                    target: target.clone(),
                    color,
                });
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Source".to_string(), source.clone());
                tooltip_data.insert("Target".to_string(), target.clone());
                tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
                
                points.push(PlotPoint {
                    x: nodes[&source].x,
                    y: nodes[&source].y,
                    z: None,
                    label: None,
                    color: Some(color),
                    size: None,
                    series_id: None,
                    tooltip_data,
                });
            }
        }
        
        // Calculate network statistics
        let statistics = calculate_network_statistics(&nodes, &edges);
        
        Ok(PlotData {
            points,
            series: vec![],
            metadata: super::PlotMetadata {
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
                ui.label(RichText::new("No data available for network graph").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Network Graph").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Nodes:");
                ui.label(format!("{}", data.points.len()));
            });
            
            ui.horizontal(|ui| {
                ui.label("Edges:");
                ui.label(format!("{}", data.points.len())); // Simplified
            });
            
            // Show network statistics
            if let Some(stats) = &data.statistics {
                ui.separator();
                ui.label(RichText::new("Network Statistics").strong());
                ui.horizontal(|ui| {
                    ui.label("Average Degree:");
                    ui.label(format!("{:.2}", stats.mean_y));
                });
                ui.horizontal(|ui| {
                    ui.label("Network Density:");
                    ui.label(format!("{:.3}", stats.std_y));
                });
                if let Some(corr) = stats.correlation {
                    ui.horizontal(|ui| {
                        ui.label("Connectivity:");
                        ui.label(if corr > 0.5 { "High" } 
                               else if corr > 0.2 { "Medium" } 
                               else { "Low" });
                    });
                }
            }
            
            ui.separator();
            
            // Network graph visualization
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(400.0));
            
            ui.allocate_ui(plot_size, |ui| {
                render_network_graph(ui, data, config, plot_size);
            });
            
            // Configuration panel
            ui.separator();
            ui.label(RichText::new("Configuration").strong());
            ui.horizontal(|ui| {
                ui.label("Layout:");
                ui.radio_value(&mut 0, 0, "Force-Directed");
                ui.radio_value(&mut 0, 1, "Circular");
                ui.radio_value(&mut 0, 2, "Hierarchical");
            });
            
            ui.horizontal(|ui| {
                ui.label("Show Edges:");
                ui.checkbox(&mut true, "");
            });
            
            ui.horizontal(|ui| {
                ui.label("Show Labels:");
                ui.checkbox(&mut true, "");
            });
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() && config.show_legend {
            ui.group(|ui| {
                ui.label(RichText::new("Network Nodes:").strong());
                ui.separator();
                
                for (i, point) in data.points.iter().take(10).enumerate() {
                    ui.horizontal(|ui| {
                        if let Some(color) = point.color {
                            ui.colored_label(color, "●");
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
        // Handle hover and selection for network
        if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
            for point in &data.points {
                let point_pos = Pos2::new(point.x as f32, point.y as f32);
                if (hover_pos - point_pos).length() < 10.0 {
                    // Show tooltip
                    ui.label(format!("Source: {} | Target: {}", 
                        point.tooltip_data.get("Source").unwrap_or(&"Unknown".to_string()),
                        point.tooltip_data.get("Target").unwrap_or(&"Unknown".to_string())));
                    break;
                }
            }
        }
        
        None
    }
}

/// Network node data structure
#[derive(Debug, Clone)]
struct NodeData {
    id: String,
    x: f64,
    y: f64,
    color: Color32,
}

/// Network edge data structure
#[derive(Debug, Clone)]
struct EdgeData {
    source: String,
    target: String,
    color: Color32,
}

/// Calculate network statistics
fn calculate_network_statistics(nodes: &HashMap<String, NodeData>, edges: &[EdgeData]) -> super::DataStatistics {
    if nodes.is_empty() {
        return super::DataStatistics {
            mean_x: 0.0,
            mean_y: 0.0,
            std_x: 0.0,
            std_y: 0.0,
            correlation: None,
            count: 0,
        };
    }
    
    let node_positions: Vec<(f64, f64)> = nodes.values().map(|n| (n.x, n.y)).collect();
    let degrees: Vec<f64> = nodes.values().map(|_| edges.len() as f64 / nodes.len() as f64).collect();
    
    let mean_x = node_positions.iter().map(|(x, _)| x).sum::<f64>() / node_positions.len() as f64;
    let mean_y = node_positions.iter().map(|(_, y)| y).sum::<f64>() / node_positions.len() as f64;
    
    let variance_x = node_positions.iter()
        .map(|(x, _)| (x - mean_x).powi(2))
        .sum::<f64>() / node_positions.len() as f64;
    let std_x = variance_x.sqrt();
    
    let variance_y = degrees.iter()
        .map(|d| (d - mean_y).powi(2))
        .sum::<f64>() / degrees.len() as f64;
    let std_y = variance_y.sqrt();
    
    // Calculate correlation between position and degree
    let correlation = if std_x > 0.0 && std_y > 0.0 {
        let covariance = node_positions.iter().zip(degrees.iter())
            .map(|((x, _), d)| (x - mean_x) * (d - mean_y))
            .sum::<f64>() / node_positions.len() as f64;
        Some(covariance / (std_x * std_y))
    } else {
        None
    };
    
    super::DataStatistics {
        mean_x,
        mean_y,
        std_x,
        std_y,
        correlation,
        count: nodes.len(),
    }
}

/// Render network graph
fn render_network_graph(ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration, size: Vec2) {
    if data.points.is_empty() {
        return;
    }
    
    let margin = 50.0;
    let plot_width = size.x - 2.0 * margin;
    let plot_height = size.y - 2.0 * margin;
    
    // Generate sample network data for demonstration
    let num_nodes = data.points.len().min(10);
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    
    // Create nodes in a circular layout
    for i in 0..num_nodes {
        let angle = 2.0 * std::f64::consts::PI * i as f64 / num_nodes as f64;
        let radius = 0.3;
        let x = 0.5 + radius * angle.cos();
        let y = 0.5 + radius * angle.sin();
        
        let color = data.points.get(i).and_then(|p| p.color).unwrap_or(Color32::BLUE);
        
        nodes.push((x, y, color));
    }
    
    // Create some edges
    for i in 0..num_nodes {
        let target = (i + 1) % num_nodes;
        if i != target {
            edges.push((i, target));
        }
    }
    
    // Draw edges
    for (source_idx, target_idx) in edges {
        if source_idx < nodes.len() && target_idx < nodes.len() {
            let (x1, y1, _) = nodes[source_idx];
            let (x2, y2, _) = nodes[target_idx];
            
            let screen_x1 = (margin as f64 + x1 * plot_width as f64) as f32;
            let screen_y1 = (margin as f64 + y1 * plot_height as f64) as f32;
            let screen_x2 = (margin as f64 + x2 * plot_width as f64) as f32;
            let screen_y2 = (margin as f64 + y2 * plot_height as f64) as f32;
            
            ui.painter().line_segment(
                [Pos2::new(screen_x1, screen_y1), Pos2::new(screen_x2, screen_y2)],
                Stroke::new(1.0, Color32::from_gray(150)),
            );
        }
    }
    
    // Draw nodes
    for (i, (x, y, color)) in nodes.iter().enumerate() {
        let screen_x = (margin as f64 + x * plot_width as f64) as f32;
        let screen_y = (margin as f64 + y * plot_height as f64) as f32;
        
        // Draw node
        ui.painter().circle_filled(
            Pos2::new(screen_x, screen_y),
            8.0,
            *color,
        );
        
        // Draw node border
        ui.painter().circle_stroke(
            Pos2::new(screen_x, screen_y),
            8.0,
            Stroke::new(2.0, Color32::BLACK),
        );
        
        // Draw node label
        ui.painter().text(
            Pos2::new(screen_x, screen_y + 15.0),
            egui::Align2::CENTER_TOP,
            &format!("N{}", i),
            egui::FontId::proportional(10.0),
            Color32::BLACK,
        );
    }
    
    // Draw legend
    ui.painter().text(
        Pos2::new(margin, margin + plot_height + 20.0),
        egui::Align2::LEFT_TOP,
        "Network Graph - Nodes represent entities, edges represent relationships",
        egui::FontId::proportional(12.0),
        Color32::GRAY,
    );
}
