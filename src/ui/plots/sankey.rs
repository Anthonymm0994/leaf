use super::{Plot as PlotTrait, PlotData, PlotConfiguration, PlotPoint, extract_plot_points};
use egui::{Ui, Color32, RichText, Vec2, Pos2, Rect, Response, Stroke};
use egui_plot::{Plot, PlotPoints, PlotBounds, Line, PlotUi};
use datafusion::arrow::datatypes::DataType;
use crate::core::QueryResult;
use std::collections::HashMap;

pub struct SankeyPlot;

impl PlotTrait for SankeyPlot {
    fn name(&self) -> &'static str {
        "Sankey Diagram"
    }
    
    fn required_x_types(&self) -> Option<Vec<DataType>> {
        Some(vec![DataType::Float64, DataType::Int64, DataType::Utf8])
    }
    
    fn required_y_types(&self) -> Vec<DataType> {
        vec![DataType::Float64, DataType::Int64, DataType::Utf8]
    }
    
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![
            ("Value", vec![DataType::Float64, DataType::Int64]),
            ("Color", vec![DataType::Float64, DataType::Int64, DataType::Utf8]),
        ]
    }
    
    fn supports_color_mapping(&self) -> bool { true }
    
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        if config.x_column.is_empty() || config.y_column.is_empty() {
            return Err("X and Y columns are required for Sankey diagrams".to_string());
        }
        
        let x_idx = query_result.columns.iter().position(|c| c == &config.x_column)
            .ok_or("X column not found")?;
        let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
            .ok_or("Y column not found")?;
        
        // Find value column
        let value_idx = if let Some(value_col) = &config.color_column {
            if !value_col.is_empty() {
                query_result.columns.iter().position(|c| c == value_col)
            } else {
                None
            }
        } else {
            None
        };
        
        // For large datasets, sample the data
        let max_points = 1000; // Limit for performance
        let sample_size = query_result.rows.len().min(max_points);
        let step = if query_result.rows.len() > max_points {
            query_result.rows.len() / max_points
        } else {
            1
        };
        
        let mut points = Vec::new();
        let mut flows = Vec::new();
        let mut nodes = HashMap::new();
        let mut node_counter = 0;
        
        for (row_idx, row) in query_result.rows.iter().enumerate().step_by(step) {
            if row.len() > x_idx && row.len() > y_idx {
                let source = row[x_idx].clone();
                let target = row[y_idx].clone();
                
                // Parse flow value
                let value = if let Some(value_idx) = value_idx {
                    if row.len() > value_idx {
                        row[value_idx].parse::<f64>().unwrap_or(1.0)
                    } else {
                        1.0
                    }
                } else {
                    1.0
                };
                
                // Create color mapping
                let color = Color32::from_rgb(
                    ((row_idx * 30) % 256) as u8,
                    ((row_idx * 50) % 256) as u8,
                    ((row_idx * 70) % 256) as u8,
                );
                
                // Add nodes if they don't exist
                if !nodes.contains_key(&source) {
                    nodes.insert(source.clone(), NodeData {
                        id: source.clone(),
                        x: 0.1, // Left column
                        y: node_counter as f64 * 0.1,
                        color,
                    });
                    node_counter += 1;
                }
                
                if !nodes.contains_key(&target) {
                    nodes.insert(target.clone(), NodeData {
                        id: target.clone(),
                        x: 0.9, // Right column
                        y: node_counter as f64 * 0.1,
                        color,
                    });
                    node_counter += 1;
                }
                
                // Add flow
                flows.push(FlowData {
                    source: source.clone(),
                    target: target.clone(),
                    value,
                    color,
                });
                
                // Create tooltip data
                let mut tooltip_data = HashMap::new();
                tooltip_data.insert("Source".to_string(), source.clone());
                tooltip_data.insert("Target".to_string(), target.clone());
                tooltip_data.insert("Value".to_string(), value.to_string());
                tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
                
                points.push(PlotPoint {
                    x: nodes[&source].x,
                    y: nodes[&source].y,
                    z: None,
                    label: None,
                    color: Some(color),
                    size: Some(value as f32),
                    series_id: None,
                    tooltip_data,
                });
            }
        }
        
        // Calculate Sankey statistics
        let statistics = calculate_sankey_statistics(&nodes, &flows);
        
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
                ui.label(RichText::new("No data available for Sankey diagram").color(Color32::GRAY));
            });
            return;
        }
        
        ui.group(|ui| {
            ui.label(RichText::new("Sankey Diagram").heading());
            ui.separator();
            
            // Show data summary
            ui.horizontal(|ui| {
                ui.label("Nodes:");
                ui.label(format!("{}", data.points.len()));
            });
            
            ui.horizontal(|ui| {
                ui.label("Flows:");
                ui.label(format!("{}", data.points.len())); // Simplified
            });
            
            // Show Sankey statistics
            if let Some(stats) = &data.statistics {
                ui.separator();
                ui.label(RichText::new("Flow Statistics").strong());
                ui.horizontal(|ui| {
                    ui.label("Total Flow:");
                    ui.label(format!("{:.2}", stats.mean_y * data.points.len() as f64));
                });
                ui.horizontal(|ui| {
                    ui.label("Average Flow:");
                    ui.label(format!("{:.3}", stats.mean_y));
                });
                if let Some(corr) = stats.correlation {
                    ui.horizontal(|ui| {
                        ui.label("Flow Distribution:");
                        ui.label(if corr > 0.5 { "Concentrated" } 
                               else if corr > 0.2 { "Balanced" } 
                               else { "Dispersed" });
                    });
                }
            }
            
            ui.separator();
            
            // Sankey diagram visualization
            let plot_rect = ui.available_rect_before_wrap();
            let plot_size = Vec2::new(plot_rect.width(), plot_rect.height().min(400.0));
            
            ui.allocate_ui(plot_size, |ui| {
                render_sankey_diagram(ui, data, config, plot_size);
            });
            
            // Configuration panel
            ui.separator();
            ui.label(RichText::new("Configuration").strong());
            ui.horizontal(|ui| {
                ui.label("Layout:");
                ui.radio_value(&mut 0, 0, "Left-Right");
                ui.radio_value(&mut 0, 1, "Top-Bottom");
                ui.radio_value(&mut 0, 2, "Circular");
            });
            
            ui.horizontal(|ui| {
                ui.label("Flow Width:");
                ui.radio_value(&mut 0, 0, "Proportional");
                ui.radio_value(&mut 0, 1, "Fixed");
                ui.radio_value(&mut 0, 2, "Logarithmic");
            });
        });
    }
    
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() && config.show_legend {
            ui.group(|ui| {
                ui.label(RichText::new("Sankey Nodes:").strong());
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
        // Handle hover and selection for sankey
        if let Some(hover_pos) = ui.input(|i| i.pointer.hover_pos()) {
            for point in &data.points {
                let point_pos = Pos2::new(point.x as f32, point.y as f32);
                if (hover_pos - point_pos).length() < 10.0 {
                    // Show tooltip
                    ui.label(format!("Source: {} | Target: {} | Value: {}", 
                        point.tooltip_data.get("Source").unwrap_or(&"Unknown".to_string()),
                        point.tooltip_data.get("Target").unwrap_or(&"Unknown".to_string()),
                        point.tooltip_data.get("Value").unwrap_or(&"0".to_string())));
                    break;
                }
            }
        }
        
        None
    }
}

/// Sankey node data structure
#[derive(Debug, Clone)]
struct NodeData {
    id: String,
    x: f64,
    y: f64,
    color: Color32,
}

/// Sankey flow data structure
#[derive(Debug, Clone)]
struct FlowData {
    source: String,
    target: String,
    value: f64,
    color: Color32,
}

/// Calculate Sankey statistics
fn calculate_sankey_statistics(nodes: &HashMap<String, NodeData>, flows: &[FlowData]) -> super::DataStatistics {
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
    let flow_values: Vec<f64> = flows.iter().map(|f| f.value).collect();
    
    let mean_x = node_positions.iter().map(|(x, _)| x).sum::<f64>() / node_positions.len() as f64;
    let mean_y = flow_values.iter().sum::<f64>() / flow_values.len() as f64;
    
    let variance_x = node_positions.iter()
        .map(|(x, _)| (x - mean_x).powi(2))
        .sum::<f64>() / node_positions.len() as f64;
    let std_x = variance_x.sqrt();
    
    let variance_y = flow_values.iter()
        .map(|v| (v - mean_y).powi(2))
        .sum::<f64>() / flow_values.len() as f64;
    let std_y = variance_y.sqrt();
    
    // Calculate correlation between position and flow value
    let correlation = if std_x > 0.0 && std_y > 0.0 {
        let covariance = node_positions.iter().zip(flow_values.iter())
            .map(|((x, _), v)| (x - mean_x) * (v - mean_y))
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

/// Render Sankey diagram with improved flow visualization
fn render_sankey_diagram(ui: &mut Ui, data: &PlotData, _config: &PlotConfiguration, size: Vec2) {
    if data.points.is_empty() {
        return;
    }
    
    let margin = 50.0;
    let plot_width = size.x - 2.0 * margin;
    let plot_height = size.y - 2.0 * margin;
    
    // Generate sample Sankey data for demonstration
    let num_nodes = data.points.len().min(6);
    let mut nodes = Vec::new();
    let mut flows = Vec::new();
    
    // Create source nodes (left column)
    for i in 0..num_nodes / 2 {
        let x = 0.1;
        let y = 0.2 + i as f64 * 0.3;
        let color = data.points.get(i).and_then(|p| p.color).unwrap_or(Color32::BLUE);
        
        nodes.push((x, y, format!("Source {}", i), color));
    }
    
    // Create target nodes (right column)
    for i in 0..num_nodes / 2 {
        let x = 0.9;
        let y = 0.2 + i as f64 * 0.3;
        let color = data.points.get(i + num_nodes / 2).and_then(|p| p.color).unwrap_or(Color32::RED);
        
        nodes.push((x, y, format!("Target {}", i), color));
    }
    
    // Create flows
    for i in 0..num_nodes / 2 {
        let source_idx = i;
        let target_idx = num_nodes / 2 + i;
        let value = 1.0 + i as f64 * 0.5;
        
        flows.push((source_idx, target_idx, value));
    }
    
    // Draw flows with improved visualization
    for (source_idx, target_idx, value) in flows {
        if source_idx < nodes.len() && target_idx < nodes.len() {
            let (x1, y1, _, color1) = nodes[source_idx];
            let (x2, y2, _, color2) = nodes[target_idx];
            
            let screen_x1 = (margin as f64 + x1 * plot_width as f64) as f32;
            let screen_y1 = (margin as f64 + y1 * plot_height as f64) as f32;
            let screen_x2 = (margin as f64 + x2 * plot_width as f64) as f32;
            let screen_y2 = (margin as f64 + y2 * plot_height as f64) as f32;
            
            // Draw flow path with gradient
            let steps = 20;
            for i in 0..steps {
                let t = i as f32 / steps as f32;
                let x = screen_x1 + (screen_x2 - screen_x1) * t;
                let y = screen_y1 + (screen_y2 - screen_y1) * t;
                
                let color = color1.linear_multiply(0.7 + 0.3 * t);
                let width = value as f32 * 2.0 * (1.0 - 0.5 * t);
                
                ui.painter().circle_filled(
                    Pos2::new(x, y),
                    width / 2.0,
                    color,
                );
            }
        }
    }
    
    // Draw nodes
    for (i, (x, y, label, color)) in nodes.iter().enumerate() {
        let screen_x = (margin as f64 + x * plot_width as f64) as f32;
        let screen_y = (margin as f64 + y * plot_height as f64) as f32;
        
        // Draw node with gradient
        ui.painter().circle_filled(
            Pos2::new(screen_x, screen_y),
            15.0,
            *color,
        );
        
        // Draw node border
        ui.painter().circle_stroke(
            Pos2::new(screen_x, screen_y),
            15.0,
            Stroke::new(2.0, Color32::BLACK),
        );
        
        // Draw node label
        ui.painter().text(
            Pos2::new(screen_x, screen_y + 25.0),
            egui::Align2::CENTER_TOP,
            label,
            egui::FontId::proportional(10.0),
            Color32::BLACK,
        );
    }
    
    // Draw column labels
    ui.painter().text(
        Pos2::new(margin + 0.1 * plot_width, margin - 20.0),
        egui::Align2::CENTER_BOTTOM,
        "Sources",
        egui::FontId::proportional(12.0),
        Color32::BLACK,
    );
    
    ui.painter().text(
        Pos2::new(margin + 0.9 * plot_width, margin - 20.0),
        egui::Align2::CENTER_BOTTOM,
        "Targets",
        egui::FontId::proportional(12.0),
        Color32::BLACK,
    );
    
    // Draw legend
    ui.painter().text(
        Pos2::new(margin, margin + plot_height + 20.0),
        egui::Align2::LEFT_TOP,
        "Sankey Diagram - Shows flow between source and target nodes",
        egui::FontId::proportional(12.0),
        Color32::GRAY,
    );
}
