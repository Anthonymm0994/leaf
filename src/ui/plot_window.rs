use egui::{Color32, RichText, Ui, ScrollArea, CollapsingHeader, Grid, ComboBox};
use crate::core::QueryResult;
use crate::ui::plots::{self, PlotType, PlotData, PlotPoint, Plot as PlotTrait, PlotConfiguration, PlotSpecificConfig};
use crate::ui::gpu_renderer::{GpuPlotRenderer, RenderMode};
use datafusion::arrow::datatypes::DataType;

#[derive(Debug, Clone)]
pub struct PlotConfig {
    pub title: String,
    pub plot_type: Option<PlotType>,
    
    // Dynamic column configuration based on plot type
    pub primary_columns: Vec<String>, // X, Y for basic plots
    pub secondary_columns: Vec<String>, // Additional columns like Z, Color, Size, etc.
    pub group_column: Option<String>,
    pub color_column: Option<String>,
    pub size_column: Option<String>,
    
    // Visual settings
    pub show_legend: bool,
    pub show_grid: bool,
    pub color_scheme: plots::ColorScheme,
    
    // GPU rendering options
    pub use_gpu_rendering: bool,
    pub render_mode: RenderMode,
    
    // Plot-specific configuration
    pub plot_specific_config: PlotSpecificConfig,
}

impl Default for PlotConfig {
    fn default() -> Self {
        Self {
            title: String::new(),
            plot_type: None,
            primary_columns: vec![String::new(), String::new()], // X, Y
            secondary_columns: vec![],
            group_column: None,
            color_column: None,
            size_column: None,
            show_legend: true,
            show_grid: true,
            color_scheme: plots::ColorScheme::Viridis,
            use_gpu_rendering: false, // Disable GPU rendering by default for better compatibility
            render_mode: RenderMode::Auto,
            plot_specific_config: PlotSpecificConfig::None,
        }
    }
}

impl PlotConfig {
    /// Get required column count for the selected plot type
    pub fn get_required_column_count(&self) -> (usize, usize) {
        match self.plot_type.as_ref() {
            Some(PlotType::BarChart) => (2, 0), // X (categorical), Y (numeric)
            Some(PlotType::LineChart) => (2, 0), // X (numeric/temporal), Y (numeric)
            Some(PlotType::ScatterPlot) => (2, 0), // X, Y (both numeric)
            Some(PlotType::Histogram) => (1, 0), // X (numeric) only
            Some(PlotType::BoxPlot) => (2, 0), // X (categorical), Y (numeric)
            Some(PlotType::ViolinPlot) => (2, 0), // X (categorical), Y (numeric)
            Some(PlotType::HeatMap) => (3, 0), // X, Y (categorical), Value (numeric)
            Some(PlotType::CorrelationMatrix) => (0, 0), // Multiple columns
            Some(PlotType::Scatter3D) => (3, 0), // X, Y, Z (all numeric)
            Some(PlotType::Surface3D) => (3, 0), // X, Y, Z (all numeric)
            Some(PlotType::ParallelCoordinates) => (0, 0), // Multiple columns
            Some(PlotType::RadarChart) => (0, 0), // Multiple numeric columns
            Some(PlotType::SankeyDiagram) => (3, 0), // Source, Target, Value
            Some(PlotType::NetworkGraph) => (3, 0), // Source, Target, Weight
            Some(PlotType::GeoPlot) => (3, 0), // Lat, Lon, Value
            Some(PlotType::TimeAnalysis) => (2, 0), // Time, Value
            Some(PlotType::CandlestickChart) => (4, 0), // OHLC
            Some(PlotType::StreamGraph) => (2, 0), // X (temporal), Y (numeric)
            Some(PlotType::PolarPlot) => (2, 0), // Angle, Radius
            _ => (2, 0), // Default to X, Y
        }
    }
    
    /// Get column labels for the selected plot type
    pub fn get_column_labels(&self) -> Vec<String> {
        match self.plot_type.as_ref() {
            Some(PlotType::BarChart) => {
                // Check orientation for bar charts
                let is_horizontal = if let PlotSpecificConfig::BarChart(config) = &self.plot_specific_config {
                    matches!(config.orientation, plots::BarOrientation::Horizontal)
                } else {
                    false
                };
                
                if is_horizontal {
                    vec!["Value (X)".to_string(), "Category (Y)".to_string()]
                } else {
                    vec!["Category (X)".to_string(), "Value (Y)".to_string()]
                }
            },
            Some(PlotType::LineChart) => vec!["X Axis".to_string(), "Y Axis".to_string()],
            Some(PlotType::ScatterPlot) => vec!["X Axis".to_string(), "Y Axis".to_string()],
            Some(PlotType::Histogram) => vec!["Value".to_string()],
            Some(PlotType::BoxPlot) => vec!["Category (X)".to_string(), "Value (Y)".to_string()],
            Some(PlotType::ViolinPlot) => vec!["Category (X)".to_string(), "Value (Y)".to_string()],
            Some(PlotType::HeatMap) => vec!["Row (Y)".to_string(), "Column (X)".to_string(), "Value".to_string()],
            Some(PlotType::Scatter3D) => vec!["X Axis".to_string(), "Y Axis".to_string(), "Z Axis".to_string()],
            Some(PlotType::Surface3D) => vec!["X Axis".to_string(), "Y Axis".to_string(), "Z Axis".to_string()],
            Some(PlotType::SankeyDiagram) => vec!["Source".to_string(), "Target".to_string(), "Value".to_string()],
            Some(PlotType::NetworkGraph) => vec!["Source".to_string(), "Target".to_string(), "Weight".to_string()],
            Some(PlotType::GeoPlot) => vec!["Latitude".to_string(), "Longitude".to_string(), "Value".to_string()],
            Some(PlotType::TimeAnalysis) => vec!["Time".to_string(), "Value".to_string()],
            Some(PlotType::CandlestickChart) => vec!["Open".to_string(), "High".to_string(), "Low".to_string(), "Close".to_string()],
            Some(PlotType::StreamGraph) => vec!["Time".to_string(), "Value".to_string()],
            Some(PlotType::PolarPlot) => vec!["Angle".to_string(), "Radius".to_string()],
            _ => vec!["X Axis".to_string(), "Y Axis".to_string()],
        }
    }
    
    /// Get optional column types for the selected plot type
    pub fn get_optional_columns(&self) -> Vec<(&'static str, &'static str)> {
        match self.plot_type.as_ref() {
            Some(PlotType::BarChart) => vec![("Group", "Group data by category"), ("Color", "Color by category")],
            Some(PlotType::LineChart) => vec![("Color", "Color by category"), ("Shape", "Shape by category"), ("Style", "Line style")],
            Some(PlotType::ScatterPlot) => vec![("Color", "Color by value"), ("Size", "Size by value"), ("Shape", "Shape by category")],
            Some(PlotType::BoxPlot) => vec![("Group", "Group by category")],
            Some(PlotType::ViolinPlot) => vec![("Group", "Group by category")],
            Some(PlotType::HeatMap) => vec![("Color Scale", "Color scale type")],
            Some(PlotType::Scatter3D) => vec![("Color", "Color by value"), ("Size", "Size by value")],
            Some(PlotType::Surface3D) => vec![("Interpolation", "Surface interpolation")],
            _ => vec![],
        }
    }
    
    /// Update plot-specific configuration when plot type changes
    pub fn update_plot_specific_config(&mut self) {
        if let Some(plot_type) = &self.plot_type {
            self.plot_specific_config = match plot_type {
                PlotType::BarChart => PlotSpecificConfig::BarChart(plots::BarChartConfig::default()),
                PlotType::LineChart => PlotSpecificConfig::LineChart(plots::LineChartConfig::default()),
                PlotType::ScatterPlot => PlotSpecificConfig::ScatterPlot(plots::ScatterPlotConfig::default()),
                PlotType::Histogram => PlotSpecificConfig::Histogram(plots::HistogramConfig::default()),
                PlotType::BoxPlot => PlotSpecificConfig::BoxPlot(plots::BoxPlotConfig::default()),
                PlotType::ViolinPlot => PlotSpecificConfig::Violin(plots::ViolinPlotConfig::default()),
                _ => PlotSpecificConfig::None,
            };
        }
    }
}

pub struct PlotWindow<'a> {
    pub id: String,
    pub title: String,
    pub config: PlotConfig,
    pub is_config_open: bool,
    pub open: bool,
    data: Option<QueryResult>,
    source_query_id: Option<String>,
    gpu_renderer: Option<GpuPlotRenderer<'a>>,
}

impl<'a> PlotWindow<'a> {
    pub fn new(id: String, title: String) -> Self {
        Self {
            id,
            title,
            config: PlotConfig::default(),
            is_config_open: true,
            open: true,
            data: None,
            source_query_id: None,
            gpu_renderer: None,
        }
    }

    pub fn update_data(&mut self, data: QueryResult) {
        self.data = Some(data);
    }

    pub fn set_source_query(&mut self, query_id: String) {
        self.source_query_id = Some(query_id);
    }

    /// Initialize GPU renderer if available
    pub async fn initialize_gpu_renderer(&mut self) {
        if self.gpu_renderer.is_none() {
            match GpuPlotRenderer::new().await {
                Ok(renderer) => {
                    self.gpu_renderer = Some(renderer);
                    println!("GPU renderer initialized successfully");
                }
                Err(e) => {
                    println!("Failed to initialize GPU renderer: {}", e);
                    self.config.use_gpu_rendering = false;
                    self.config.render_mode = RenderMode::Cpu;
                }
            }
        }
    }

    /// Get GPU capabilities if available
    pub fn get_gpu_capabilities(&self) -> Option<String> {
        self.gpu_renderer.as_ref().and_then(|renderer| {
            renderer.get_capabilities().map(|caps| {
                format!(
                    "Max vertices: {}, Max instances: {}, Instancing: {}, Compute: {}",
                    caps.max_vertices, caps.max_instances, caps.supports_instancing, caps.supports_compute
                )
            })
        })
    }

    /// Reset all column selections when plot type changes
    fn reset_column_selections(&mut self) {
        self.config.primary_columns.clear();
        self.config.secondary_columns.clear();
        self.config.group_column = None;
        self.config.color_column = None;
        self.config.size_column = None;
        
        // Update plot-specific configuration
        self.config.update_plot_specific_config();
    }
    
    /// Update column selections when plot type changes
    fn update_column_selections_for_plot_type(&mut self, new_plot_type: &PlotType) {
        let (primary_count, secondary_count) = self.config.get_required_column_count();
        
        // Ensure primary_columns has the right size
        while self.config.primary_columns.len() < primary_count {
            self.config.primary_columns.push(String::new());
        }
        while self.config.primary_columns.len() > primary_count {
            self.config.primary_columns.pop();
        }
        
        // Ensure secondary_columns has the right size
        while self.config.secondary_columns.len() < secondary_count {
            self.config.secondary_columns.push(String::new());
        }
        while self.config.secondary_columns.len() > secondary_count {
            self.config.secondary_columns.pop();
        }
    }

    /// Check if a column is valid for the current plot type
    fn is_column_valid_for_plot(&self, column_name: &str, plot_type: &PlotType) -> bool {
        if let Some(data) = &self.data {
            if let Some(col_idx) = data.columns.iter().position(|c| c == column_name) {
                if col_idx < data.column_types.len() {
                    let column_type = &data.column_types[col_idx];
                    return plot_type.supports_column_types(Some(column_type), column_type);
                }
            }
        }
        false
    }

    /// Get valid columns for the current plot type
    fn get_valid_columns_for_plot(&self, plot_type: &PlotType) -> Vec<String> {
        if let Some(data) = &self.data {
            data.columns.iter()
                .enumerate()
                .filter_map(|(idx, col_name)| {
                    if idx < data.column_types.len() {
                        let column_type = &data.column_types[idx];
                        if plot_type.supports_column_types(Some(column_type), column_type) {
                            Some(col_name.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![]
        }
    }

    /// Get valid numeric columns for Y axis
    fn get_valid_numeric_columns(&self) -> Vec<String> {
        if let Some(data) = &self.data {
            data.columns.iter()
                .enumerate()
                .filter_map(|(idx, col_name)| {
                    if idx < data.column_types.len() {
                        let column_type = &data.column_types[idx];
                        if plots::is_numeric_type(column_type) {
                            Some(col_name.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![]
        }
    }

    /// Get valid categorical columns for X axis
    fn get_valid_categorical_columns(&self) -> Vec<String> {
        if let Some(data) = &self.data {
            data.columns.iter()
                .enumerate()
                .filter_map(|(idx, col_name)| {
                    if idx < data.column_types.len() {
                        let column_type = &data.column_types[idx];
                        if plots::is_categorical_type(column_type) {
                            Some(col_name.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![]
        }
    }


    pub fn ui(&mut self, ui: &mut Ui) {
        ui.visuals_mut().widgets.noninteractive.bg_fill = egui::Color32::from_gray(18);
        
        // Header with collapsible configuration
        ui.horizontal(|ui| {
            if ui.button(if self.is_config_open { "▼ Plot Configuration" } else { "▶ Plot Configuration" }).clicked() {
                self.is_config_open = !self.is_config_open;
            }
        });

        if self.is_config_open {
            ui.group(|ui| {
                ui.visuals_mut().widgets.noninteractive.bg_fill = egui::Color32::from_gray(22);
                
                // Title
                ui.horizontal(|ui| {
                    ui.label("Title:");
                    ui.text_edit_singleline(&mut self.config.title);
                });
                
                ui.separator();
                
                // Plot Type Selection with categories
                ui.horizontal(|ui| {
                    ui.label("Plot Type:");
                    
                    egui::ComboBox::new("plot_type", "")
                        .selected_text(self.config.plot_type.as_ref().map(|pt| pt.name()).unwrap_or("Select..."))
                        .width(200.0)
                        .show_ui(ui, |ui| {
                            // Show plots by category
                            for (category, plot_types) in PlotType::categories() {
                                ui.label(RichText::new(category).strong());
                                ui.separator();
                                
                                for plot_type in plot_types {
                                    if ui.selectable_value(&mut self.config.plot_type, Some(plot_type.clone()), RichText::new(plot_type.name())).clicked() {
                                        // Reset all column selections when plot type changes
                                        self.reset_column_selections();
                                        // Update column selections for the new plot type
                                        self.update_column_selections_for_plot_type(&plot_type);
                                        // Update plot-specific configuration when plot type changes
                                        self.config.update_plot_specific_config();
                                        ui.close_menu();
                                    }
                                }
                                
                                ui.add_space(5.0);
                            }
                        });
                    
                    ui.add_space(20.0);
                    
                    ui.checkbox(&mut self.config.show_legend, "Show Legend");
                    ui.checkbox(&mut self.config.show_grid, "Show Grid");
                });
                
                ui.separator();
                
                // Dynamic Column Selection based on plot type
                if let Some(data) = &self.data {
                    if let Some(plot_type) = &self.config.plot_type {
                        let plot_type_clone = plot_type.clone();
                        let data_clone = data.clone();
                        self.render_dynamic_column_selection(ui, &data_clone, &plot_type_clone);
                    }
                }
                
                ui.separator();
                
                // Plot-specific configuration
                if let Some(plot_type) = &self.config.plot_type {
                    let plot_type_clone = plot_type.clone();
                    self.render_plot_specific_config(ui, &plot_type_clone);
                }
                
                ui.separator();
                
                // Visual settings
                CollapsingHeader::new("Visual Settings")
                    .default_open(false)
                    .show(ui, |ui| {
                        Grid::new("visual_settings").num_columns(2).spacing([40.0, 4.0]).show(ui, |ui| {
                            ui.label("Color Scheme:");
                            ComboBox::new("color_scheme", "")
                                .selected_text(format!("{:?}", self.config.color_scheme))
                                .show_ui(ui, |ui| {
                                    for scheme in &[
                                        plots::ColorScheme::Viridis,
                                        plots::ColorScheme::Plasma,
                                        plots::ColorScheme::Inferno,
                                        plots::ColorScheme::Magma,
                                    ] {
                                        if ui.selectable_value(&mut self.config.color_scheme, scheme.clone(), format!("{:?}", scheme)).clicked() {
                                            ui.close_menu();
                                        }
                                    }
                                });
                            ui.end_row();
                            
                            ui.label("GPU Rendering:");
                            ui.checkbox(&mut self.config.use_gpu_rendering, "Enable");
                            ui.end_row();
                        });
                    });
            });
        }
        
        // Plot rendering area
        if let Some(data) = &self.data {
            if let Some(plot_type) = &self.config.plot_type {
                let plot_type_clone = plot_type.clone();
                match self.prepare_plot_data(data, &plot_type_clone) {
                    Ok(plot_data) => {
                        self.render_plot(ui, &plot_type_clone, plot_data);
                    },
                    Err(error) => {
                        ui.colored_label(egui::Color32::RED, "Failed to prepare plot data");
                        ui.label(RichText::new(format!("Error: {}", error)).weak());
                        ui.label("Please check your column selections and data types.");
                    }
                }
            } else {
                ui.label("Please select a plot type");
            }
        } else {
            ui.label("No data available");
        }
    }
    
    /// Render dynamic column selection based on plot type
    fn render_dynamic_column_selection(&mut self, ui: &mut Ui, data: &QueryResult, plot_type: &PlotType) {
        let (primary_count, secondary_count) = self.config.get_required_column_count();
        let labels = self.config.get_column_labels();
        
        // Ensure primary_columns has the right size
        while self.config.primary_columns.len() < primary_count {
            self.config.primary_columns.push(String::new());
        }
        while self.config.primary_columns.len() > primary_count {
            self.config.primary_columns.pop();
        }
        
        // Ensure secondary_columns has the right size
        while self.config.secondary_columns.len() < secondary_count {
            self.config.secondary_columns.push(String::new());
        }
        while self.config.secondary_columns.len() > secondary_count {
            self.config.secondary_columns.pop();
        }
        
        // Get valid columns for this plot type
        let valid_columns = self.get_valid_columns_for_plot(plot_type);
        let numeric_columns = self.get_valid_numeric_columns();
        let categorical_columns = self.get_valid_categorical_columns();
        
        // Primary columns (required)
        ui.label(RichText::new("Required Columns:").strong());
        for (i, (label, column)) in labels.iter().take(primary_count).zip(self.config.primary_columns.iter_mut()).enumerate() {
            ui.horizontal(|ui| {
                ui.label(format!("{}:", label));
                let mut column_clone = column.clone();
                
                // Determine which columns to show based on plot type and position
                let available_columns = match plot_type {
                    PlotType::BarChart => {
                        // Check if we have bar chart config to determine orientation
                        let is_horizontal = if let PlotSpecificConfig::BarChart(config) = &self.config.plot_specific_config {
                            matches!(config.orientation, plots::BarOrientation::Horizontal)
                        } else {
                            false
                        };
                        
                        if is_horizontal {
                            // Horizontal: X (numeric), Y (categorical)
                            if i == 0 { &numeric_columns } else { &categorical_columns }
                        } else {
                            // Vertical: X (categorical), Y (numeric)
                            if i == 0 { &categorical_columns } else { &numeric_columns }
                        }
                    },
                    PlotType::Histogram => {
                        &numeric_columns // Only numeric columns for histograms
                    },
                    PlotType::ScatterPlot | PlotType::LineChart => {
                        &numeric_columns // Both X and Y should be numeric
                    },
                    PlotType::BoxPlot | PlotType::ViolinPlot => {
                        if i == 0 { &categorical_columns } else { &numeric_columns }
                    },
                    _ => &valid_columns, // Default to all valid columns
                };
                
                ComboBox::new(format!("primary_col_{}", i), "")
                    .selected_text(if column_clone.is_empty() { "Select..." } else { &column_clone })
                    .show_ui(ui, |ui| {
                        for col in available_columns {
                            if ui.selectable_value(&mut column_clone, col.clone(), col).clicked() {
                                ui.close_menu();
                            }
                        }
                    });
                *column = column_clone;
            });
        }
        
        // Secondary columns (optional)
        if secondary_count > 0 {
            ui.separator();
            ui.label(RichText::new("Secondary Columns:").strong());
            for i in 0..secondary_count {
                ui.horizontal(|ui| {
                    ui.label(format!("Column {}:", i + 1));
                    let mut col = self.config.secondary_columns.get(i).cloned().unwrap_or_default();
                    ComboBox::new(format!("secondary_col_{}", i), "")
                        .selected_text(if col.is_empty() { "Select..." } else { &col })
                        .show_ui(ui, |ui| {
                            if ui.selectable_value(&mut col, String::new(), "None").clicked() {
                                ui.close_menu();
                            }
                            for data_col in &valid_columns {
                                if ui.selectable_value(&mut col, data_col.clone(), data_col).clicked() {
                                    ui.close_menu();
                                }
                            }
                        });
                    
                    // Update secondary_columns
                    while self.config.secondary_columns.len() <= i {
                        self.config.secondary_columns.push(String::new());
                    }
                    self.config.secondary_columns[i] = col;
                });
            }
        }
        
        // Optional columns based on plot type
        let optional_columns = self.config.get_optional_columns();
        if !optional_columns.is_empty() {
            ui.separator();
            ui.label(RichText::new("Optional Columns:").strong());
            for (col_type, description) in optional_columns {
                ui.horizontal(|ui| {
                    ui.label(format!("{}:", col_type));
                    let mut col = match col_type {
                        "Group" => self.config.group_column.clone().unwrap_or_default(),
                        "Color" => self.config.color_column.clone().unwrap_or_default(),
                        "Size" => self.config.size_column.clone().unwrap_or_default(),
                        _ => String::new(),
                    };
                    
                    // Determine which columns to show for optional columns
                    let available_optional_columns = match col_type {
                        "Group" => &categorical_columns,
                        "Color" => &categorical_columns,
                        "Size" => &numeric_columns,
                        _ => &valid_columns,
                    };
                    
                    ComboBox::new(format!("optional_{}", col_type), "")
                        .selected_text(if col.is_empty() { "Select..." } else { &col })
                        .show_ui(ui, |ui| {
                            if ui.selectable_value(&mut col, String::new(), "None").clicked() {
                                ui.close_menu();
                            }
                            for data_col in available_optional_columns {
                                if ui.selectable_value(&mut col, data_col.clone(), data_col).clicked() {
                                    ui.close_menu();
                                }
                            }
                        });
                    
                    // Update the appropriate field
                    match col_type {
                        "Group" => self.config.group_column = if col.is_empty() { None } else { Some(col) },
                        "Color" => self.config.color_column = if col.is_empty() { None } else { Some(col) },
                        "Size" => self.config.size_column = if col.is_empty() { None } else { Some(col) },
                        _ => {},
                    }
                    
                    ui.label(RichText::new(description).weak());
                });
            }
        }
        
        // Show validation errors if any
        if let Some(data) = &self.data {
            if let Err(validation_error) = self.validate_columns_with_data(plot_type, data) {
                ui.separator();
                ui.colored_label(egui::Color32::RED, format!("⚠ {}", validation_error));
            }
        }
    }
    
    /// Render plot-specific configuration options
    fn render_plot_specific_config(&mut self, ui: &mut Ui, plot_type: &PlotType) {
        CollapsingHeader::new("Plot-Specific Settings")
            .default_open(false)
            .show(ui, |ui| {
                match plot_type {
                    PlotType::BarChart => self.render_bar_chart_config(ui),
                    PlotType::LineChart => self.render_line_chart_config(ui),
                    PlotType::ScatterPlot => self.render_scatter_plot_config(ui),
                    PlotType::Histogram => self.render_histogram_config(ui),
                    PlotType::BoxPlot => self.render_box_plot_config(ui),
                    PlotType::ViolinPlot => self.render_violin_plot_config(ui),
                    _ => {
                        ui.label("No specific configuration available for this plot type");
                    }
                }
            });
    }
    
    fn render_bar_chart_config(&mut self, ui: &mut Ui) {
        if let PlotSpecificConfig::BarChart(config) = &mut self.config.plot_specific_config {
            Grid::new("bar_chart_config").num_columns(2).spacing([40.0, 4.0]).show(ui, |ui| {
                ui.label("Bar Width:");
                ui.add(egui::Slider::new(&mut config.bar_width, 0.1..=2.0));
                ui.end_row();
                
                ui.label("Group Spacing:");
                ui.add(egui::Slider::new(&mut config.group_spacing, 0.0..=1.0));
                ui.end_row();
                
                ui.label("Orientation:");
                ComboBox::new("orientation", "")
                    .selected_text(format!("{:?}", config.orientation))
                    .show_ui(ui, |ui| {
                        for orientation in &[plots::BarOrientation::Vertical, plots::BarOrientation::Horizontal] {
                            if ui.selectable_value(&mut config.orientation, orientation.clone(), format!("{:?}", orientation)).clicked() {
                                ui.close_menu();
                            }
                        }
                    });
                ui.end_row();
                
                ui.label("Stacking Mode:");
                ComboBox::new("stacking_mode", "")
                    .selected_text(format!("{:?}", config.stacking_mode))
                    .show_ui(ui, |ui| {
                        for mode in &[plots::StackingMode::None, plots::StackingMode::Stacked, plots::StackingMode::Percent] {
                            if ui.selectable_value(&mut config.stacking_mode, mode.clone(), format!("{:?}", mode)).clicked() {
                                ui.close_menu();
                            }
                        }
                    });
                ui.end_row();
                
                ui.label("Sort Order:");
                ComboBox::new("sort_order", "")
                    .selected_text(format!("{:?}", config.sort_order))
                    .show_ui(ui, |ui| {
                        for order in &[plots::SortOrder::None, plots::SortOrder::Ascending, plots::SortOrder::Descending, plots::SortOrder::ByValue] {
                            if ui.selectable_value(&mut config.sort_order, order.clone(), format!("{:?}", order)).clicked() {
                                ui.close_menu();
                            }
                        }
                    });
                ui.end_row();
            });
        }
    }
    
    fn render_line_chart_config(&mut self, ui: &mut Ui) {
        if let PlotSpecificConfig::LineChart(config) = &mut self.config.plot_specific_config {
            // Add note about GPU rendering for line charts
            ui.colored_label(egui::Color32::from_rgb(255, 200, 100), 
                "ℹ Line charts use CPU rendering for better interaction support");
            ui.separator();
            
            Grid::new("line_chart_config").num_columns(2).spacing([40.0, 4.0]).show(ui, |ui| {
                ui.label("Line Style:");
                ComboBox::new("line_style", "")
                    .selected_text(format!("{:?}", config.line_style))
                    .show_ui(ui, |ui| {
                        for style in &[plots::LineStyle::Solid, plots::LineStyle::Dashed, plots::LineStyle::Dotted, plots::LineStyle::DashDot] {
                            if ui.selectable_value(&mut config.line_style, style.clone(), format!("{:?}", style)).clicked() {
                                ui.close_menu();
                            }
                        }
                    });
                ui.end_row();
                
                ui.label("Show Points:");
                ui.checkbox(&mut config.show_points, "");
                ui.end_row();
                
                ui.label("Smooth Lines:");
                ui.checkbox(&mut config.smooth_lines, "");
                ui.end_row();
                
                ui.label("Fill Area:");
                ui.checkbox(&mut config.fill_area, "");
                ui.end_row();
                
                ui.label("Point Shapes:");
                ui.label("● ■ ◆ ▲ ✚ ➕ ★");
                ui.end_row();
            });
        }
    }
    
    fn render_scatter_plot_config(&mut self, ui: &mut Ui) {
        if let PlotSpecificConfig::ScatterPlot(config) = &mut self.config.plot_specific_config {
            Grid::new("scatter_plot_config").num_columns(2).spacing([40.0, 4.0]).show(ui, |ui| {
                ui.label("Point Shape:");
                ComboBox::new("point_shape", "")
                    .selected_text(format!("{:?}", config.point_shape))
                    .show_ui(ui, |ui| {
                        for shape in &[plots::MarkerShape::Circle, plots::MarkerShape::Square, plots::MarkerShape::Diamond, plots::MarkerShape::Triangle] {
                            if ui.selectable_value(&mut config.point_shape, shape.clone(), format!("{:?}", shape)).clicked() {
                                ui.close_menu();
                            }
                        }
                    });
                ui.end_row();
                
                ui.label("Show Trend Line:");
                ui.checkbox(&mut config.show_trend_line, "");
                ui.end_row();
                
                ui.label("Show Density:");
                ui.checkbox(&mut config.show_density, "");
                ui.end_row();
                
                ui.label("Jitter Amount:");
                ui.add(egui::Slider::new(&mut config.jitter_amount, 0.0..=0.5));
                ui.end_row();
            });
        }
    }
    
    fn render_histogram_config(&mut self, ui: &mut Ui) {
        if let PlotSpecificConfig::Histogram(config) = &mut self.config.plot_specific_config {
            Grid::new("histogram_config").num_columns(2).spacing([40.0, 4.0]).show(ui, |ui| {
                ui.label("Bin Count:");
                let mut bin_count = config.bin_count.unwrap_or(20);
                if ui.add(egui::Slider::new(&mut bin_count, 5..=100)).changed() {
                    config.bin_count = Some(bin_count);
                }
                ui.end_row();
                
                ui.label("Show Density:");
                ui.checkbox(&mut config.show_density, "");
                ui.end_row();
                
                ui.label("Show Normal Curve:");
                ui.checkbox(&mut config.show_normal_curve, "");
                ui.end_row();
            });
        }
    }
    
    fn render_box_plot_config(&mut self, ui: &mut Ui) {
        if let PlotSpecificConfig::BoxPlot(config) = &mut self.config.plot_specific_config {
            Grid::new("box_plot_config").num_columns(2).spacing([40.0, 4.0]).show(ui, |ui| {
                ui.label("Show Outliers:");
                ui.checkbox(&mut config.show_outliers, "");
                ui.end_row();
                
                ui.label("Show Mean:");
                ui.checkbox(&mut config.show_mean, "");
                ui.end_row();
                
                ui.label("Notched:");
                ui.checkbox(&mut config.notched, "");
                ui.end_row();
                
                ui.label("Violin Overlay:");
                ui.checkbox(&mut config.violin_overlay, "");
                ui.end_row();
            });
        }
    }
    
    fn render_violin_plot_config(&mut self, ui: &mut Ui) {
        if let PlotSpecificConfig::Violin(config) = &mut self.config.plot_specific_config {
            Grid::new("violin_plot_config").num_columns(2).spacing([40.0, 4.0]).show(ui, |ui| {
                ui.label("Show Box Plot:");
                ui.checkbox(&mut config.show_box_plot, "");
                ui.end_row();
                
                ui.label("Show Mean:");
                ui.checkbox(&mut config.show_mean, "");
                ui.end_row();
                
                ui.label("Show Median:");
                ui.checkbox(&mut config.show_median, "");
                ui.end_row();
                
                ui.label("Show Quartiles:");
                ui.checkbox(&mut config.show_quartiles, "");
                ui.end_row();
                
                ui.label("Show Outliers:");
                ui.checkbox(&mut config.show_outliers, "");
                ui.end_row();
                
                ui.label("Violin Width:");
                ui.add(egui::Slider::new(&mut config.violin_width, 0.1..=2.0));
                ui.end_row();
                
                ui.label("Bandwidth:");
                ui.add(egui::Slider::new(&mut config.bandwidth, 0.1..=2.0));
                ui.end_row();
            });
        }
    }
    

    
    fn validate_columns_with_data(&self, plot_type: &PlotType, data: &QueryResult) -> Result<(), String> {
            let (primary_count, secondary_count) = self.config.get_required_column_count();
            
            // Validate primary columns are selected
            for i in 0..primary_count {
                if i >= self.config.primary_columns.len() || self.config.primary_columns[i].is_empty() {
                    return Err(format!("Primary column {} is required", i + 1));
                }
                
                // Check if the selected column exists in the data
                if !data.columns.iter().any(|c| c == &self.config.primary_columns[i]) {
                    return Err(format!("Primary column '{}' not found in data", self.config.primary_columns[i]));
                }
                
                // Check if the column type is valid for this plot type
                if let Some(col_idx) = data.columns.iter().position(|c| c == &self.config.primary_columns[i]) {
                    if col_idx < data.column_types.len() {
                        let column_type = &data.column_types[col_idx];
                        
                        // Special handling for bar charts with orientation
                        let is_valid = if let PlotType::BarChart = plot_type {
                            if let PlotSpecificConfig::BarChart(config) = &self.config.plot_specific_config {
                                let is_horizontal = matches!(config.orientation, plots::BarOrientation::Horizontal);
                                if is_horizontal {
                                    // Horizontal: X (numeric), Y (categorical)
                                    if i == 0 {
                                        plots::is_numeric_type(column_type)
                                    } else {
                                        plots::is_categorical_type(column_type)
                                    }
                                } else {
                                    // Vertical: X (categorical), Y (numeric)
                                    if i == 0 {
                                        plots::is_categorical_type(column_type)
                                    } else {
                                        plots::is_numeric_type(column_type)
                                    }
                                }
                            } else {
                                // Default to vertical if no config
                                if i == 0 {
                                    plots::is_categorical_type(column_type)
                                } else {
                                    plots::is_numeric_type(column_type)
                                }
                            }
                        } else {
                            plot_type.supports_column_types(Some(column_type), column_type)
                        };
                        
                        if !is_valid {
                            let expected_type = if let PlotType::BarChart = plot_type {
                                if let PlotSpecificConfig::BarChart(config) = &self.config.plot_specific_config {
                                    let is_horizontal = matches!(config.orientation, plots::BarOrientation::Horizontal);
                                    if is_horizontal {
                                        if i == 0 { "numeric" } else { "categorical" }
                                    } else {
                                        if i == 0 { "categorical" } else { "numeric" }
                                    }
                                } else {
                                    if i == 0 { "categorical" } else { "numeric" }
                                }
                            } else {
                                "compatible"
                            };
                            
                            let error_msg = if let PlotType::BoxPlot = plot_type {
                                if i == 0 {
                                    format!("Column '{}' has type {:?}. Box plots expect categorical data (strings) for grouping on the X-axis.", 
                                        self.config.primary_columns[i], column_type)
                                } else {
                                    format!("Column '{}' has type {:?}. Box plots expect numeric data for the Y-axis.", 
                                        self.config.primary_columns[i], column_type)
                                }
                            } else if let PlotType::ViolinPlot = plot_type {
                                if i == 0 {
                                    format!("Column '{}' has type {:?}. Violin plots expect categorical data (strings) for grouping on the X-axis.", 
                                        self.config.primary_columns[i], column_type)
                                } else {
                                    format!("Column '{}' has type {:?}. Violin plots expect numeric data for the Y-axis.", 
                                        self.config.primary_columns[i], column_type)
                                }
                            } else if let PlotType::PolarPlot = plot_type {
                                if i == 0 {
                                    format!("Column '{}' has type {:?}. Polar plots expect numeric data for the angle (X-axis).", 
                                        self.config.primary_columns[i], column_type)
                                } else {
                                    format!("Column '{}' has type {:?}. Polar plots expect numeric data for the radius (Y-axis).", 
                                        self.config.primary_columns[i], column_type)
                                }
                            } else {
                                format!("Column '{}' has type {:?} which is not valid for {} plot (expected {} for position {})", 
                                    self.config.primary_columns[i], column_type, plot_type.name(), expected_type, i + 1)
                            };
                            return Err(error_msg);
                        }
                    }
                }
            }
            
            // Validate secondary columns (if selected)
            for i in 0..secondary_count {
                if i < self.config.secondary_columns.len() && !self.config.secondary_columns[i].is_empty() {
                    if !data.columns.iter().any(|c| c == &self.config.secondary_columns[i]) {
                        return Err(format!("Secondary column '{}' not found in data", self.config.secondary_columns[i]));
                    }
                    
                    // Check if the column type is valid
                    if let Some(col_idx) = data.columns.iter().position(|c| c == &self.config.secondary_columns[i]) {
                        if col_idx < data.column_types.len() {
                            let column_type = &data.column_types[col_idx];
                            if !plot_type.supports_column_types(Some(column_type), column_type) {
                                return Err(format!("Secondary column '{}' has type {:?} which is not valid for {} plot", 
                                    self.config.secondary_columns[i], column_type, plot_type.name()));
                            }
                        }
                    }
                }
            }
            
            // Validate optional columns (if selected)
            if let Some(group_col) = &self.config.group_column {
                if !data.columns.iter().any(|c| c == group_col) {
                    return Err(format!("Group column '{}' not found in data", group_col));
                }
                
                // Check if group column is categorical
                if let Some(col_idx) = data.columns.iter().position(|c| c == group_col) {
                    if col_idx < data.column_types.len() {
                        let column_type = &data.column_types[col_idx];
                        if !plots::is_categorical_type(column_type) {
                            return Err(format!("Group column '{}' must be categorical (string), but has type {:?}", 
                                group_col, column_type));
                        }
                    }
                }
            }
            
            if let Some(color_col) = &self.config.color_column {
                if !data.columns.iter().any(|c| c == color_col) {
                    return Err(format!("Color column '{}' not found in data", color_col));
                }
                
                // Check if color column is categorical
                if let Some(col_idx) = data.columns.iter().position(|c| c == color_col) {
                    if col_idx < data.column_types.len() {
                        let column_type = &data.column_types[col_idx];
                        if !plots::is_categorical_type(column_type) {
                            return Err(format!("Color column '{}' must be categorical (string), but has type {:?}", 
                                color_col, column_type));
                        }
                    }
                }
            }
            
            if let Some(size_col) = &self.config.size_column {
                if !data.columns.iter().any(|c| c == size_col) {
                    return Err(format!("Size column '{}' not found in data", size_col));
                }
                
                // Check if size column is numeric
                if let Some(col_idx) = data.columns.iter().position(|c| c == size_col) {
                    if col_idx < data.column_types.len() {
                        let column_type = &data.column_types[col_idx];
                        if !plots::is_numeric_type(column_type) {
                            return Err(format!("Size column '{}' must be numeric, but has type {:?}", 
                                size_col, column_type));
                        }
                    }
                }
        }
        Ok(())
    }
    
    fn prepare_plot_data(&self, data: &QueryResult, plot_type: &PlotType) -> Result<PlotData, String> {
        // Validate columns against the actual data being processed
        self.validate_columns_with_data(plot_type, data)?;
        
        // Create a plot configuration from the current settings
        let plot_config = plots::PlotConfiguration {
            title: self.config.title.clone(),
            x_column: self.config.primary_columns.get(0).cloned().unwrap_or_default(),
            y_column: self.config.primary_columns.get(1).cloned().unwrap_or_default(),
            color_column: self.config.color_column.clone(),
            size_column: self.config.size_column.clone(),
            group_column: self.config.group_column.clone(),
            show_legend: self.config.show_legend,
            show_grid: self.config.show_grid,
            show_axes_labels: true,
            color_scheme: self.config.color_scheme.clone(),
            marker_size: 4.0,
            line_width: 2.0,
            allow_zoom: true,
            allow_pan: true,
            allow_selection: true,
            show_tooltips: true,
            plot_specific: self.config.plot_specific_config.clone(),
        };
        
        // Use the plot-specific prepare_data method
        match plot_type {
            plots::PlotType::BarChart => plots::bar::BarChartPlot.prepare_data(data, &plot_config),
            plots::PlotType::LineChart => plots::line::LineChartPlot.prepare_data(data, &plot_config),
            plots::PlotType::ScatterPlot => plots::scatter::ScatterPlot.prepare_data(data, &plot_config),
            plots::PlotType::Histogram => plots::histogram::HistogramPlot.prepare_data(data, &plot_config),
            plots::PlotType::BoxPlot => plots::box_plot::BoxPlotImpl.prepare_data(data, &plot_config),
            plots::PlotType::HeatMap => plots::heatmap::HeatmapPlot.prepare_data(data, &plot_config),
            plots::PlotType::ViolinPlot => plots::violin::ViolinPlot::new().prepare_data(data, &plot_config),
            plots::PlotType::AnomalyDetection => plots::anomaly::AnomalyPlot.prepare_data(data, &plot_config),
            plots::PlotType::CorrelationMatrix => plots::correlation::CorrelationPlot.prepare_data(data, &plot_config),
    
            plots::PlotType::Scatter3D => plots::scatter3d::Scatter3DPlot.prepare_data(data, &plot_config),
            plots::PlotType::Surface3D => plots::surface3d::Surface3dPlot.prepare_data(data, &plot_config),
            plots::PlotType::ContourPlot => plots::contour::ContourPlot.prepare_data(data, &plot_config),
            plots::PlotType::ParallelCoordinates => plots::parallel_coordinates::ParallelCoordinatesPlot.prepare_data(data, &plot_config),
            plots::PlotType::RadarChart => plots::radar::RadarPlot.prepare_data(data, &plot_config),
            plots::PlotType::SankeyDiagram => plots::sankey::SankeyPlot.prepare_data(data, &plot_config),
            plots::PlotType::Treemap => plots::treemap::TreemapPlot.prepare_data(data, &plot_config),
            plots::PlotType::SunburstChart => plots::sunburst::SunburstPlot.prepare_data(data, &plot_config),
            plots::PlotType::NetworkGraph => plots::network::NetworkPlot.prepare_data(data, &plot_config),
            plots::PlotType::GeoPlot => plots::geo::GeoPlot.prepare_data(data, &plot_config),
            plots::PlotType::TimeAnalysis => plots::time_analysis::TimeAnalysisPlot.prepare_data(data, &plot_config),
            plots::PlotType::CandlestickChart => plots::candlestick::CandlestickPlot.prepare_data(data, &plot_config),
            plots::PlotType::StreamGraph => plots::stream::StreamPlot.prepare_data(data, &plot_config),
            plots::PlotType::PolarPlot => plots::polar::PolarPlot.prepare_data(data, &plot_config),
        }
    }
    
    fn extract_plot_points(&self, data: &QueryResult) -> Result<Vec<PlotPoint>, String> {
        let (primary_count, _) = self.config.get_required_column_count();
        
        // For plots that need a Y column (most plots)
        if primary_count > 1 && self.config.primary_columns.len() > 1 {
            if self.config.primary_columns[1].is_empty() {
                return Err("Y column not selected".to_string());
            }

            let y_idx = data.columns.iter().position(|c| c == &self.config.primary_columns[1])
                .ok_or("Y column not found")?;
            
            let x_idx = if !self.config.primary_columns[0].is_empty() {
                data.columns.iter().position(|c| c == &self.config.primary_columns[0])
            } else {
                None
            };

            let mut points = Vec::new();
            
            for (row_idx, row) in data.rows.iter().enumerate() {
                if row.len() > y_idx {
                    let y_val = row[y_idx].parse::<f64>()
                        .map_err(|_| format!("Failed to parse Y value '{}' as number", row[y_idx]))?;
                    
                    let x_val = if let Some(x_idx) = x_idx {
                        if row.len() > x_idx {
                            row[x_idx].parse::<f64>().unwrap_or(row_idx as f64)
                        } else {
                            row_idx as f64
                        }
                    } else {
                        row_idx as f64
                    };
                    
                    // Create tooltip data
                    let mut tooltip_data = std::collections::HashMap::new();
                    tooltip_data.insert("X".to_string(), x_val.to_string());
                    tooltip_data.insert("Y".to_string(), y_val.to_string());
                    
                    points.push(PlotPoint {
                        x: x_val,
                        y: y_val,
                        z: None,
                        label: None,
                        color: None,
                        size: None,
                        series_id: None,
                        tooltip_data,
                    });
                }
            }
            
            Ok(points)
        } else {
            // For plots that only need one column (like histograms)
            if self.config.primary_columns.is_empty() || self.config.primary_columns[0].is_empty() {
                return Err("Primary column not selected".to_string());
            }
            
            let col_idx = data.columns.iter().position(|c| c == &self.config.primary_columns[0])
                .ok_or("Primary column not found")?;
            
            let mut points = Vec::new();
            
            for (row_idx, row) in data.rows.iter().enumerate() {
                if row.len() > col_idx {
                    let val = row[col_idx].parse::<f64>()
                        .map_err(|_| format!("Failed to parse value '{}' as number", row[col_idx]))?;
                    
                    // Create tooltip data
                    let mut tooltip_data = std::collections::HashMap::new();
                    tooltip_data.insert("Value".to_string(), val.to_string());
                    
                    points.push(PlotPoint {
                        x: row_idx as f64,
                        y: val,
                        z: None,
                        label: None,
                        color: None,
                        size: None,
                        series_id: None,
                        tooltip_data,
                    });
                }
            }
            
            Ok(points)
        }
    }
    
    fn render_plot(&mut self, ui: &mut Ui, plot_type: &PlotType, plot_data: PlotData) {
        // Create a plot configuration from the current settings with proper plot-specific config
        let plot_config = plots::PlotConfiguration {
            title: self.config.title.clone(),
            x_column: self.config.primary_columns.get(0).cloned().unwrap_or_default(),
            y_column: self.config.primary_columns.get(1).cloned().unwrap_or_default(),
            color_column: self.config.color_column.clone(),
            size_column: self.config.size_column.clone(),
            group_column: self.config.group_column.clone(),
            show_legend: self.config.show_legend,
            show_grid: self.config.show_grid,
            show_axes_labels: true,
            color_scheme: self.config.color_scheme.clone(),
            marker_size: 4.0,
            line_width: 2.0,
            allow_zoom: true,
            allow_pan: true,
            allow_selection: true,
            show_tooltips: true,
            plot_specific: self.config.plot_specific_config.clone(),
        };
        
        // Prepare GPU rendering data if needed
        let gpu_points = if self.config.use_gpu_rendering && matches!(plot_type, PlotType::LineChart) {
            if let Some(data_ref) = self.data.as_ref() {
                self.extract_plot_points(data_ref).ok()
            } else {
                None
            }
        } else {
            None
        };
        
        // Try GPU rendering if enabled and available
        if self.config.use_gpu_rendering {
            if let (Some(ref mut gpu_renderer), Some(points)) = (self.gpu_renderer.as_mut(), gpu_points) {
                // For line charts, always use CPU rendering to maintain proper interactions
                if matches!(plot_type, PlotType::LineChart) {
                    // Fall through to CPU rendering for line charts
                } else if let Ok(()) = gpu_renderer.render_line_chart(
                    &points.iter().map(|p| egui::Pos2::new(p.x as f32, p.y as f32)).collect::<Vec<_>>(),
                    egui::Color32::BLUE,
                    2.0
                ) {
                    // GPU rendering successful for non-line charts, return early
                    return;
                }
            }
        }
        
        // Fall back to CPU rendering with scroll area
        ScrollArea::vertical().show(ui, |ui| {
            use PlotType::*;
            
            match plot_type {
                BarChart => {
                    plots::bar::BarChartPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::bar::BarChartPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                LineChart => {
                    plots::line::LineChartPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::line::LineChartPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                ScatterPlot => {
                    plots::scatter::ScatterPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::scatter::ScatterPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                Histogram => {
                    plots::histogram::HistogramPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::histogram::HistogramPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                BoxPlot => {
                    plots::box_plot::BoxPlotImpl.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::box_plot::BoxPlotImpl.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                
                // These will show "coming soon" messages for now
                HeatMap => {
                    plots::heatmap::HeatmapPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::heatmap::HeatmapPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                ViolinPlot => {
                    plots::violin::ViolinPlot::new().render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::violin::ViolinPlot::new().render_legend(ui, &plot_data, &plot_config);
                    }
                },
                AnomalyDetection => {
                    plots::anomaly::AnomalyPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::anomaly::AnomalyPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                CorrelationMatrix => {
                    plots::correlation::CorrelationPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::correlation::CorrelationPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
    
                Scatter3D => {
                    plots::scatter3d::Scatter3DPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::scatter3d::Scatter3DPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                Surface3D => {
                    plots::surface3d::Surface3dPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::surface3d::Surface3dPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                ContourPlot => {
                    plots::contour::ContourPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::contour::ContourPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                ParallelCoordinates => {
                    plots::parallel_coordinates::ParallelCoordinatesPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::parallel_coordinates::ParallelCoordinatesPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                RadarChart => {
                    plots::radar::RadarPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::radar::RadarPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                SankeyDiagram => {
                    plots::sankey::SankeyPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::sankey::SankeyPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                Treemap => {
                    plots::treemap::TreemapPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::treemap::TreemapPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                SunburstChart => {
                    plots::sunburst::SunburstPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::sunburst::SunburstPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                NetworkGraph => {
                    plots::network::NetworkPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::network::NetworkPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                GeoPlot => {
                    plots::geo::GeoPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::geo::GeoPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                TimeAnalysis => {
                    plots::time_analysis::TimeAnalysisPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::time_analysis::TimeAnalysisPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                CandlestickChart => {
                    plots::candlestick::CandlestickPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::candlestick::CandlestickPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                StreamGraph => {
                    plots::stream::StreamPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::stream::StreamPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
                PolarPlot => {
                    plots::polar::PolarPlot.render(ui, &plot_data, &plot_config);
                    if plot_config.show_legend {
                        plots::polar::PolarPlot.render_legend(ui, &plot_data, &plot_config);
                    }
                },
            }
        });
    }
}

fn format_data_type(dtype: &DataType) -> &'static str {
    use DataType::*;
    match dtype {
        Int8 | Int16 | Int32 | Int64 => "Integer",
        UInt8 | UInt16 | UInt32 | UInt64 => "Unsigned Int",
        Float16 | Float32 | Float64 => "Float",
        Utf8 | LargeUtf8 => "Text",
        Boolean => "Boolean",
        Date32 | Date64 => "Date",
        Timestamp(_, _) => "Timestamp",
        _ => "Other",
    }
} 

