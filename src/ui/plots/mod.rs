//! Plot implementations module
//! 
//! This module contains all the plot implementations ported from frog-viz,
//! with proper column type validation using DataFusion's type system.

use datafusion::arrow::datatypes::DataType;
use egui::{Ui, Color32, RichText};
use crate::core::QueryResult;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

// Import all plot modules
pub mod bar;
pub mod line;
pub mod scatter;
pub mod histogram;
pub mod box_plot;
pub mod heatmap;
pub mod violin;
pub mod anomaly;
pub mod correlation;

pub mod scatter3d;
pub mod surface3d;
pub mod contour;
pub mod parallel_coordinates;
pub mod radar;
pub mod sankey;
pub mod treemap;
pub mod sunburst;
pub mod network;
pub mod geo;
pub mod time_analysis;
pub mod candlestick;
pub mod stream;
pub mod polar;

// DataFusion integration layer
pub mod data_processor;

// Enhanced utilities based on frog-viz patterns
pub mod utils;
pub mod enhanced_config;

// Re-export enhanced utilities
pub use utils::{categorical_color, viridis_color, plasma_color, diverging_color};
pub use utils::{calculate_quartiles, detect_outliers_iqr, zscore_outliers, calculate_statistics};
pub use utils::{extract_numeric_values, extract_string_values, extract_temporal_values};
pub use enhanced_config::{EnhancedPlotConfig, ColorScheme};

// Enhanced Plot trait with advanced functionality
pub trait Plot {
    /// Get the name of the plot type
    fn name(&self) -> &'static str;
    
    /// Get required column types for X axis (None means no X column required)
    fn required_x_types(&self) -> Option<Vec<DataType>>;
    
    /// Get required column types for Y axis
    fn required_y_types(&self) -> Vec<DataType>;
    
    /// Get optional column types for additional dimensions (e.g., size, color)
    fn optional_column_types(&self) -> Vec<(&'static str, Vec<DataType>)> {
        vec![]
    }
    
    /// Check if this plot supports multiple data series
    fn supports_multiple_series(&self) -> bool { false }
    
    /// Check if this plot supports color mapping
    fn supports_color_mapping(&self) -> bool { false }
    
    /// Check if this plot supports size mapping
    fn supports_size_mapping(&self) -> bool { false }
    
    /// Check if this plot supports interactive selection
    fn supports_interactive_selection(&self) -> bool { true }
    
    /// Get default configuration for this plot type
    fn get_default_config(&self) -> PlotConfiguration {
        PlotConfiguration {
            title: String::new(),
            x_column: String::new(),
            y_column: String::new(),
            color_column: None,
            size_column: None,
            group_column: None,
            show_legend: true,
            show_grid: true,
            show_axes_labels: true,
            color_scheme: ColorScheme::Viridis,
            marker_size: 4.0,
            line_width: 2.0,
            allow_zoom: true,
            allow_pan: true,
            allow_selection: true,
            show_tooltips: true,
            plot_specific: PlotSpecificConfig::None,
        }
    }
    
    /// Validate if the selected columns are appropriate for this plot type
    fn validate_columns(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<(), String> {
        // Enhanced validation logic
        if let Some(required_x_types) = self.required_x_types() {
            if config.x_column.is_empty() {
                return Err("X column is required for this plot type".to_string());
            }
            
            // Check actual column type against required types
            if let Some(x_idx) = query_result.columns.iter().position(|c| c == &config.x_column) {
                if x_idx < query_result.column_types.len() {
                    let actual_type = &query_result.column_types[x_idx];
                    if !required_x_types.iter().any(|req_type| req_type == actual_type) {
                        return Err(format!(
                            "X column '{}' has type {:?} which is not valid for {} plot. Required types: {:?}",
                            config.x_column, actual_type, self.name(), required_x_types
                        ));
                    }
                }
            }
        } else {
            // No X column required, but if one is provided, validate it's a valid column
            if !config.x_column.is_empty() {
                if !query_result.columns.contains(&config.x_column) {
                    return Err(format!("X column '{}' not found in data", config.x_column));
                }
            }
        }
        
        if config.y_column.is_empty() {
            return Err("Y column is required for this plot type".to_string());
        }
        
        // Check Y column type against required types
        let required_y_types = self.required_y_types();
        if let Some(y_idx) = query_result.columns.iter().position(|c| c == &config.y_column) {
            if y_idx < query_result.column_types.len() {
                let actual_type = &query_result.column_types[y_idx];
                if !required_y_types.iter().any(|req_type| req_type == actual_type) {
                    return Err(format!(
                        "Y column '{}' has type {:?} which is not valid for {} plot. Required types: {:?}",
                        config.y_column, actual_type, self.name(), required_y_types
                    ));
                }
            }
        }
        
        // Validate optional columns
        if let Some(color_col) = &config.color_column {
            if !color_col.is_empty() && !query_result.columns.contains(color_col) {
                return Err(format!("Color column '{}' not found in data", color_col));
            }
        }
        
        if let Some(size_col) = &config.size_column {
            if !size_col.is_empty() && !query_result.columns.contains(size_col) {
                return Err(format!("Size column '{}' not found in data", size_col));
            }
        }
        
        Ok(())
    }
    
    /// Prepare data from QueryResult into PlotData structure
    fn prepare_data(&self, query_result: &QueryResult, config: &PlotConfiguration) -> Result<PlotData, String> {
        // Default implementation - can be overridden by specific plot types
        let points = extract_plot_points(query_result, config)?;
        
        Ok(PlotData {
            points,
            series: vec![],
            metadata: PlotMetadata {
                title: config.title.clone(),
                x_label: config.x_column.clone(),
                y_label: config.y_column.clone(),
                show_legend: config.show_legend,
                show_grid: config.show_grid,
                color_scheme: config.color_scheme.clone(),
                extra_data: None,
            },
            statistics: None,
        })
    }
    
    /// Render the plot
    fn render(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration);
    
    /// Render legend for the plot
    fn render_legend(&self, ui: &mut Ui, data: &PlotData, config: &PlotConfiguration) {
        if !data.series.is_empty() && config.show_legend {
            ui.group(|ui| {
                ui.label(RichText::new("Series:").strong());
                ui.separator();
                
                for series in &data.series {
                    let mut is_visible = series.visible;
                    if ui.checkbox(&mut is_visible, &series.name).changed() {
                        // Note: This would require mutable access to data, which we don't have here
                        // The actual visibility toggle should be handled in handle_interaction
                    }
                    
                    // Show series style indicator
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
                    });
                }
                
                // Show statistics if available
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
    
    /// Handle plot interactions
    fn handle_interaction(&self, _ui: &mut Ui, _data: &PlotData, _config: &PlotConfiguration) -> Option<PlotInteraction> {
        None
    }
}

/// Enhanced plot data structure with rich metadata and series support
#[derive(Debug, Clone)]
pub struct PlotData {
    pub points: Vec<PlotPoint>,
    pub series: Vec<DataSeries>,
    pub metadata: PlotMetadata,
    pub statistics: Option<DataStatistics>,
}

/// Enhanced plot point with multi-dimensional support and tooltip data
#[derive(Debug, Clone)]
pub struct PlotPoint {
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>, // For 3D plots
    pub label: Option<String>,
    pub color: Option<Color32>,
    pub size: Option<f32>,
    pub series_id: Option<String>,
    pub tooltip_data: HashMap<String, String>,
}

/// Data series for multi-series plots
#[derive(Debug, Clone)]
pub struct DataSeries {
    pub id: String,
    pub name: String,
    pub points: Vec<PlotPoint>,
    pub color: Color32,
    pub visible: bool,
    pub style: SeriesStyle,
}

/// Plot metadata containing display information
#[derive(Debug, Clone)]
pub struct PlotMetadata {
    pub title: String,
    pub x_label: String,
    pub y_label: String,
    pub show_legend: bool,
    pub show_grid: bool,
    pub color_scheme: ColorScheme,
    pub extra_data: Option<serde_json::Value>,
}

/// Statistical information about the data
#[derive(Debug, Clone)]
pub struct DataStatistics {
    pub mean_x: f64,
    pub mean_y: f64,
    pub std_x: f64,
    pub std_y: f64,
    pub correlation: Option<f64>,
    pub count: usize,
}

/// Series styling options
#[derive(Debug, Clone)]
pub enum SeriesStyle {
    Points { size: f32, shape: MarkerShape },
    Lines { width: f32, style: LineStyle },
    Bars { width: f32 },
    Area { fill: bool },
}

/// Marker shapes for point plots
#[derive(Debug, Clone, PartialEq)]
pub enum MarkerShape {
    Circle,
    Square,
    Diamond,
    Triangle,
    Cross,
    Plus,
    Star,
}

/// Comprehensive plot configuration
#[derive(Debug, Clone)]
pub struct PlotConfiguration {
    pub title: String,
    pub x_column: String,
    pub y_column: String,
    pub color_column: Option<String>,
    pub size_column: Option<String>,
    pub group_column: Option<String>,
    
    // Visual settings
    pub show_legend: bool,
    pub show_grid: bool,
    pub show_axes_labels: bool,
    pub color_scheme: ColorScheme,
    pub marker_size: f32,
    pub line_width: f32,
    
    // Interaction settings
    pub allow_zoom: bool,
    pub allow_pan: bool,
    pub allow_selection: bool,
    pub show_tooltips: bool,
    
    // Plot-specific settings
    pub plot_specific: PlotSpecificConfig,
}

impl Default for PlotConfiguration {
    fn default() -> Self {
        Self {
            title: String::new(),
            x_column: String::new(),
            y_column: String::new(),
            color_column: None,
            size_column: None,
            group_column: None,
            show_legend: true,
            show_grid: true,
            show_axes_labels: true,
            color_scheme: ColorScheme::Viridis,
            marker_size: 4.0,
            line_width: 2.0,
            allow_zoom: true,
            allow_pan: true,
            allow_selection: true,
            show_tooltips: true,
            plot_specific: PlotSpecificConfig::None,
        }
    }
}

/// Plot-specific configuration options
#[derive(Debug, Clone)]
pub enum PlotSpecificConfig {
    None,
    BarChart(BarChartConfig),
    LineChart(LineChartConfig),
    ScatterPlot(ScatterPlotConfig),
    Histogram(HistogramConfig),
    BoxPlot(BoxPlotConfig),
    Violin(ViolinPlotConfig),
    Heatmap(HeatmapConfig),
    Anomaly(AnomalyConfig),
    Correlation(CorrelationConfig),

    Scatter3D(Scatter3DConfig),
    Surface3D(Surface3DConfig),
    Contour(ContourConfig),
    ParallelCoordinates(ParallelCoordinatesConfig),
    Radar(RadarConfig),
    Sankey(SankeyConfig),
    Treemap(TreemapConfig),
    Sunburst(SunburstConfig),
    Network(NetworkConfig),
    Geo(GeoConfig),
    TimeAnalysis(TimeAnalysisConfig),
    Candlestick(CandlestickConfig),
    Stream(StreamConfig),
    Polar(PolarConfig),
}

impl PlotSpecificConfig {
    pub fn as_bar_chart(&self) -> &BarChartConfig {
        match self {
            PlotSpecificConfig::BarChart(config) => config,
            _ => panic!("Expected BarChart config"),
        }
    }
    
    pub fn as_line_chart(&self) -> &LineChartConfig {
        match self {
            PlotSpecificConfig::LineChart(config) => config,
            _ => panic!("Expected LineChart config"),
        }
    }
    
    pub fn as_scatter_plot(&self) -> &ScatterPlotConfig {
        match self {
            PlotSpecificConfig::ScatterPlot(config) => config,
            _ => panic!("Expected ScatterPlot config"),
        }
    }
    
    pub fn as_histogram(&self) -> &HistogramConfig {
        match self {
            PlotSpecificConfig::Histogram(config) => config,
            _ => panic!("Expected Histogram config"),
        }
    }
    
    pub fn as_box_plot(&self) -> &BoxPlotConfig {
        match self {
            PlotSpecificConfig::BoxPlot(config) => config,
            _ => panic!("Expected BoxPlot config"),
        }
    }
    
    pub fn as_violin(&self) -> &ViolinPlotConfig {
        match self {
            PlotSpecificConfig::Violin(config) => config,
            _ => panic!("Expected Violin config"),
        }
    }
    
    pub fn as_heatmap(&self) -> &HeatmapConfig {
        match self {
            PlotSpecificConfig::Heatmap(config) => config,
            _ => panic!("Expected Heatmap config"),
        }
    }
    
    pub fn as_anomaly(&self) -> &AnomalyConfig {
        match self {
            PlotSpecificConfig::Anomaly(config) => config,
            _ => panic!("Expected Anomaly config"),
        }
    }
    
    pub fn as_correlation(&self) -> &CorrelationConfig {
        match self {
            PlotSpecificConfig::Correlation(config) => config,
            _ => panic!("Expected Correlation config"),
        }
    }
    
    pub fn as_scatter3d(&self) -> &Scatter3DConfig {
        match self {
            PlotSpecificConfig::Scatter3D(config) => config,
            _ => panic!("Expected Scatter3D config"),
        }
    }
    
    pub fn as_surface3d(&self) -> &Surface3DConfig {
        match self {
            PlotSpecificConfig::Surface3D(config) => config,
            _ => panic!("Expected Surface3D config"),
        }
    }
    
    pub fn as_contour(&self) -> &ContourConfig {
        match self {
            PlotSpecificConfig::Contour(config) => config,
            _ => panic!("Expected Contour config"),
        }
    }
    
    pub fn as_parallel_coordinates(&self) -> &ParallelCoordinatesConfig {
        match self {
            PlotSpecificConfig::ParallelCoordinates(config) => config,
            _ => panic!("Expected ParallelCoordinates config"),
        }
    }
    
    pub fn as_radar(&self) -> &RadarConfig {
        match self {
            PlotSpecificConfig::Radar(config) => config,
            _ => panic!("Expected Radar config"),
        }
    }
    
    pub fn as_sankey(&self) -> &SankeyConfig {
        match self {
            PlotSpecificConfig::Sankey(config) => config,
            _ => panic!("Expected Sankey config"),
        }
    }
    
    pub fn as_treemap(&self) -> &TreemapConfig {
        match self {
            PlotSpecificConfig::Treemap(config) => config,
            _ => panic!("Expected Treemap config"),
        }
    }
    
    pub fn as_sunburst(&self) -> &SunburstConfig {
        match self {
            PlotSpecificConfig::Sunburst(config) => config,
            _ => panic!("Expected Sunburst config"),
        }
    }
    
    pub fn as_network(&self) -> &NetworkConfig {
        match self {
            PlotSpecificConfig::Network(config) => config,
            _ => panic!("Expected Network config"),
        }
    }
    
    pub fn as_geo(&self) -> &GeoConfig {
        match self {
            PlotSpecificConfig::Geo(config) => config,
            _ => panic!("Expected Geo config"),
        }
    }
    
    pub fn as_time_analysis(&self) -> &TimeAnalysisConfig {
        match self {
            PlotSpecificConfig::TimeAnalysis(config) => config,
            _ => panic!("Expected TimeAnalysis config"),
        }
    }
    
    pub fn as_candlestick(&self) -> &CandlestickConfig {
        match self {
            PlotSpecificConfig::Candlestick(config) => config,
            _ => panic!("Expected Candlestick config"),
        }
    }
    
    pub fn as_stream(&self) -> &StreamConfig {
        match self {
            PlotSpecificConfig::Stream(config) => config,
            _ => panic!("Expected Stream config"),
        }
    }
    
    pub fn as_polar(&self) -> &PolarConfig {
        match self {
            PlotSpecificConfig::Polar(config) => config,
            _ => panic!("Expected Polar config"),
        }
    }
}

/// Bar chart specific configuration
#[derive(Debug, Clone)]
pub struct BarChartConfig {
    pub bar_width: f32,
    pub group_spacing: f32,
    pub stacking_mode: StackingMode,
    pub sort_order: SortOrder,
    pub orientation: BarOrientation,
}

impl Default for BarChartConfig {
    fn default() -> Self {
        Self {
            bar_width: 0.8,
            group_spacing: 0.2,
            stacking_mode: StackingMode::None,
            sort_order: SortOrder::None,
            orientation: BarOrientation::Vertical,
        }
    }
}

/// Line chart specific configuration
#[derive(Debug, Clone)]
pub struct LineChartConfig {
    pub line_style: LineStyle,
    pub show_points: bool,
    pub smooth_lines: bool,
    pub fill_area: bool,
}

impl Default for LineChartConfig {
    fn default() -> Self {
        Self {
            line_style: LineStyle::Solid,
            show_points: true,
            smooth_lines: false,
            fill_area: false,
        }
    }
}

/// Scatter plot specific configuration
#[derive(Debug, Clone)]
pub struct ScatterPlotConfig {
    pub point_shape: MarkerShape,
    pub show_trend_line: bool,
    pub show_density: bool,
    pub jitter_amount: f32,
}

impl Default for ScatterPlotConfig {
    fn default() -> Self {
        Self {
            point_shape: MarkerShape::Circle,
            show_trend_line: false,
            show_density: false,
            jitter_amount: 0.0,
        }
    }
}

/// Histogram specific configuration
#[derive(Debug, Clone)]
pub struct HistogramConfig {
    pub bin_count: Option<usize>,
    pub bin_width: Option<f64>,
    pub show_density: bool,
    pub show_normal_curve: bool,
}

impl Default for HistogramConfig {
    fn default() -> Self {
        Self {
            bin_count: None,
            bin_width: None,
            show_density: false,
            show_normal_curve: false,
        }
    }
}

/// Box plot specific configuration
#[derive(Debug, Clone)]
pub struct BoxPlotConfig {
    pub show_outliers: bool,
    pub show_mean: bool,
    pub notched: bool,
    pub violin_overlay: bool,
}

impl Default for BoxPlotConfig {
    fn default() -> Self {
        Self {
            show_outliers: true,
            show_mean: false,
            notched: false,
            violin_overlay: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BarOrientation {
    Vertical,
    Horizontal,
}

impl Default for BarOrientation {
    fn default() -> Self {
        BarOrientation::Vertical
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StackingMode {
    None,
    Stacked,
    Percent,
}

/// Sort orders for categorical data
#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    None,
    Ascending,
    Descending,
    ByValue,
}

/// Line styles
#[derive(Debug, Clone, PartialEq)]
pub enum LineStyle {
    Solid,
    Dashed,
    Dotted,
    DashDot,
}

/// Plot interaction events
#[derive(Debug, Clone)]
pub enum PlotInteraction {
    PointSelected(Vec<usize>),
    AreaSelected(f64, f64, f64, f64), // x1, y1, x2, y2
    SeriesToggled(String),
    ZoomChanged(f64, f64, f64, f64), // x1, y1, x2, y2
}

/// All available plot types
#[derive(Debug, Clone, PartialEq)]
pub enum PlotType {
    // Basic 2D plots
    BarChart,
    LineChart,
    ScatterPlot,
    Histogram,
    BoxPlot,
    HeatMap,
    ViolinPlot,
    
    // Statistical plots
    AnomalyDetection,
    CorrelationMatrix,
    
    // 3D plots
    Scatter3D,
    Surface3D,
    ContourPlot,
    
    // Multi-dimensional plots
    ParallelCoordinates,
    RadarChart,
    
    // Hierarchical and flow plots
    SankeyDiagram,
    Treemap,
    SunburstChart,
    NetworkGraph,
    
    // Geographic plots
    GeoPlot,
    
    // Time series analysis
    TimeAnalysis,
    
    // Financial plots
    CandlestickChart,
    
    // Utility plots
    StreamGraph,
    PolarPlot,
}

impl PlotType {
    pub fn name(&self) -> &'static str {
        match self {
            PlotType::BarChart => "Bar Chart",
            PlotType::LineChart => "Line Chart",
            PlotType::ScatterPlot => "Scatter Plot",
            PlotType::Histogram => "Histogram",
            PlotType::BoxPlot => "Box Plot",
            PlotType::HeatMap => "Heat Map",
            PlotType::ViolinPlot => "Violin Plot",
            PlotType::AnomalyDetection => "Anomaly Detection",
            PlotType::CorrelationMatrix => "Correlation Matrix",
            PlotType::Scatter3D => "3D Scatter Plot",
            PlotType::Surface3D => "3D Surface Plot",
            PlotType::ContourPlot => "Contour Plot",
            PlotType::ParallelCoordinates => "Parallel Coordinates",
            PlotType::RadarChart => "Radar Chart",
            PlotType::SankeyDiagram => "Sankey Diagram",
            PlotType::Treemap => "Treemap",
            PlotType::SunburstChart => "Sunburst Chart",
            PlotType::NetworkGraph => "Network Graph",
            PlotType::GeoPlot => "Geographic Plot",
            PlotType::TimeAnalysis => "Time Series Analysis",
            PlotType::CandlestickChart => "Candlestick Chart",
            PlotType::StreamGraph => "Stream Graph",
            PlotType::PolarPlot => "Polar Plot",
        }
    }
    
    pub fn all_types() -> Vec<Self> {
        vec![
            // Basic 2D plots
            PlotType::BarChart,
            PlotType::LineChart,
            PlotType::ScatterPlot,
            PlotType::Histogram,
            PlotType::BoxPlot,
            PlotType::HeatMap,
            PlotType::ViolinPlot,
            
            // Statistical plots
            PlotType::AnomalyDetection,
            PlotType::CorrelationMatrix,
            
            // 3D plots
            PlotType::Scatter3D,
            PlotType::Surface3D,
            PlotType::ContourPlot,
            
            // Multi-dimensional plots
            PlotType::ParallelCoordinates,
            PlotType::RadarChart,
            
            // Hierarchical and flow plots
            PlotType::SankeyDiagram,
            PlotType::Treemap,
            PlotType::SunburstChart,
            PlotType::NetworkGraph,
            
            // Geographic plots
            PlotType::GeoPlot,
            
            // Time series analysis
            PlotType::TimeAnalysis,
            
            // Financial plots
            PlotType::CandlestickChart,
            
            // Utility plots
            PlotType::StreamGraph,
            PlotType::PolarPlot,
        ]
    }
    
    /// Get the categories for grouping plots in the UI
    pub fn categories() -> Vec<(&'static str, Vec<PlotType>)> {
        vec![
            ("Basic 2D", vec![
                PlotType::BarChart,
                PlotType::LineChart,
                PlotType::ScatterPlot,
                PlotType::Histogram,
                PlotType::BoxPlot,
                PlotType::HeatMap,
                PlotType::ViolinPlot,
            ]),
            ("Statistical", vec![
                PlotType::AnomalyDetection,
                PlotType::CorrelationMatrix,
    
            ]),
            ("3D Plots", vec![
                PlotType::Scatter3D,
                PlotType::Surface3D,
                PlotType::ContourPlot,
            ]),
            ("Multi-dimensional", vec![
                PlotType::ParallelCoordinates,
                PlotType::RadarChart,
            ]),
            ("Hierarchical & Flow", vec![
                PlotType::SankeyDiagram,
                PlotType::Treemap,
                PlotType::SunburstChart,
                PlotType::NetworkGraph,
            ]),
            ("Geographic", vec![
                PlotType::GeoPlot,
            ]),
            ("Time Series", vec![
                PlotType::TimeAnalysis,
                PlotType::CandlestickChart,
            ]),
            ("Specialized", vec![
                PlotType::StreamGraph,
                PlotType::PolarPlot,
            ]),
        ]
    }
    
    /// Get required column types for X axis (None means no X column required)
    pub fn required_x_types(&self) -> Option<Vec<DataType>> {
        match self {
            // Plots that require X column
            PlotType::BarChart | PlotType::LineChart | PlotType::ScatterPlot | 
            PlotType::Scatter3D | PlotType::Surface3D | PlotType::ContourPlot |
            PlotType::TimeAnalysis | PlotType::CandlestickChart => {
                Some(vec![DataType::Utf8, DataType::Int64, DataType::Float64])
            }
            
            // Plots that don't require X column
            PlotType::Histogram | PlotType::BoxPlot | PlotType::ViolinPlot |
            PlotType::HeatMap | PlotType::CorrelationMatrix |
            PlotType::AnomalyDetection => None,
            
            // Default: require X column
            _ => Some(vec![DataType::Utf8, DataType::Int64, DataType::Float64]),
        }
    }
    
    /// Get required column types for Y axis
    pub fn required_y_types(&self) -> Vec<DataType> {
        match self {
            // Plots that require numeric Y
            PlotType::BarChart | PlotType::LineChart | PlotType::ScatterPlot |
            PlotType::Histogram | PlotType::BoxPlot | PlotType::ViolinPlot |
            PlotType::HeatMap | PlotType::CorrelationMatrix |
            PlotType::AnomalyDetection | PlotType::Scatter3D | PlotType::Surface3D |
            PlotType::ContourPlot => {
                vec![DataType::Int64, DataType::Float64]
            }
            
            // Default: allow any type
            _ => vec![DataType::Utf8, DataType::Int64, DataType::Float64],
        }
    }
    
    /// Check if this plot type supports the given column types
    pub fn supports_column_types(&self, x_type: Option<&DataType>, y_type: &DataType) -> bool {
        match self {
            // Numeric plots require numeric data
            PlotType::ScatterPlot | PlotType::LineChart | PlotType::Scatter3D | 
            PlotType::Surface3D | PlotType::ContourPlot => {
                is_numeric_type(y_type) && x_type.map_or(true, is_numeric_type)
            }
            
            // Bar charts can have categorical X axis
            PlotType::BarChart => {
                is_numeric_type(y_type) && x_type.map_or(true, |t| is_categorical_type(t) || is_numeric_type(t))
            }
            
            // Histograms only need numeric Y
            PlotType::Histogram | PlotType::BoxPlot | PlotType::ViolinPlot => {
                is_numeric_type(y_type)
            }
            
            // Heat maps need numeric values
            PlotType::HeatMap => is_numeric_type(y_type),
            
            // Time series need temporal X axis
            PlotType::TimeAnalysis | PlotType::CandlestickChart => {
                is_numeric_type(y_type) && x_type.map_or(false, is_temporal_type)
            }
            
            // Statistical plots have specific requirements
            PlotType::CorrelationMatrix => is_numeric_type(y_type),
            PlotType::AnomalyDetection => is_numeric_type(y_type),
            
            // Default: allow any types
            _ => true,
        }
    }
}

/// Helper functions for type checking
pub fn is_numeric_type(dtype: &DataType) -> bool {
    matches!(dtype,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float16 | DataType::Float32 | DataType::Float64 |
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    )
}

pub fn is_categorical_type(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Utf8 | DataType::LargeUtf8)
}

pub fn is_temporal_type(dtype: &DataType) -> bool {
    matches!(dtype,
        DataType::Date32 | DataType::Date64 |
        DataType::Time32(_) | DataType::Time64(_) |
        DataType::Timestamp(_, _) | DataType::Duration(_) |
        DataType::Interval(_)
    )
}

/// Helper function to extract plot points from QueryResult
pub fn extract_plot_points(query_result: &QueryResult, config: &PlotConfiguration) -> Result<Vec<PlotPoint>, String> {
    if config.y_column.is_empty() {
        return Err("Y column not selected".to_string());
    }

    let y_idx = query_result.columns.iter().position(|c| c == &config.y_column)
        .ok_or("Y column not found")?;
    
    let x_idx = if !config.x_column.is_empty() {
        query_result.columns.iter().position(|c| c == &config.x_column)
    } else {
        None
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
    let colors = get_categorical_colors(&config.color_scheme);
    let mut color_map: HashMap<String, Color32> = HashMap::new();
    let mut color_index = 0;
    
    for (row_idx, row) in query_result.rows.iter().enumerate() {
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

            // Handle color mapping
            let point_color = if let Some(color_idx) = color_idx {
                if row.len() > color_idx {
                    let color_value = &row[color_idx];
                    if let Some(&existing_color) = color_map.get(color_value) {
                        Some(existing_color)
                    } else {
                        let new_color = colors[color_index % colors.len()];
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

            // Handle size mapping
            let point_size = if let Some(size_idx) = size_idx {
                if row.len() > size_idx {
                    row[size_idx].parse::<f32>().ok()
                } else {
                    None
                }
            } else {
                None
            };

            // Create tooltip data
            let mut tooltip_data = HashMap::new();
            tooltip_data.insert("X".to_string(), x_val.to_string());
            tooltip_data.insert("Y".to_string(), y_val.to_string());
            
            if let Some(x_idx) = x_idx {
                if row.len() > x_idx {
                    tooltip_data.insert(config.x_column.clone(), row[x_idx].clone());
                }
            }
            tooltip_data.insert(config.y_column.clone(), row[y_idx].clone());
            
            if let Some(color_idx) = color_idx {
                if row.len() > color_idx {
                    tooltip_data.insert(config.color_column.as_ref().unwrap().clone(), row[color_idx].clone());
                }
            }

            points.push(PlotPoint {
                x: x_val,
                y: y_val,
                z: None,
                label: None,
                color: point_color,
                size: point_size,
                series_id: None,
                tooltip_data,
            });
        }
    }
    
    Ok(points)
}

/// Get categorical colors from color scheme
pub fn get_categorical_colors(scheme: &ColorScheme) -> Vec<Color32> {
    scheme.get_colors(10)
}

/// Re-export commonly used items
pub use self::bar::BarChartPlot;
pub use self::line::LineChartPlot;
pub use self::scatter::{ScatterPlotImpl, ScatterPlot};
pub use self::histogram::HistogramPlot;
pub use self::box_plot::BoxPlotImpl;
pub use self::data_processor::{DataProcessor, AnomalyMethod, BoxPlotStats};
pub use self::heatmap::HeatmapPlot;
pub use self::violin::ViolinPlot;
pub use self::anomaly::AnomalyPlot;
pub use self::correlation::CorrelationPlot;

pub use self::scatter3d::Scatter3DPlot;
pub use self::surface3d::Surface3dPlot;
pub use self::contour::ContourPlot;
pub use self::parallel_coordinates::ParallelCoordinatesPlot;
pub use self::radar::RadarPlot;
pub use self::sankey::SankeyPlot;
pub use self::treemap::TreemapPlot;
pub use self::sunburst::SunburstPlot;
pub use self::network::NetworkPlot;
pub use self::geo::GeoPlot;
pub use self::time_analysis::TimeAnalysisPlot;
pub use self::candlestick::CandlestickPlot;
pub use self::stream::StreamPlot;
pub use self::polar::PolarPlot;

#[derive(Debug, Clone)]
pub struct ViolinPlotConfig {
    pub bandwidth: f32,
    pub show_box_plot: bool,
    pub show_mean: bool,
    pub show_median: bool,
    pub show_quartiles: bool,
    pub show_outliers: bool,
    pub violin_width: f32,
    pub kde_points: usize,
    pub comparison_mode: bool,
    pub normalize_width: bool,
    pub show_distribution_curve: bool,
    pub show_points: bool,
    pub point_alpha: f32,
    pub orientation: Orientation,
    pub scale: ViolinScale,
}

impl Default for ViolinPlotConfig {
    fn default() -> Self {
        Self {
            bandwidth: 0.5,
            show_box_plot: true,
            show_mean: true,
            show_median: true,
            show_quartiles: true,
            show_outliers: true,
            violin_width: 0.8,
            kde_points: 100,
            comparison_mode: false,
            normalize_width: true,
            show_distribution_curve: false,
            show_points: false,
            point_alpha: 0.6,
            orientation: Orientation::Vertical,
            scale: ViolinScale::Width,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeatmapConfig {
    pub aggregation: AggregationMethod,
    pub cell_size: f32,
    pub show_values: bool,
    pub color_scale: ColorScale,
}

impl Default for HeatmapConfig {
    fn default() -> Self {
        Self {
            aggregation: AggregationMethod::Mean,
            cell_size: 1.0,
            show_values: false,
            color_scale: ColorScale::Linear,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnomalyConfig {
    pub detection_method: AnomalyMethod,
    pub threshold: f64,
    pub window_size: usize,
    pub show_normal_range: bool,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            detection_method: AnomalyMethod::IQR { multiplier: 1.5 },
            threshold: 2.0,
            window_size: 10,
            show_normal_range: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CorrelationConfig {
    pub method: CorrelationMethod,
    pub show_p_values: bool,
    pub significance_threshold: f64,
    pub cluster_method: ClusterMethod,
}

impl Default for CorrelationConfig {
    fn default() -> Self {
        Self {
            method: CorrelationMethod::Pearson,
            show_p_values: false,
            significance_threshold: 0.05,
            cluster_method: ClusterMethod::None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Scatter3DConfig {
    pub point_size: f32,
    pub show_axes: bool,
    pub rotation_speed: f32,
    pub projection: Projection3D,
}

impl Default for Scatter3DConfig {
    fn default() -> Self {
        Self {
            point_size: 3.0,
            show_axes: true,
            rotation_speed: 1.0,
            projection: Projection3D::Orthographic,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Surface3DConfig {
    pub resolution: usize,
    pub interpolation: InterpolationMethod,
    pub show_wireframe: bool,
    pub wireframe_alpha: f32,
}

impl Default for Surface3DConfig {
    fn default() -> Self {
        Self {
            resolution: 50,
            interpolation: InterpolationMethod::Linear,
            show_wireframe: false,
            wireframe_alpha: 0.3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContourConfig {
    pub levels: usize,
    pub smooth_contours: bool,
    pub fill_contours: bool,
    pub show_labels: bool,
}

impl Default for ContourConfig {
    fn default() -> Self {
        Self {
            levels: 10,
            smooth_contours: true,
            fill_contours: true,
            show_labels: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParallelCoordinatesConfig {
    pub show_axes_labels: bool,
    pub line_alpha: f32,
    pub axis_spacing: f32,
    pub show_brush: bool,
}

impl Default for ParallelCoordinatesConfig {
    fn default() -> Self {
        Self {
            show_axes_labels: true,
            line_alpha: 0.7,
            axis_spacing: 1.0,
            show_brush: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RadarConfig {
    pub show_axes: bool,
    pub fill_area: bool,
    pub area_alpha: f32,
    pub max_value: Option<f64>,
}

impl Default for RadarConfig {
    fn default() -> Self {
        Self {
            show_axes: true,
            fill_area: false,
            area_alpha: 0.3,
            max_value: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SankeyConfig {
    pub node_width: f32,
    pub node_padding: f32,
    pub link_alpha: f32,
    pub show_values: bool,
}

impl Default for SankeyConfig {
    fn default() -> Self {
        Self {
            node_width: 20.0,
            node_padding: 10.0,
            link_alpha: 0.6,
            show_values: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TreemapConfig {
    pub algorithm: TreemapAlgorithm,
    pub padding: f32,
    pub show_labels: bool,
    pub label_threshold: usize,
}

impl Default for TreemapConfig {
    fn default() -> Self {
        Self {
            algorithm: TreemapAlgorithm::Squarified,
            padding: 2.0,
            show_labels: true,
            label_threshold: 5,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SunburstConfig {
    pub inner_radius: f32,
    pub show_labels: bool,
    pub label_threshold: f32,
    pub animation_speed: f32,
}

impl Default for SunburstConfig {
    fn default() -> Self {
        Self {
            inner_radius: 0.0,
            show_labels: true,
            label_threshold: 0.02,
            animation_speed: 1.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub layout: NetworkLayout,
    pub node_size: f32,
    pub edge_width: f32,
    pub show_labels: bool,
    pub physics: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            layout: NetworkLayout::ForceDirected,
            node_size: 5.0,
            edge_width: 1.0,
            show_labels: false,
            physics: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GeoConfig {
    pub projection: GeoProjection,
    pub show_coastlines: bool,
    pub show_countries: bool,
    pub color_by: GeoColorBy,
}

impl Default for GeoConfig {
    fn default() -> Self {
        Self {
            projection: GeoProjection::Mercator,
            show_coastlines: true,
            show_countries: false,
            color_by: GeoColorBy::Value,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeAnalysisConfig {
    pub analysis_type: TimeAnalysisType,
    pub window_size: usize,
    pub show_trend: bool,
    pub show_seasonality: bool,
}

impl Default for TimeAnalysisConfig {
    fn default() -> Self {
        Self {
            analysis_type: TimeAnalysisType::Trend,
            window_size: 10,
            show_trend: true,
            show_seasonality: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CandlestickConfig {
    pub candle_width: f32,
    pub show_volume: bool,
    pub volume_alpha: f32,
    pub show_indicators: bool,
}

impl Default for CandlestickConfig {
    fn default() -> Self {
        Self {
            candle_width: 0.8,
            show_volume: false,
            volume_alpha: 0.5,
            show_indicators: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub interpolation: InterpolationMethod,
    pub stack_order: StackOrder,
    pub show_labels: bool,
    pub label_threshold: f32,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            interpolation: InterpolationMethod::Linear,
            stack_order: StackOrder::None,
            show_labels: false,
            label_threshold: 0.05,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PolarConfig {
    pub radius_range: (f64, f64),
    pub angle_range: (f64, f64),
    pub show_grid: bool,
    pub grid_alpha: f32,
}

impl Default for PolarConfig {
    fn default() -> Self {
        Self {
            radius_range: (0.0, 1.0),
            angle_range: (0.0, 2.0 * std::f64::consts::PI),
            show_grid: true,
            grid_alpha: 0.2,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Orientation {
    Vertical,
    Horizontal,
}

#[derive(Debug, Clone)]
pub enum ViolinScale {
    Count,
    Width,
    Area,
}

#[derive(Debug, Clone)]
pub enum AggregationMethod {
    Sum,
    Mean,
    Count,
    Min,
    Max,
    Median,
}

#[derive(Debug, Clone)]
pub enum ColorScale {
    Linear,
    Log,
    SymLog,
    Custom,
}

#[derive(Debug, Clone)]
pub enum CorrelationMethod {
    Pearson,
    Spearman,
    Kendall,
}

#[derive(Debug, Clone)]
pub enum ClusterMethod {
    None,
    Hierarchical,
    KMeans,
}

#[derive(Debug, Clone)]
pub enum DistributionType {
    Histogram,
    KDE,
    Box,
    Violin,
}

#[derive(Debug, Clone)]
pub enum BandwidthMethod {
    Silverman,
    Scott,
    Manual,
}

#[derive(Debug, Clone)]
pub enum Projection3D {
    Orthographic,
    Perspective,
}

#[derive(Debug, Clone)]
pub enum InterpolationMethod {
    Linear,
    Cubic,
    Nearest,
}

#[derive(Debug, Clone)]
pub enum StackOrder {
    None,
    Ascending,
    Descending,
    InsideOut,
    OutsideIn,
}

#[derive(Debug, Clone)]
pub enum NetworkLayout {
    ForceDirected,
    Circular,
    Hierarchical,
    Random,
}

#[derive(Debug, Clone)]
pub enum GeoProjection {
    Mercator,
    Albers,
    Orthographic,
}

#[derive(Debug, Clone)]
pub enum GeoColorBy {
    Value,
    Category,
    Density,
}

#[derive(Debug, Clone)]
pub enum TimeAnalysisType {
    Trend,
    Seasonality,
    Decomposition,
    Forecasting,
}

#[derive(Debug, Clone)]
pub enum TreemapAlgorithm {
    Squarified,
    Slice,
    Dice,
}