//! Enhanced plot configuration system
//! Based on frog-viz best practices with improved structure and validation

use datafusion::arrow::datatypes::DataType;
use egui::{Color32, Pos2, Vec2};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ColorScheme {
    Viridis,
    Plasma,
    Inferno,
    Magma,
    Cividis,
    Turbo,
    Rainbow,
    Spectral,
    RdYlBu,
    RdYlGn,
    RdBu,
    RdGy,
    PuOr,
    BrBG,
    PiYG,
    PRGn,
    Pastel1,
    Pastel2,
    Set1,
    Set2,
    Set3,
    Tab10,
    Tab20,
    Tab20b,
    Tab20c,
}

impl ColorScheme {
    pub fn get_colors(&self, count: usize) -> Vec<Color32> {
        match self {
            ColorScheme::Viridis => get_viridis_colors(count),
            ColorScheme::Plasma => get_plasma_colors(count),
            ColorScheme::Inferno => get_inferno_colors(count),
            ColorScheme::Magma => get_magma_colors(count),
            ColorScheme::Cividis => get_cividis_colors(count),
            ColorScheme::Turbo => get_turbo_colors(count),
            ColorScheme::Rainbow => get_rainbow_colors(count),
            ColorScheme::Spectral => get_spectral_colors(count),
            ColorScheme::RdYlBu => get_rdylbu_colors(count),
            ColorScheme::RdYlGn => get_rdylgn_colors(count),
            ColorScheme::RdBu => get_rdbu_colors(count),
            ColorScheme::RdGy => get_rdgy_colors(count),
            ColorScheme::PuOr => get_puor_colors(count),
            ColorScheme::BrBG => get_brbg_colors(count),
            ColorScheme::PiYG => get_piyg_colors(count),
            ColorScheme::PRGn => get_prgn_colors(count),
            ColorScheme::Pastel1 => get_pastel1_colors(count),
            ColorScheme::Pastel2 => get_pastel2_colors(count),
            ColorScheme::Set1 => get_set1_colors(count),
            ColorScheme::Set2 => get_set2_colors(count),
            ColorScheme::Set3 => get_set3_colors(count),
            ColorScheme::Tab10 => get_tab10_colors(count),
            ColorScheme::Tab20 => get_tab20_colors(count),
            ColorScheme::Tab20b => get_tab20b_colors(count),
            ColorScheme::Tab20c => get_tab20c_colors(count),
        }
    }
}

impl Default for ColorScheme {
    fn default() -> Self {
        ColorScheme::Viridis
    }
}

/// Enhanced plot configuration with frog-viz inspired structure
#[derive(Debug, Clone)]
pub struct EnhancedPlotConfig {
    // Data source configuration
    pub data_source_id: Option<String>,
    pub x_column: Option<String>,
    pub y_columns: Vec<String>,
    pub color_column: Option<String>,
    pub size_column: Option<String>,
    pub category_column: Option<String>,
    
    // Visual configuration
    pub title: String,
    pub x_label: String,
    pub y_label: String,
    pub color_scheme: ColorScheme,
    pub show_legend: bool,
    pub show_grid: bool,
    pub show_axes_labels: bool,
    
    // Interaction configuration
    pub allow_zoom: bool,
    pub allow_pan: bool,
    pub allow_selection: bool,
    pub show_tooltips: bool,
    
    // Performance configuration
    pub max_points: usize,
    pub enable_caching: bool,
    pub auto_sample: bool,
    
    // Plot-specific configuration
    pub plot_specific: PlotSpecificConfig,
    
    // Validation state
    pub validation_errors: Vec<String>,
    pub is_valid: bool,
}

/// Plot-specific configuration options
#[derive(Debug, Clone)]
pub enum PlotSpecificConfig {
    None,
    LineChart(LineChartConfig),
    ScatterPlot(ScatterPlotConfig),
    BarChart(BarChartConfig),
    Histogram(HistogramConfig),
    BoxPlot(BoxPlotConfig),
    Heatmap(HeatmapConfig),
    Violin(ViolinConfig),
    Anomaly(AnomalyConfig),
    Correlation(CorrelationConfig),
    Distribution(DistributionConfig),
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

/// Line chart specific configuration
#[derive(Debug, Clone)]
pub struct LineChartConfig {
    pub line_width: f32,
    pub show_points: bool,
    pub point_radius: f32,
    pub line_style: LineStyle,
    pub fill_area: bool,
    pub fill_alpha: f32,
    pub smooth_lines: bool,
    pub handle_missing_data: bool,
}

/// Scatter plot specific configuration
#[derive(Debug, Clone)]
pub struct ScatterPlotConfig {
    pub point_radius: f32,
    pub marker_shape: MarkerShape,
    pub show_trend_line: bool,
    pub show_density: bool,
    pub jitter_amount: f32,
    pub alpha: f32,
}

/// Bar chart specific configuration
#[derive(Debug, Clone)]
pub struct BarChartConfig {
    pub bar_width: f32,
    pub group_spacing: f32,
    pub stacking_mode: StackingMode,
    pub sort_order: SortOrder,
    pub show_values: bool,
    pub value_position: ValuePosition,
}

/// Histogram specific configuration
#[derive(Debug, Clone)]
pub struct HistogramConfig {
    pub bin_count: Option<usize>,
    pub bin_width: Option<f64>,
    pub show_density: bool,
    pub show_normal_curve: bool,
    pub cumulative: bool,
    pub orientation: Orientation,
}

/// Box plot specific configuration
#[derive(Debug, Clone)]
pub struct BoxPlotConfig {
    pub show_outliers: bool,
    pub show_mean: bool,
    pub notched: bool,
    pub violin_overlay: bool,
    pub outlier_style: OutlierStyle,
}

/// Heatmap specific configuration
#[derive(Debug, Clone)]
pub struct HeatmapConfig {
    pub aggregation: AggregationMethod,
    pub cell_size: f32,
    pub show_values: bool,
    pub color_scale: ColorScale,
}

/// Violin plot specific configuration
#[derive(Debug, Clone)]
pub struct ViolinConfig {
    pub show_box_plot: bool,
    pub show_points: bool,
    pub point_alpha: f32,
    pub orientation: Orientation,
    pub scale: ViolinScale,
}

/// Anomaly detection specific configuration
#[derive(Debug, Clone)]
pub struct AnomalyConfig {
    pub detection_method: AnomalyMethod,
    pub threshold: f64,
    pub window_size: usize,
    pub show_normal_range: bool,
}

/// Correlation matrix specific configuration
#[derive(Debug, Clone)]
pub struct CorrelationConfig {
    pub method: CorrelationMethod,
    pub show_p_values: bool,
    pub significance_threshold: f64,
    pub cluster_method: ClusterMethod,
}

/// Distribution plot specific configuration
#[derive(Debug, Clone)]
pub struct DistributionConfig {
    pub plot_type: DistributionType,
    pub show_kde: bool,
    pub show_histogram: bool,
    pub bandwidth_method: BandwidthMethod,
}

/// 3D Scatter plot specific configuration
#[derive(Debug, Clone)]
pub struct Scatter3DConfig {
    pub point_size: f32,
    pub show_axes: bool,
    pub rotation_speed: f32,
    pub projection: Projection3D,
}

/// 3D Surface plot specific configuration
#[derive(Debug, Clone)]
pub struct Surface3DConfig {
    pub resolution: usize,
    pub interpolation: InterpolationMethod,
    pub show_wireframe: bool,
    pub wireframe_alpha: f32,
}

/// Contour plot specific configuration
#[derive(Debug, Clone)]
pub struct ContourConfig {
    pub levels: usize,
    pub smooth_contours: bool,
    pub fill_contours: bool,
    pub show_labels: bool,
}

/// Parallel coordinates specific configuration
#[derive(Debug, Clone)]
pub struct ParallelCoordinatesConfig {
    pub show_axes_labels: bool,
    pub line_alpha: f32,
    pub axis_spacing: f32,
    pub show_brush: bool,
}

/// Radar chart specific configuration
#[derive(Debug, Clone)]
pub struct RadarConfig {
    pub show_axes: bool,
    pub fill_area: bool,
    pub area_alpha: f32,
    pub max_value: Option<f64>,
}

/// Sankey diagram specific configuration
#[derive(Debug, Clone)]
pub struct SankeyConfig {
    pub node_width: f32,
    pub node_padding: f32,
    pub link_alpha: f32,
    pub show_values: bool,
}

/// Treemap specific configuration
#[derive(Debug, Clone)]
pub struct TreemapConfig {
    pub algorithm: TreemapAlgorithm,
    pub padding: f32,
    pub show_labels: bool,
    pub label_threshold: usize,
}

/// Sunburst chart specific configuration
#[derive(Debug, Clone)]
pub struct SunburstConfig {
    pub inner_radius: f32,
    pub show_labels: bool,
    pub label_threshold: f32,
    pub animation_speed: f32,
}

/// Network graph specific configuration
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub layout: NetworkLayout,
    pub node_size: f32,
    pub edge_width: f32,
    pub show_labels: bool,
    pub physics: bool,
}

/// Geographic plot specific configuration
#[derive(Debug, Clone)]
pub struct GeoConfig {
    pub projection: GeoProjection,
    pub show_coastlines: bool,
    pub show_countries: bool,
    pub color_by: GeoColorBy,
}

/// Time analysis specific configuration
#[derive(Debug, Clone)]
pub struct TimeAnalysisConfig {
    pub analysis_type: TimeAnalysisType,
    pub window_size: usize,
    pub show_trend: bool,
    pub show_seasonality: bool,
}

/// Candlestick chart specific configuration
#[derive(Debug, Clone)]
pub struct CandlestickConfig {
    pub candle_width: f32,
    pub show_volume: bool,
    pub volume_alpha: f32,
    pub show_indicators: bool,
}

/// Stream graph specific configuration
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub interpolation: InterpolationMethod,
    pub stack_order: StackOrder,
    pub show_labels: bool,
    pub label_threshold: f32,
}

/// Polar plot specific configuration
#[derive(Debug, Clone)]
pub struct PolarConfig {
    pub radius_range: (f64, f64),
    pub angle_range: (f64, f64),
    pub show_grid: bool,
    pub grid_alpha: f32,
}

// Enums for configuration options

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LineStyle {
    Solid,
    Dashed,
    Dotted,
    DashDot,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarkerShape {
    Circle,
    Square,
    Diamond,
    Triangle,
    Cross,
    Plus,
    Star,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StackingMode {
    None,
    Stacked,
    Percent,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortOrder {
    None,
    Ascending,
    Descending,
    ByValue,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValuePosition {
    Inside,
    Outside,
    Center,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Orientation {
    Vertical,
    Horizontal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutlierStyle {
    Points,
    Whiskers,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregationMethod {
    Sum,
    Mean,
    Count,
    Min,
    Max,
    Median,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColorScale {
    Linear,
    Log,
    SymLog,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ViolinScale {
    Count,
    Width,
    Area,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnomalyMethod {
    IQR,
    ZScore,
    IsolationForest,
    LOF,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CorrelationMethod {
    Pearson,
    Spearman,
    Kendall,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClusterMethod {
    None,
    Hierarchical,
    KMeans,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DistributionType {
    Histogram,
    KDE,
    Box,
    Violin,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BandwidthMethod {
    Silverman,
    Scott,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Projection3D {
    Orthographic,
    Perspective,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InterpolationMethod {
    Linear,
    Cubic,
    Nearest,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StackOrder {
    None,
    Ascending,
    Descending,
    InsideOut,
    OutsideIn,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NetworkLayout {
    ForceDirected,
    Circular,
    Hierarchical,
    Random,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoProjection {
    Mercator,
    Albers,
    Orthographic,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoColorBy {
    Value,
    Category,
    Density,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeAnalysisType {
    Trend,
    Seasonality,
    Decomposition,
    Forecasting,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TreemapAlgorithm {
    Squarified,
    Slice,
    Dice,
}

impl Default for EnhancedPlotConfig {
    fn default() -> Self {
        Self {
            data_source_id: None,
            x_column: None,
            y_columns: Vec::new(),
            color_column: None,
            size_column: None,
            category_column: None,
            title: String::new(),
            x_label: String::new(),
            y_label: String::new(),
            color_scheme: ColorScheme::default(),
            show_legend: true,
            show_grid: true,
            show_axes_labels: true,
            allow_zoom: true,
            allow_pan: true,
            allow_selection: true,
            show_tooltips: true,
            max_points: 10000,
            enable_caching: true,
            auto_sample: true,
            plot_specific: PlotSpecificConfig::None,
            validation_errors: Vec::new(),
            is_valid: false,
        }
    }
}

impl EnhancedPlotConfig {
    /// Create a new configuration for a specific plot type
    pub fn new(plot_type: &str) -> Self {
        let mut config = Self::default();
        config.plot_specific = match plot_type {
            "line" => PlotSpecificConfig::LineChart(LineChartConfig::default()),
            "scatter" => PlotSpecificConfig::ScatterPlot(ScatterPlotConfig::default()),
            "bar" => PlotSpecificConfig::BarChart(BarChartConfig::default()),
            "histogram" => PlotSpecificConfig::Histogram(HistogramConfig::default()),
            "box" => PlotSpecificConfig::BoxPlot(BoxPlotConfig::default()),
            "heatmap" => PlotSpecificConfig::Heatmap(HeatmapConfig::default()),
            "violin" => PlotSpecificConfig::Violin(ViolinConfig::default()),
            "anomaly" => PlotSpecificConfig::Anomaly(AnomalyConfig::default()),
            "correlation" => PlotSpecificConfig::Correlation(CorrelationConfig::default()),
            "distribution" => PlotSpecificConfig::Distribution(DistributionConfig::default()),
            "scatter3d" => PlotSpecificConfig::Scatter3D(Scatter3DConfig::default()),
            "surface3d" => PlotSpecificConfig::Surface3D(Surface3DConfig::default()),
            "contour" => PlotSpecificConfig::Contour(ContourConfig::default()),
            "parallel_coordinates" => PlotSpecificConfig::ParallelCoordinates(ParallelCoordinatesConfig::default()),
            "radar" => PlotSpecificConfig::Radar(RadarConfig::default()),
            "sankey" => PlotSpecificConfig::Sankey(SankeyConfig::default()),
            "treemap" => PlotSpecificConfig::Treemap(TreemapConfig::default()),
            "sunburst" => PlotSpecificConfig::Sunburst(SunburstConfig::default()),
            "network" => PlotSpecificConfig::Network(NetworkConfig::default()),
            "geo" => PlotSpecificConfig::Geo(GeoConfig::default()),
            "time_analysis" => PlotSpecificConfig::TimeAnalysis(TimeAnalysisConfig::default()),
            "candlestick" => PlotSpecificConfig::Candlestick(CandlestickConfig::default()),
            "stream" => PlotSpecificConfig::Stream(StreamConfig::default()),
            "polar" => PlotSpecificConfig::Polar(PolarConfig::default()),
            _ => PlotSpecificConfig::None,
        };
        config
    }

    /// Validate the configuration
    pub fn validate(&mut self, available_columns: &[String], column_types: &HashMap<String, DataType>) -> bool {
        self.validation_errors.clear();
        
        // Check required columns
        if self.y_columns.is_empty() {
            self.validation_errors.push("At least one Y column is required".to_string());
        }
        
        // Check if columns exist
        for col in &self.y_columns {
            if !available_columns.contains(col) {
                self.validation_errors.push(format!("Y column '{}' not found in data", col));
            }
        }
        
        if let Some(x_col) = &self.x_column {
            if !available_columns.contains(x_col) {
                self.validation_errors.push(format!("X column '{}' not found in data", x_col));
            }
        }
        
        if let Some(color_col) = &self.color_column {
            if !available_columns.contains(color_col) {
                self.validation_errors.push(format!("Color column '{}' not found in data", color_col));
            }
        }
        
        if let Some(size_col) = &self.size_column {
            if !available_columns.contains(size_col) {
                self.validation_errors.push(format!("Size column '{}' not found in data", size_col));
            }
        }
        
        if let Some(cat_col) = &self.category_column {
            if !available_columns.contains(cat_col) {
                self.validation_errors.push(format!("Category column '{}' not found in data", cat_col));
            }
        }
        
        // Check column type compatibility
        self.validate_column_types(column_types);
        
        self.is_valid = self.validation_errors.is_empty();
        self.is_valid
    }

    /// Validate column type compatibility
    fn validate_column_types(&mut self, column_types: &HashMap<String, DataType>) {
        // Check Y columns are numeric
        for col in &self.y_columns {
            if let Some(dtype) = column_types.get(col) {
                if !is_numeric_type(dtype) {
                    self.validation_errors.push(format!("Y column '{}' must be numeric, found {:?}", col, dtype));
                }
            }
        }
        
        // Check X column type based on plot type
        if let Some(x_col) = &self.x_column {
            if let Some(dtype) = column_types.get(x_col) {
                match &self.plot_specific {
                    PlotSpecificConfig::LineChart(_) | PlotSpecificConfig::TimeAnalysis(_) => {
                        if !is_numeric_type(dtype) && !is_temporal_type(dtype) {
                            self.validation_errors.push(format!("X column '{}' must be numeric or temporal for line charts, found {:?}", x_col, dtype));
                        }
                    },
                    PlotSpecificConfig::ScatterPlot(_) | PlotSpecificConfig::Scatter3D(_) => {
                        if !is_numeric_type(dtype) {
                            self.validation_errors.push(format!("X column '{}' must be numeric for scatter plots, found {:?}", x_col, dtype));
                        }
                    },
                    PlotSpecificConfig::BarChart(_) => {
                        if !is_numeric_type(dtype) && !is_categorical_type(dtype) {
                            self.validation_errors.push(format!("X column '{}' must be numeric or categorical for bar charts, found {:?}", x_col, dtype));
                        }
                    },
                    _ => {
                        if !is_numeric_type(dtype) {
                            self.validation_errors.push(format!("X column '{}' must be numeric, found {:?}", x_col, dtype));
                        }
                    }
                }
            }
        }
    }

    /// Get validation errors
    pub fn get_validation_errors(&self) -> &[String] {
        &self.validation_errors
    }

    /// Check if configuration is valid
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }
}

// Default implementations for all config structs

impl Default for LineChartConfig {
    fn default() -> Self {
        Self {
            line_width: 2.0,
            show_points: false,
            point_radius: 3.0,
            line_style: LineStyle::Solid,
            fill_area: false,
            fill_alpha: 0.2,
            smooth_lines: false,
            handle_missing_data: true,
        }
    }
}

impl Default for ScatterPlotConfig {
    fn default() -> Self {
        Self {
            point_radius: 3.0,
            marker_shape: MarkerShape::Circle,
            show_trend_line: false,
            show_density: false,
            jitter_amount: 0.0,
            alpha: 1.0,
        }
    }
}

impl Default for BarChartConfig {
    fn default() -> Self {
        Self {
            bar_width: 0.8,
            group_spacing: 0.1,
            stacking_mode: StackingMode::None,
            sort_order: SortOrder::None,
            show_values: false,
            value_position: ValuePosition::Inside,
        }
    }
}

impl Default for HistogramConfig {
    fn default() -> Self {
        Self {
            bin_count: None,
            bin_width: None,
            show_density: false,
            show_normal_curve: false,
            cumulative: false,
            orientation: Orientation::Vertical,
        }
    }
}

impl Default for BoxPlotConfig {
    fn default() -> Self {
        Self {
            show_outliers: true,
            show_mean: false,
            notched: false,
            violin_overlay: false,
            outlier_style: OutlierStyle::Points,
        }
    }
}

impl Default for HeatmapConfig {
    fn default() -> Self {
        Self {
            aggregation: AggregationMethod::Mean,
            cell_size: 50.0,
            show_values: false,
            color_scale: ColorScale::Linear,
        }
    }
}

impl Default for ViolinConfig {
    fn default() -> Self {
        Self {
            show_box_plot: true,
            show_points: false,
            point_alpha: 0.5,
            orientation: Orientation::Vertical,
            scale: ViolinScale::Width,
        }
    }
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            detection_method: AnomalyMethod::IQR,
            threshold: 1.5,
            window_size: 10,
            show_normal_range: true,
        }
    }
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

impl Default for DistributionConfig {
    fn default() -> Self {
        Self {
            plot_type: DistributionType::Histogram,
            show_kde: false,
            show_histogram: true,
            bandwidth_method: BandwidthMethod::Silverman,
        }
    }
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

impl Default for SankeyConfig {
    fn default() -> Self {
        Self {
            node_width: 20.0,
            node_padding: 10.0,
            link_alpha: 0.7,
            show_values: true,
        }
    }
}

impl Default for TreemapConfig {
    fn default() -> Self {
        Self {
            algorithm: TreemapAlgorithm::Squarified,
            padding: 2.0,
            show_labels: true,
            label_threshold: 50,
        }
    }
}

impl Default for SunburstConfig {
    fn default() -> Self {
        Self {
            inner_radius: 0.0,
            show_labels: true,
            label_threshold: 0.05,
            animation_speed: 1.0,
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            layout: NetworkLayout::ForceDirected,
            node_size: 10.0,
            edge_width: 1.0,
            show_labels: false,
            physics: true,
        }
    }
}

impl Default for GeoConfig {
    fn default() -> Self {
        Self {
            projection: GeoProjection::Mercator,
            show_coastlines: true,
            show_countries: true,
            color_by: GeoColorBy::Value,
        }
    }
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

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            interpolation: InterpolationMethod::Linear,
            stack_order: StackOrder::None,
            show_labels: false,
            label_threshold: 0.1,
        }
    }
}

impl Default for PolarConfig {
    fn default() -> Self {
        Self {
            radius_range: (0.0, 1.0),
            angle_range: (0.0, 360.0),
            show_grid: true,
            grid_alpha: 0.3,
        }
    }
}

// Helper functions for type checking

fn is_numeric_type(dtype: &DataType) -> bool {
    matches!(dtype,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float32 | DataType::Float64
    )
}

fn is_categorical_type(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Utf8 | DataType::LargeUtf8)
}

fn is_temporal_type(dtype: &DataType) -> bool {
    matches!(dtype,
        DataType::Date32 | DataType::Date64 |
        DataType::Time32(_) | DataType::Time64(_) |
        DataType::Timestamp(_, _)
    )
}

// Color generation functions for ColorScheme
fn get_viridis_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(68, 1, 84),
        Color32::from_rgb(59, 82, 139),
        Color32::from_rgb(33, 145, 140),
        Color32::from_rgb(94, 201, 98),
        Color32::from_rgb(253, 231, 37),
    ];
    interpolate_colors(&colors, count)
}

fn get_plasma_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(13, 8, 135),
        Color32::from_rgb(84, 2, 163),
        Color32::from_rgb(139, 10, 165),
        Color32::from_rgb(185, 19, 114),
        Color32::from_rgb(219, 84, 77),
        Color32::from_rgb(249, 164, 63),
        Color32::from_rgb(254, 255, 136),
    ];
    interpolate_colors(&colors, count)
}

fn get_inferno_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(0, 0, 4),
        Color32::from_rgb(31, 12, 72),
        Color32::from_rgb(85, 15, 109),
        Color32::from_rgb(136, 34, 85),
        Color32::from_rgb(186, 54, 84),
        Color32::from_rgb(227, 89, 138),
        Color32::from_rgb(249, 140, 182),
        Color32::from_rgb(254, 197, 217),
        Color32::from_rgb(252, 247, 243),
    ];
    interpolate_colors(&colors, count)
}

fn get_magma_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(0, 0, 4),
        Color32::from_rgb(28, 16, 68),
        Color32::from_rgb(79, 18, 123),
        Color32::from_rgb(129, 37, 129),
        Color32::from_rgb(181, 54, 122),
        Color32::from_rgb(229, 80, 57),
        Color32::from_rgb(251, 154, 153),
        Color32::from_rgb(254, 217, 118),
        Color32::from_rgb(252, 255, 164),
    ];
    interpolate_colors(&colors, count)
}

fn get_cividis_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(0, 32, 76),
        Color32::from_rgb(0, 84, 136),
        Color32::from_rgb(0, 135, 147),
        Color32::from_rgb(0, 167, 119),
        Color32::from_rgb(0, 191, 92),
        Color32::from_rgb(0, 211, 73),
        Color32::from_rgb(0, 230, 51),
        Color32::from_rgb(0, 248, 14),
        Color32::from_rgb(0, 255, 0),
    ];
    interpolate_colors(&colors, count)
}

fn get_turbo_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(35, 23, 27),
        Color32::from_rgb(51, 51, 153),
        Color32::from_rgb(0, 153, 255),
        Color32::from_rgb(0, 255, 255),
        Color32::from_rgb(0, 255, 0),
        Color32::from_rgb(255, 255, 0),
        Color32::from_rgb(255, 153, 0),
        Color32::from_rgb(255, 0, 0),
        Color32::from_rgb(153, 0, 0),
    ];
    interpolate_colors(&colors, count)
}

fn get_rainbow_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(255, 0, 0),    // Red
        Color32::from_rgb(255, 127, 0),  // Orange
        Color32::from_rgb(255, 255, 0),  // Yellow
        Color32::from_rgb(0, 255, 0),    // Green
        Color32::from_rgb(0, 0, 255),    // Blue
        Color32::from_rgb(75, 0, 130),   // Indigo
        Color32::from_rgb(148, 0, 211),  // Violet
    ];
    interpolate_colors(&colors, count)
}

fn get_spectral_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(158, 1, 66),
        Color32::from_rgb(213, 62, 79),
        Color32::from_rgb(244, 109, 67),
        Color32::from_rgb(253, 174, 97),
        Color32::from_rgb(254, 224, 139),
        Color32::from_rgb(230, 245, 152),
        Color32::from_rgb(171, 221, 164),
        Color32::from_rgb(102, 194, 165),
        Color32::from_rgb(50, 136, 189),
        Color32::from_rgb(94, 79, 162),
    ];
    interpolate_colors(&colors, count)
}

fn get_rdylbu_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(165, 0, 38),
        Color32::from_rgb(215, 48, 39),
        Color32::from_rgb(244, 109, 67),
        Color32::from_rgb(253, 174, 97),
        Color32::from_rgb(254, 224, 144),
        Color32::from_rgb(224, 243, 248),
        Color32::from_rgb(171, 217, 233),
        Color32::from_rgb(116, 173, 209),
        Color32::from_rgb(69, 117, 180),
        Color32::from_rgb(49, 54, 149),
    ];
    interpolate_colors(&colors, count)
}

fn get_rdylgn_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(165, 0, 38),
        Color32::from_rgb(215, 48, 39),
        Color32::from_rgb(244, 109, 67),
        Color32::from_rgb(253, 174, 97),
        Color32::from_rgb(254, 224, 139),
        Color32::from_rgb(217, 239, 139),
        Color32::from_rgb(166, 217, 106),
        Color32::from_rgb(102, 189, 99),
        Color32::from_rgb(26, 152, 80),
        Color32::from_rgb(0, 104, 55),
    ];
    interpolate_colors(&colors, count)
}

fn get_rdbu_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(165, 0, 38),
        Color32::from_rgb(215, 48, 39),
        Color32::from_rgb(244, 109, 67),
        Color32::from_rgb(253, 174, 97),
        Color32::from_rgb(254, 224, 144),
        Color32::from_rgb(224, 243, 248),
        Color32::from_rgb(171, 217, 233),
        Color32::from_rgb(116, 173, 209),
        Color32::from_rgb(69, 117, 180),
        Color32::from_rgb(49, 54, 149),
    ];
    interpolate_colors(&colors, count)
}

fn get_rdgy_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(165, 0, 38),
        Color32::from_rgb(215, 48, 39),
        Color32::from_rgb(244, 109, 67),
        Color32::from_rgb(253, 174, 97),
        Color32::from_rgb(254, 224, 144),
        Color32::from_rgb(224, 224, 224),
        Color32::from_rgb(186, 186, 186),
        Color32::from_rgb(135, 135, 135),
        Color32::from_rgb(77, 77, 77),
        Color32::from_rgb(26, 26, 26),
    ];
    interpolate_colors(&colors, count)
}

fn get_puor_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(127, 59, 8),
        Color32::from_rgb(179, 88, 6),
        Color32::from_rgb(224, 130, 20),
        Color32::from_rgb(253, 184, 99),
        Color32::from_rgb(254, 224, 182),
        Color32::from_rgb(216, 218, 235),
        Color32::from_rgb(178, 171, 210),
        Color32::from_rgb(128, 115, 172),
        Color32::from_rgb(84, 39, 136),
        Color32::from_rgb(45, 0, 75),
    ];
    interpolate_colors(&colors, count)
}

fn get_brbg_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(84, 48, 5),
        Color32::from_rgb(140, 81, 10),
        Color32::from_rgb(191, 129, 45),
        Color32::from_rgb(223, 194, 125),
        Color32::from_rgb(246, 232, 195),
        Color32::from_rgb(199, 234, 229),
        Color32::from_rgb(128, 205, 193),
        Color32::from_rgb(53, 151, 143),
        Color32::from_rgb(1, 102, 94),
        Color32::from_rgb(0, 60, 48),
    ];
    interpolate_colors(&colors, count)
}

fn get_piyg_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(142, 1, 82),
        Color32::from_rgb(197, 27, 125),
        Color32::from_rgb(222, 119, 174),
        Color32::from_rgb(241, 182, 218),
        Color32::from_rgb(253, 224, 239),
        Color32::from_rgb(230, 245, 208),
        Color32::from_rgb(184, 225, 134),
        Color32::from_rgb(127, 188, 65),
        Color32::from_rgb(77, 146, 33),
        Color32::from_rgb(39, 100, 25),
    ];
    interpolate_colors(&colors, count)
}

fn get_prgn_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(64, 0, 75),
        Color32::from_rgb(118, 42, 131),
        Color32::from_rgb(153, 112, 171),
        Color32::from_rgb(194, 165, 207),
        Color32::from_rgb(231, 212, 232),
        Color32::from_rgb(217, 240, 211),
        Color32::from_rgb(166, 219, 160),
        Color32::from_rgb(90, 174, 97),
        Color32::from_rgb(27, 120, 55),
        Color32::from_rgb(0, 68, 27),
    ];
    interpolate_colors(&colors, count)
}

fn get_pastel1_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(251, 180, 174),
        Color32::from_rgb(179, 205, 227),
        Color32::from_rgb(204, 235, 197),
        Color32::from_rgb(222, 203, 228),
        Color32::from_rgb(254, 217, 166),
        Color32::from_rgb(255, 255, 204),
        Color32::from_rgb(229, 216, 189),
        Color32::from_rgb(253, 218, 236),
        Color32::from_rgb(242, 242, 242),
    ];
    interpolate_colors(&colors, count)
}

fn get_pastel2_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(179, 226, 205),
        Color32::from_rgb(253, 205, 172),
        Color32::from_rgb(203, 213, 232),
        Color32::from_rgb(244, 202, 228),
        Color32::from_rgb(230, 245, 201),
        Color32::from_rgb(255, 242, 174),
        Color32::from_rgb(241, 226, 204),
        Color32::from_rgb(204, 204, 204),
    ];
    interpolate_colors(&colors, count)
}

fn get_set1_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(228, 26, 28),
        Color32::from_rgb(55, 126, 184),
        Color32::from_rgb(77, 175, 74),
        Color32::from_rgb(152, 78, 163),
        Color32::from_rgb(255, 127, 0),
        Color32::from_rgb(166, 86, 40),
        Color32::from_rgb(247, 129, 191),
        Color32::from_rgb(153, 153, 153),
        Color32::from_rgb(23, 190, 207),
    ];
    interpolate_colors(&colors, count)
}

fn get_set2_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(102, 194, 165),
        Color32::from_rgb(252, 141, 98),
        Color32::from_rgb(141, 160, 203),
        Color32::from_rgb(231, 138, 195),
        Color32::from_rgb(166, 216, 84),
        Color32::from_rgb(255, 217, 47),
        Color32::from_rgb(229, 196, 148),
        Color32::from_rgb(179, 179, 179),
    ];
    interpolate_colors(&colors, count)
}

fn get_set3_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(141, 211, 199),
        Color32::from_rgb(255, 255, 179),
        Color32::from_rgb(190, 186, 218),
        Color32::from_rgb(251, 128, 114),
        Color32::from_rgb(128, 177, 211),
        Color32::from_rgb(253, 180, 98),
        Color32::from_rgb(179, 222, 105),
        Color32::from_rgb(252, 205, 229),
        Color32::from_rgb(217, 217, 217),
        Color32::from_rgb(188, 128, 189),
        Color32::from_rgb(204, 235, 197),
        Color32::from_rgb(255, 237, 111),
    ];
    interpolate_colors(&colors, count)
}

fn get_tab10_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(31, 119, 180),
        Color32::from_rgb(255, 127, 14),
        Color32::from_rgb(44, 160, 44),
        Color32::from_rgb(214, 39, 40),
        Color32::from_rgb(148, 103, 189),
        Color32::from_rgb(140, 86, 75),
        Color32::from_rgb(227, 119, 194),
        Color32::from_rgb(127, 127, 127),
        Color32::from_rgb(188, 189, 34),
        Color32::from_rgb(23, 190, 207),
    ];
    interpolate_colors(&colors, count)
}

fn get_tab20_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(31, 119, 180),
        Color32::from_rgb(174, 199, 232),
        Color32::from_rgb(255, 127, 14),
        Color32::from_rgb(255, 187, 120),
        Color32::from_rgb(44, 160, 44),
        Color32::from_rgb(152, 223, 138),
        Color32::from_rgb(214, 39, 40),
        Color32::from_rgb(255, 152, 150),
        Color32::from_rgb(148, 103, 189),
        Color32::from_rgb(197, 176, 213),
        Color32::from_rgb(140, 86, 75),
        Color32::from_rgb(196, 156, 148),
        Color32::from_rgb(227, 119, 194),
        Color32::from_rgb(247, 182, 210),
        Color32::from_rgb(127, 127, 127),
        Color32::from_rgb(199, 199, 199),
        Color32::from_rgb(188, 189, 34),
        Color32::from_rgb(219, 219, 141),
        Color32::from_rgb(23, 190, 207),
        Color32::from_rgb(158, 218, 229),
    ];
    interpolate_colors(&colors, count)
}

fn get_tab20b_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(57, 59, 121),
        Color32::from_rgb(82, 84, 163),
        Color32::from_rgb(107, 110, 207),
        Color32::from_rgb(156, 158, 222),
        Color32::from_rgb(99, 121, 57),
        Color32::from_rgb(140, 162, 82),
        Color32::from_rgb(181, 207, 107),
        Color32::from_rgb(206, 219, 156),
        Color32::from_rgb(140, 109, 49),
        Color32::from_rgb(189, 158, 57),
        Color32::from_rgb(231, 186, 82),
        Color32::from_rgb(231, 203, 148),
        Color32::from_rgb(132, 60, 57),
        Color32::from_rgb(173, 73, 74),
        Color32::from_rgb(214, 97, 107),
        Color32::from_rgb(231, 150, 156),
        Color32::from_rgb(123, 65, 115),
        Color32::from_rgb(165, 81, 148),
        Color32::from_rgb(206, 109, 189),
        Color32::from_rgb(222, 158, 214),
    ];
    interpolate_colors(&colors, count)
}

fn get_tab20c_colors(count: usize) -> Vec<Color32> {
    let colors = vec![
        Color32::from_rgb(49, 130, 189),
        Color32::from_rgb(107, 174, 214),
        Color32::from_rgb(158, 202, 225),
        Color32::from_rgb(198, 219, 239),
        Color32::from_rgb(230, 85, 13),
        Color32::from_rgb(253, 141, 60),
        Color32::from_rgb(253, 174, 107),
        Color32::from_rgb(253, 208, 162),
        Color32::from_rgb(49, 163, 84),
        Color32::from_rgb(116, 196, 118),
        Color32::from_rgb(161, 217, 155),
        Color32::from_rgb(199, 233, 192),
        Color32::from_rgb(117, 107, 177),
        Color32::from_rgb(158, 154, 200),
        Color32::from_rgb(188, 189, 220),
        Color32::from_rgb(218, 218, 235),
        Color32::from_rgb(99, 99, 99),
        Color32::from_rgb(150, 150, 150),
        Color32::from_rgb(189, 189, 189),
        Color32::from_rgb(217, 217, 217),
    ];
    interpolate_colors(&colors, count)
}

fn interpolate_colors(colors: &[Color32], count: usize) -> Vec<Color32> {
    if count == 0 {
        return vec![];
    }
    if count == 1 {
        return vec![colors[0]];
    }
    if count <= colors.len() {
        return colors[..count].to_vec();
    }
    
    let mut result = Vec::with_capacity(count);
    let step = (colors.len() - 1) as f64 / (count - 1) as f64;
    
    for i in 0..count {
        let pos = i as f64 * step;
        let idx = pos.floor() as usize;
        let frac = pos - pos.floor();
        
        if idx >= colors.len() - 1 {
            result.push(colors[colors.len() - 1]);
        } else {
            let c1 = colors[idx];
            let c2 = colors[idx + 1];
            
            let r = (c1.r() as f64 * (1.0 - frac) + c2.r() as f64 * frac) as u8;
            let g = (c1.g() as f64 * (1.0 - frac) + c2.g() as f64 * frac) as u8;
            let b = (c1.b() as f64 * (1.0 - frac) + c2.b() as f64 * frac) as u8;
            
            result.push(Color32::from_rgb(r, g, b));
        }
    }
    
    result
} 