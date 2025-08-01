//! Enhanced utilities for plot views
//! Based on frog-viz best practices with improved functionality

use egui::Color32;
use std::collections::HashMap;
use super::enhanced_config::ColorScheme;

/// Enhanced color utilities based on frog-viz patterns
pub fn categorical_color(index: usize) -> Color32 {
    let colors = vec![
        Color32::from_rgb(31, 119, 180),   // Blue
        Color32::from_rgb(255, 127, 14),   // Orange
        Color32::from_rgb(44, 160, 44),    // Green
        Color32::from_rgb(214, 39, 40),    // Red
        Color32::from_rgb(148, 103, 189),  // Purple
        Color32::from_rgb(140, 86, 75),    // Brown
        Color32::from_rgb(227, 119, 194),  // Pink
        Color32::from_rgb(127, 127, 127),  // Gray
        Color32::from_rgb(188, 189, 34),   // Olive
        Color32::from_rgb(23, 190, 207),   // Cyan
    ];
    colors[index % colors.len()]
}

/// Viridis color scheme for continuous data
pub fn viridis_color(value: f64) -> Color32 {
    // Viridis color mapping (simplified)
    let r = (0.267004 + 0.004874 * value + 0.213480 * value.powi(2) + 0.263415 * value.powi(3)) * 255.0;
    let g = (0.004874 + 0.864426 * value + 0.135580 * value.powi(2)) * 255.0;
    let b = (0.329415 + 0.670585 * value.powi(2)) * 255.0;
    
    Color32::from_rgb(
        r.clamp(0.0, 255.0) as u8,
        g.clamp(0.0, 255.0) as u8,
        b.clamp(0.0, 255.0) as u8,
    )
}

/// Plasma color scheme for continuous data
pub fn plasma_color(value: f64) -> Color32 {
    // Plasma color mapping (simplified)
    let r = (0.050383 + 0.088272 * value + 0.014243 * value.powi(2) + 0.847415 * value.powi(3)) * 255.0;
    let g = (0.029803 + 0.469501 * value + 0.500710 * value.powi(2)) * 255.0;
    let b = (0.527963 + 0.472037 * value.powi(2)) * 255.0;
    
    Color32::from_rgb(
        r.clamp(0.0, 255.0) as u8,
        g.clamp(0.0, 255.0) as u8,
        b.clamp(0.0, 255.0) as u8,
    )
}

/// Diverging color scheme for data with a center point
pub fn diverging_color(value: f64, center: f64, min_val: f64, max_val: f64) -> Color32 {
    let normalized = if value < center {
        (value - min_val) / (center - min_val)
    } else {
        (value - center) / (max_val - center)
    };
    
    if value < center {
        // Blue to white
        let intensity = (normalized * 255.0) as u8;
        Color32::from_rgb(intensity, intensity, 255)
    } else {
        // White to red
        let intensity = ((1.0 - normalized) * 255.0) as u8;
        Color32::from_rgb(255, intensity, intensity)
    }
}

/// Create a categorical color map for string values
pub fn create_categorical_color_map(values: &[String]) -> HashMap<String, Color32> {
    let mut color_map = HashMap::new();
    for (i, value) in values.iter().enumerate() {
        color_map.insert(value.clone(), categorical_color(i));
    }
    color_map
}

/// Enhanced statistical utilities based on frog-viz patterns
#[derive(Debug, Clone)]
pub struct Statistics {
    pub count: usize,
    pub mean: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub q1: f64,
    pub q3: f64,
}

/// Calculate comprehensive statistics for a dataset
pub fn calculate_statistics(values: &[f64]) -> Statistics {
    if values.is_empty() {
        return Statistics {
            count: 0,
            mean: 0.0,
            std_dev: 0.0,
            min: 0.0,
            max: 0.0,
            median: 0.0,
            q1: 0.0,
            q3: 0.0,
        };
    }
    
    let count = values.len();
    let sum: f64 = values.iter().sum();
    let mean = sum / count as f64;
    
    let variance = values.iter()
        .map(|x| (x - mean).powi(2))
        .sum::<f64>() / count as f64;
    let std_dev = variance.sqrt();
    
    let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    
    let mut sorted_values = values.to_vec();
    sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    
    let median = if count % 2 == 0 {
        (sorted_values[count / 2 - 1] + sorted_values[count / 2]) / 2.0
    } else {
        sorted_values[count / 2]
    };
    
    let q1_idx = count / 4;
    let q3_idx = 3 * count / 4;
    
    let q1 = sorted_values[q1_idx];
    let q3 = sorted_values[q3_idx];
    
    Statistics {
        count,
        mean,
        std_dev,
        min,
        max,
        median,
        q1,
        q3,
    }
}

/// Calculate quartiles for box plots
pub fn calculate_quartiles(values: &[f64]) -> (f64, f64, f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0, 0.0, 0.0);
    }
    
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    
    let n = sorted.len();
    let min = sorted[0];
    let max = sorted[n - 1];
    
    let q1_idx = n / 4;
    let median_idx = n / 2;
    let q3_idx = 3 * n / 4;
    
    let q1 = sorted[q1_idx];
    let median = if n % 2 == 0 {
        (sorted[median_idx - 1] + sorted[median_idx]) / 2.0
    } else {
        sorted[median_idx]
    };
    let q3 = sorted[q3_idx];
    
    (min, q1, median, q3, max)
}

/// Detect outliers using IQR method
pub fn detect_outliers_iqr(values: &[f64]) -> Vec<usize> {
    if values.len() < 4 {
        return vec![];
    }
    
    let (_, q1, _, q3, _) = calculate_quartiles(values);
    let iqr = q3 - q1;
    let lower_bound = q1 - 1.5 * iqr;
    let upper_bound = q3 + 1.5 * iqr;
    
    values.iter()
        .enumerate()
        .filter(|(_, &value)| value < lower_bound || value > upper_bound)
        .map(|(i, _)| i)
        .collect()
}

/// Detect outliers using z-score method
pub fn zscore_outliers(values: &[f64], threshold: f64) -> Vec<usize> {
    if values.is_empty() {
        return vec![];
    }
    
    let stats = calculate_statistics(values);
    let mean = stats.mean;
    let std_dev = stats.std_dev;
    
    if std_dev == 0.0 {
        return vec![];
    }
    
    values.iter()
        .enumerate()
        .filter(|(_, &value)| {
            let z_score = (value - mean).abs() / std_dev;
            z_score > threshold
        })
        .map(|(i, _)| i)
        .collect()
}

/// Extract numeric values from string data
pub fn extract_numeric_values(values: &[String]) -> Vec<f64> {
    values.iter()
        .filter_map(|s| s.parse::<f64>().ok())
        .collect()
}

/// Extract string values for categorical data
pub fn extract_string_values(values: &[String]) -> Vec<String> {
    values.iter()
        .filter(|s| !s.is_empty())
        .cloned()
        .collect()
}

/// Extract temporal values (simplified)
pub fn extract_temporal_values(values: &[String]) -> Vec<f64> {
    values.iter()
        .filter_map(|s| s.parse::<f64>().ok())
        .collect()
}

/// Professional color palette for categorical data
pub fn get_categorical_colors(scheme: &ColorScheme) -> Vec<Color32> {
    match scheme {
        ColorScheme::Viridis => vec![
            Color32::from_rgb(68, 1, 84),      // Dark purple
            Color32::from_rgb(59, 82, 139),    // Blue
            Color32::from_rgb(33, 145, 140),   // Teal
            Color32::from_rgb(94, 201, 98),    // Green
            Color32::from_rgb(253, 231, 37),   // Yellow
        ],
        ColorScheme::Plasma => vec![
            Color32::from_rgb(13, 8, 135),     // Dark blue
            Color32::from_rgb(84, 2, 163),     // Purple
            Color32::from_rgb(139, 10, 165),   // Magenta
            Color32::from_rgb(185, 19, 114),   // Pink
            Color32::from_rgb(219, 84, 77),    // Red
            Color32::from_rgb(249, 164, 63),   // Orange
            Color32::from_rgb(254, 255, 136),  // Yellow
        ],
        ColorScheme::Set1 => vec![
            Color32::from_rgb(228, 26, 28),    // Red
            Color32::from_rgb(55, 126, 184),   // Blue
            Color32::from_rgb(77, 175, 74),    // Green
            Color32::from_rgb(152, 78, 163),   // Purple
            Color32::from_rgb(255, 127, 0),    // Orange
            Color32::from_rgb(166, 86, 40),    // Brown
            Color32::from_rgb(247, 129, 191),  // Pink
            Color32::from_rgb(153, 153, 153),  // Gray
            Color32::from_rgb(23, 190, 207),   // Cyan
        ],
        ColorScheme::Set2 => vec![
            Color32::from_rgb(102, 194, 165),  // Teal
            Color32::from_rgb(252, 141, 98),   // Orange
            Color32::from_rgb(141, 160, 203),  // Blue
            Color32::from_rgb(231, 138, 195),  // Pink
            Color32::from_rgb(166, 216, 84),   // Green
            Color32::from_rgb(255, 217, 47),   // Yellow
            Color32::from_rgb(229, 196, 148),  // Tan
            Color32::from_rgb(179, 179, 179),  // Gray
        ],
        ColorScheme::Set3 => vec![
            Color32::from_rgb(141, 211, 199),  // Light teal
            Color32::from_rgb(255, 255, 179),  // Light yellow
            Color32::from_rgb(190, 186, 218),  // Light purple
            Color32::from_rgb(251, 128, 114),  // Light red
            Color32::from_rgb(128, 177, 211),  // Light blue
            Color32::from_rgb(253, 180, 98),   // Light orange
            Color32::from_rgb(179, 222, 105),  // Light green
            Color32::from_rgb(252, 205, 229),  // Light pink
            Color32::from_rgb(217, 217, 217),  // Light gray
            Color32::from_rgb(188, 128, 189),  // Medium purple
            Color32::from_rgb(204, 235, 197),  // Very light green
            Color32::from_rgb(255, 237, 111),  // Very light yellow
        ],
        _ => vec![
            // Enhanced default scheme with better contrast for bar charts
            Color32::from_rgb(31, 119, 180),   // Blue
            Color32::from_rgb(255, 127, 14),   // Orange
            Color32::from_rgb(44, 160, 44),    // Green
            Color32::from_rgb(214, 39, 40),    // Red
            Color32::from_rgb(148, 103, 189),  // Purple
            Color32::from_rgb(140, 86, 75),    // Brown
            Color32::from_rgb(227, 119, 194),  // Pink
            Color32::from_rgb(127, 127, 127),  // Gray
            Color32::from_rgb(188, 189, 34),   // Olive
            Color32::from_rgb(23, 190, 207),   // Cyan
            Color32::from_rgb(255, 187, 120),  // Light orange
            Color32::from_rgb(174, 199, 232),  // Light blue
            Color32::from_rgb(255, 152, 150),  // Light red
            Color32::from_rgb(152, 223, 138),  // Light green
            Color32::from_rgb(197, 176, 213),  // Light purple
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_categorical_color() {
        let color1 = categorical_color(0);
        let color2 = categorical_color(1);
        assert_ne!(color1, color2);
    }

    #[test]
    fn test_calculate_statistics() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let stats = calculate_statistics(&values);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.mean, 3.0);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 5.0);
    }

    #[test]
    fn test_outlier_detection() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 100.0]; // 100 is an outlier
        let outliers = detect_outliers_iqr(&values);
        assert!(!outliers.is_empty());
    }
} 