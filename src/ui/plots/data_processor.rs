//! DataFusion integration layer for efficient data processing
//! 
//! This module provides high-performance data processing capabilities
//! using DataFusion's columnar processing engine for plot data preparation.

use datafusion::prelude::*;
use datafusion::arrow::array::{Array, Float64Array, StringArray, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use crate::core::QueryResult;

/// DataFusion-powered data processor for plot operations
pub struct DataProcessor {
    context: SessionContext,
}

impl DataProcessor {
    /// Create a new DataProcessor with default configuration
    pub fn new() -> Self {
        let context = SessionContext::new();
        Self { context }
    }

    /// Convert QueryResult to Arrow RecordBatch for DataFusion processing
    fn query_result_to_record_batch(&self, data: &QueryResult) -> Result<RecordBatch, String> {
        if data.rows.is_empty() {
            return Err("No data to process".to_string());
        }

        let mut columns: Vec<Arc<dyn Array>> = Vec::new();
        
        for (col_idx, col_type) in data.column_types.iter().enumerate() {
            match col_type {
                DataType::Float64 | DataType::Float32 => {
                    let values: Result<Vec<f64>, _> = data.rows.iter()
                        .map(|row| {
                            if col_idx < row.len() {
                                row[col_idx].parse::<f64>()
                                    .map_err(|_| format!("Failed to parse '{}' as float", row[col_idx]))
                            } else {
                                Ok(0.0)
                            }
                        })
                        .collect();
                    
                    match values {
                        Ok(vals) => {
                            let array = Float64Array::from(vals);
                            columns.push(Arc::new(array));
                        }
                        Err(e) => return Err(e),
                    }
                }
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                    let values: Result<Vec<i64>, _> = data.rows.iter()
                        .map(|row| {
                            if col_idx < row.len() {
                                row[col_idx].parse::<i64>()
                                    .map_err(|_| format!("Failed to parse '{}' as integer", row[col_idx]))
                            } else {
                                Ok(0)
                            }
                        })
                        .collect();
                    
                    match values {
                        Ok(vals) => {
                            let array = Int64Array::from(vals);
                            columns.push(Arc::new(array));
                        }
                        Err(e) => return Err(e),
                    }
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let values: Vec<String> = data.rows.iter()
                        .map(|row| {
                            if col_idx < row.len() {
                                row[col_idx].clone()
                            } else {
                                String::new()
                            }
                        })
                        .collect();
                    
                    let array = StringArray::from(values);
                    columns.push(Arc::new(array));
                }
                _ => {
                    // Default to string for unsupported types
                    let values: Vec<String> = data.rows.iter()
                        .map(|row| {
                            if col_idx < row.len() {
                                row[col_idx].clone()
                            } else {
                                String::new()
                            }
                        })
                        .collect();
                    
                    let array = StringArray::from(values);
                    columns.push(Arc::new(array));
                }
            }
        }

        // Create schema
        let fields: Vec<Field> = data.columns.iter()
            .zip(data.column_types.iter())
            .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
            .collect();
        
        let schema = Arc::new(Schema::new(fields));
        
        RecordBatch::try_new(schema, columns)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }

    /// Aggregate data for bar charts - groups by category and sums values
    pub async fn aggregate_for_bar_chart(
        &self, 
        data: &QueryResult, 
        x_col: &str, 
        y_col: &str
    ) -> Result<Vec<(String, f64)>, String> {
        let batch = self.query_result_to_record_batch(data)?;
        
        // Register the batch as a temporary table
        let table_name = "temp_data";
        self.context.register_batch(table_name, batch)
            .map_err(|e| format!("Failed to register batch: {}", e))?;

        // Create aggregation query
        let sql = format!(
            "SELECT {}, SUM(CAST({} AS DOUBLE)) as sum_value FROM {} GROUP BY {} ORDER BY {}",
            x_col, y_col, table_name, x_col, x_col
        );

        // Execute query
        let df = self.context.sql(&sql).await
            .map_err(|e| format!("Failed to execute aggregation query: {}", e))?;
        
        let results = df.collect().await
            .map_err(|e| format!("Failed to collect results: {}", e))?;

        let mut aggregated_data = Vec::new();
        
        for batch in results {
            let category_array = batch.column(0);
            let value_array = batch.column(1);
            
            if let (Some(categories), Some(values)) = (
                category_array.as_any().downcast_ref::<StringArray>(),
                value_array.as_any().downcast_ref::<Float64Array>()
            ) {
                for i in 0..batch.num_rows() {
                    if !categories.is_null(i) && !values.is_null(i) {
                        let category = categories.value(i).to_string();
                        let value = values.value(i);
                        aggregated_data.push((category, value));
                    }
                }
            }
        }

        Ok(aggregated_data)
    }

    /// Compute histogram bins with automatic bin calculation
    pub async fn compute_histogram_bins(
        &self, 
        data: &QueryResult, 
        column: &str, 
        bins: usize
    ) -> Result<Vec<(f64, f64, usize)>, String> {
        let batch = self.query_result_to_record_batch(data)?;
        
        // Register the batch as a temporary table
        let table_name = "temp_data";
        self.context.register_batch(table_name, batch)
            .map_err(|e| format!("Failed to register batch: {}", e))?;

        // Get min and max values for bin calculation
        let stats_sql = format!(
            "SELECT MIN(CAST({} AS DOUBLE)) as min_val, MAX(CAST({} AS DOUBLE)) as max_val FROM {}",
            column, column, table_name
        );

        let stats_df = self.context.sql(&stats_sql).await
            .map_err(|e| format!("Failed to get statistics: {}", e))?;
        
        let stats_results = stats_df.collect().await
            .map_err(|e| format!("Failed to collect statistics: {}", e))?;

        if stats_results.is_empty() || stats_results[0].num_rows() == 0 {
            return Err("No data for histogram calculation".to_string());
        }

        let stats_batch = &stats_results[0];
        let min_array = stats_batch.column(0).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to get min value")?;
        let max_array = stats_batch.column(1).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to get max value")?;

        let min_val = min_array.value(0);
        let max_val = max_array.value(0);
        let bin_width = (max_val - min_val) / bins as f64;

        // Create histogram bins
        let mut histogram_data = Vec::new();
        
        for i in 0..bins {
            let bin_start = min_val + i as f64 * bin_width;
            let bin_end = if i == bins - 1 { max_val } else { bin_start + bin_width };
            
            // Count values in this bin
            let count_sql = format!(
                "SELECT COUNT(*) as count FROM {} WHERE CAST({} AS DOUBLE) >= {} AND CAST({} AS DOUBLE) {}",
                table_name, column, bin_start, column,
                if i == bins - 1 { format!("<= {}", bin_end) } else { format!("< {}", bin_end) }
            );

            let count_df = self.context.sql(&count_sql).await
                .map_err(|e| format!("Failed to count bin values: {}", e))?;
            
            let count_results = count_df.collect().await
                .map_err(|e| format!("Failed to collect count: {}", e))?;

            if !count_results.is_empty() && count_results[0].num_rows() > 0 {
                let count_batch = &count_results[0];
                let count_array = count_batch.column(0).as_any().downcast_ref::<Int64Array>()
                    .ok_or("Failed to get count value")?;
                
                let count = count_array.value(0) as usize;
                histogram_data.push((bin_start, bin_end, count));
            }
        }

        Ok(histogram_data)
    }

    /// Compute correlation matrix for multiple columns
    pub async fn compute_correlation_matrix(
        &self, 
        data: &QueryResult, 
        columns: &[String]
    ) -> Result<Vec<Vec<f64>>, String> {
        if columns.len() < 2 {
            return Err("Need at least 2 columns for correlation matrix".to_string());
        }

        let batch = self.query_result_to_record_batch(data)?;
        
        // Register the batch as a temporary table
        let table_name = "temp_data";
        self.context.register_batch(table_name, batch)
            .map_err(|e| format!("Failed to register batch: {}", e))?;

        let mut correlation_matrix = vec![vec![0.0; columns.len()]; columns.len()];

        // Calculate correlation for each pair of columns
        for (i, col1) in columns.iter().enumerate() {
            for (j, col2) in columns.iter().enumerate() {
                if i == j {
                    correlation_matrix[i][j] = 1.0; // Perfect correlation with self
                } else if i < j {
                    // Calculate correlation using Pearson correlation formula
                    let corr_sql = format!(
                        "SELECT 
                            (COUNT(*) * SUM(CAST({} AS DOUBLE) * CAST({} AS DOUBLE)) - SUM(CAST({} AS DOUBLE)) * SUM(CAST({} AS DOUBLE))) /
                            (SQRT(COUNT(*) * SUM(CAST({} AS DOUBLE) * CAST({} AS DOUBLE)) - SUM(CAST({} AS DOUBLE)) * SUM(CAST({} AS DOUBLE))) *
                             SQRT(COUNT(*) * SUM(CAST({} AS DOUBLE) * CAST({} AS DOUBLE)) - SUM(CAST({} AS DOUBLE)) * SUM(CAST({} AS DOUBLE))))
                            as correlation
                        FROM {}",
                        col1, col2, col1, col2,
                        col1, col1, col1, col1,
                        col2, col2, col2, col2,
                        table_name
                    );

                    let corr_df = self.context.sql(&corr_sql).await
                        .map_err(|e| format!("Failed to calculate correlation: {}", e))?;
                    
                    let corr_results = corr_df.collect().await
                        .map_err(|e| format!("Failed to collect correlation: {}", e))?;

                    if !corr_results.is_empty() && corr_results[0].num_rows() > 0 {
                        let corr_batch = &corr_results[0];
                        let corr_array = corr_batch.column(0).as_any().downcast_ref::<Float64Array>()
                            .ok_or("Failed to get correlation value")?;
                        
                        let correlation = if corr_array.is_null(0) { 0.0 } else { corr_array.value(0) };
                        correlation_matrix[i][j] = correlation;
                        correlation_matrix[j][i] = correlation; // Symmetric matrix
                    }
                }
            }
        }

        Ok(correlation_matrix)
    }

    /// Detect anomalies using statistical methods
    pub async fn detect_anomalies(
        &self, 
        data: &QueryResult, 
        column: &str, 
        method: AnomalyMethod
    ) -> Result<Vec<bool>, String> {
        let batch = self.query_result_to_record_batch(data)?;
        
        // Register the batch as a temporary table
        let table_name = "temp_data";
        self.context.register_batch(table_name, batch)
            .map_err(|e| format!("Failed to register batch: {}", e))?;

        match method {
            AnomalyMethod::ZScore { threshold } => {
                // Calculate mean and standard deviation
                let stats_sql = format!(
                    "SELECT AVG(CAST({} AS DOUBLE)) as mean_val, STDDEV(CAST({} AS DOUBLE)) as std_val FROM {}",
                    column, column, table_name
                );

                let stats_df = self.context.sql(&stats_sql).await
                    .map_err(|e| format!("Failed to get statistics: {}", e))?;
                
                let stats_results = stats_df.collect().await
                    .map_err(|e| format!("Failed to collect statistics: {}", e))?;

                if stats_results.is_empty() || stats_results[0].num_rows() == 0 {
                    return Err("No data for anomaly detection".to_string());
                }

                let stats_batch = &stats_results[0];
                let mean_array = stats_batch.column(0).as_any().downcast_ref::<Float64Array>()
                    .ok_or("Failed to get mean value")?;
                let std_array = stats_batch.column(1).as_any().downcast_ref::<Float64Array>()
                    .ok_or("Failed to get std value")?;

                let mean_val = mean_array.value(0);
                let std_val = std_array.value(0);

                // Calculate Z-scores and identify anomalies
                let anomaly_sql = format!(
                    "SELECT ABS((CAST({} AS DOUBLE) - {}) / {}) > {} as is_anomaly FROM {}",
                    column, mean_val, std_val, threshold, table_name
                );

                let anomaly_df = self.context.sql(&anomaly_sql).await
                    .map_err(|e| format!("Failed to detect anomalies: {}", e))?;
                
                let anomaly_results = anomaly_df.collect().await
                    .map_err(|e| format!("Failed to collect anomalies: {}", e))?;

                let mut anomalies = Vec::new();
                for batch in anomaly_results {
                    if let Some(anomaly_array) = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>() {
                        for i in 0..batch.num_rows() {
                            anomalies.push(!anomaly_array.is_null(i) && anomaly_array.value(i));
                        }
                    }
                }

                Ok(anomalies)
            }
            AnomalyMethod::IQR { multiplier } => {
                // Calculate quartiles
                let quartile_sql = format!(
                    "SELECT 
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST({} AS DOUBLE)) as q1,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST({} AS DOUBLE)) as q3
                    FROM {}",
                    column, column, table_name
                );

                let quartile_df = self.context.sql(&quartile_sql).await
                    .map_err(|e| format!("Failed to get quartiles: {}", e))?;
                
                let quartile_results = quartile_df.collect().await
                    .map_err(|e| format!("Failed to collect quartiles: {}", e))?;

                if quartile_results.is_empty() || quartile_results[0].num_rows() == 0 {
                    return Err("No data for IQR anomaly detection".to_string());
                }

                let quartile_batch = &quartile_results[0];
                let q1_array = quartile_batch.column(0).as_any().downcast_ref::<Float64Array>()
                    .ok_or("Failed to get Q1 value")?;
                let q3_array = quartile_batch.column(1).as_any().downcast_ref::<Float64Array>()
                    .ok_or("Failed to get Q3 value")?;

                let q1 = q1_array.value(0);
                let q3 = q3_array.value(0);
                let iqr = q3 - q1;
                let lower_bound = q1 - multiplier * iqr;
                let upper_bound = q3 + multiplier * iqr;

                // Identify outliers
                let anomaly_sql = format!(
                    "SELECT (CAST({} AS DOUBLE) < {} OR CAST({} AS DOUBLE) > {}) as is_anomaly FROM {}",
                    column, lower_bound, column, upper_bound, table_name
                );

                let anomaly_df = self.context.sql(&anomaly_sql).await
                    .map_err(|e| format!("Failed to detect anomalies: {}", e))?;
                
                let anomaly_results = anomaly_df.collect().await
                    .map_err(|e| format!("Failed to collect anomalies: {}", e))?;

                let mut anomalies = Vec::new();
                for batch in anomaly_results {
                    if let Some(anomaly_array) = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>() {
                        for i in 0..batch.num_rows() {
                            anomalies.push(!anomaly_array.is_null(i) && anomaly_array.value(i));
                        }
                    }
                }

                Ok(anomalies)
            }
        }
    }

    /// Compute box plot statistics (quartiles, median, outliers)
    pub async fn compute_box_plot_stats(
        &self, 
        data: &QueryResult, 
        column: &str, 
        group_by: Option<&str>
    ) -> Result<Vec<BoxPlotStats>, String> {
        let batch = self.query_result_to_record_batch(data)?;
        
        // Register the batch as a temporary table
        let table_name = "temp_data";
        self.context.register_batch(table_name, batch)
            .map_err(|e| format!("Failed to register batch: {}", e))?;

        let mut box_plot_stats = Vec::new();

        if let Some(group_col) = group_by {
            // Get unique groups
            let groups_sql = format!("SELECT DISTINCT {} FROM {} ORDER BY {}", group_col, table_name, group_col);
            let groups_df = self.context.sql(&groups_sql).await
                .map_err(|e| format!("Failed to get groups: {}", e))?;
            
            let groups_results = groups_df.collect().await
                .map_err(|e| format!("Failed to collect groups: {}", e))?;

            for batch in groups_results {
                if let Some(group_array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                    for i in 0..batch.num_rows() {
                        if !group_array.is_null(i) {
                            let group_name = group_array.value(i).to_string();
                            let stats = self.compute_single_box_plot_stats(table_name, column, Some(&group_name), group_col).await?;
                            box_plot_stats.push(stats);
                        }
                    }
                }
            }
        } else {
            // Single box plot for entire dataset
            let stats = self.compute_single_box_plot_stats(table_name, column, None, "").await?;
            box_plot_stats.push(stats);
        }

        Ok(box_plot_stats)
    }

    /// Helper method to compute box plot statistics for a single group
    async fn compute_single_box_plot_stats(
        &self,
        table_name: &str,
        column: &str,
        group_value: Option<&str>,
        group_column: &str,
    ) -> Result<BoxPlotStats, String> {
        let where_clause = if let Some(group_val) = group_value {
            format!("WHERE {} = '{}'", group_column, group_val)
        } else {
            String::new()
        };

        // Calculate quartiles and other statistics
        let stats_sql = format!(
            "SELECT 
                MIN(CAST({} AS DOUBLE)) as min_val,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST({} AS DOUBLE)) as q1,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST({} AS DOUBLE)) as median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST({} AS DOUBLE)) as q3,
                MAX(CAST({} AS DOUBLE)) as max_val,
                AVG(CAST({} AS DOUBLE)) as mean_val,
                COUNT(*) as count
            FROM {} {}",
            column, column, column, column, column, column, table_name, where_clause
        );

        let stats_df = self.context.sql(&stats_sql).await
            .map_err(|e| format!("Failed to get box plot statistics: {}", e))?;
        
        let stats_results = stats_df.collect().await
            .map_err(|e| format!("Failed to collect box plot statistics: {}", e))?;

        if stats_results.is_empty() || stats_results[0].num_rows() == 0 {
            return Err("No data for box plot statistics".to_string());
        }

        let stats_batch = &stats_results[0];
        let min_val = stats_batch.column(0).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to get min value")?.value(0);
        let q1 = stats_batch.column(1).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to get Q1 value")?.value(0);
        let median = stats_batch.column(2).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to get median value")?.value(0);
        let q3 = stats_batch.column(3).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to get Q3 value")?.value(0);
        let max_val = stats_batch.column(4).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to get max value")?.value(0);
        let mean_val = stats_batch.column(5).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to get mean value")?.value(0);
        let count = stats_batch.column(6).as_any().downcast_ref::<Int64Array>()
            .ok_or("Failed to get count value")?.value(0) as usize;

        // Calculate IQR and outlier bounds
        let iqr = q3 - q1;
        let lower_fence = q1 - 1.5 * iqr;
        let upper_fence = q3 + 1.5 * iqr;

        // Find outliers
        let outliers_sql = format!(
            "SELECT CAST({} AS DOUBLE) as outlier_value FROM {} {} AND (CAST({} AS DOUBLE) < {} OR CAST({} AS DOUBLE) > {})",
            column, table_name, where_clause, column, lower_fence, column, upper_fence
        );

        let outliers_df = self.context.sql(&outliers_sql).await
            .map_err(|e| format!("Failed to get outliers: {}", e))?;
        
        let outliers_results = outliers_df.collect().await
            .map_err(|e| format!("Failed to collect outliers: {}", e))?;

        let mut outliers = Vec::new();
        for batch in outliers_results {
            if let Some(outlier_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
                for i in 0..batch.num_rows() {
                    if !outlier_array.is_null(i) {
                        outliers.push(outlier_array.value(i));
                    }
                }
            }
        }

        Ok(BoxPlotStats {
            group: group_value.map(|s| s.to_string()),
            min: min_val,
            q1,
            median,
            q3,
            max: max_val,
            mean: mean_val,
            count,
            outliers,
            lower_fence,
            upper_fence,
        })
    }
}

impl Default for DataProcessor {
    fn default() -> Self {
        Self::new()
    }
}

/// Anomaly detection methods
#[derive(Debug, Clone)]
pub enum AnomalyMethod {
    ZScore { threshold: f64 },
    IQR { multiplier: f64 },
}

/// Box plot statistics for a single group or entire dataset
#[derive(Debug, Clone)]
pub struct BoxPlotStats {
    pub group: Option<String>,
    pub min: f64,
    pub q1: f64,
    pub median: f64,
    pub q3: f64,
    pub max: f64,
    pub mean: f64,
    pub count: usize,
    pub outliers: Vec<f64>,
    pub lower_fence: f64,
    pub upper_fence: f64,
}