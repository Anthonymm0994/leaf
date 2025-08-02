use std::sync::Arc;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use crate::core::error::Result;
use crate::ui::time_bin_dialog::{TimeBinStrategy as GroupingStrategy, TimeBinConfig as TimeBasedGroupingConfig};

pub struct TimeGroupingEngine;

impl TimeGroupingEngine {
    /// Apply time-based grouping to a table
    pub fn apply_grouping(
        database: &Arc<crate::core::database::Database>,
        config: &TimeBasedGroupingConfig,
        output_dir: &std::path::Path,
    ) -> Result<String> {
        // Get the table data
        let query = format!("SELECT * FROM \"{}\"", config.selected_table);
        let rows = database.execute_query(&query)?;
        
        // Get column names for the table
        let column_names = database.get_column_names(&query)?;
        
        // Extract time column data
        let time_column_idx = column_names
            .iter()
            .position(|name| name == &config.selected_column)
            .ok_or_else(|| crate::core::error::LeafError::Custom(format!("Time column '{}' not found", config.selected_column)))?;

        // Parse time values and create groups
        let groups = Self::create_groups(&rows, time_column_idx, &config.strategy)?;
        
        // Create new table with grouping column
        let output_table_name = format!("{}_grouped", config.selected_table);
        Self::create_grouped_table(database, &rows, &column_names, &groups, &config.output_column_name, &output_table_name, output_dir)?;
        
        Ok(output_table_name)
    }

    /// Create groups based on the selected strategy
    fn create_groups(
        rows: &[Vec<String>],
        time_column_idx: usize,
        strategy: &GroupingStrategy,
    ) -> Result<Vec<i64>> {
        let mut groups = Vec::with_capacity(rows.len());
        
        match strategy {
            GroupingStrategy::FixedInterval { interval_seconds, .. } => {
                Self::create_fixed_interval_groups(rows, time_column_idx, *interval_seconds, &mut groups)?;
            }
            GroupingStrategy::ManualIntervals { intervals, .. } => {
                Self::create_manual_interval_groups(rows, time_column_idx, intervals, &mut groups)?;
            }
            GroupingStrategy::ThresholdBased { threshold_seconds, .. } => {
                Self::create_threshold_based_groups(rows, time_column_idx, *threshold_seconds, &mut groups)?;
            }
        }
        
        Ok(groups)
    }

    /// Create groups using fixed time intervals
    fn create_fixed_interval_groups(
        rows: &[Vec<String>],
        time_column_idx: usize,
        interval_seconds: u64,
        groups: &mut Vec<i64>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        
        let mut current_group = 0i64;
        let mut last_time: Option<i64> = None;
        
        for row in rows {
            let time_str = &row[time_column_idx];
            let timestamp = Self::parse_timestamp(time_str)?;
            
            if let Some(last) = last_time {
                let time_diff = timestamp - last;
                if time_diff >= interval_seconds as i64 {
                    current_group += 1;
                }
            }
            
            groups.push(current_group);
            last_time = Some(timestamp);
        }
        
        Ok(())
    }

    /// Create groups using manual interval boundaries
    fn create_manual_interval_groups(
        rows: &[Vec<String>],
        time_column_idx: usize,
        intervals: &[String],
        groups: &mut Vec<i64>,
    ) -> Result<()> {
        // Parse interval boundaries
        let mut boundaries = Vec::new();
        for interval in intervals {
            let seconds = Self::parse_time_format(interval)?;
            boundaries.push(seconds);
        }
        boundaries.sort();
        
        if rows.is_empty() {
            return Ok(());
        }
        
        // Get the first timestamp to establish a baseline
        let first_time_str = &rows[0][time_column_idx];
        let first_timestamp = Self::parse_timestamp(first_time_str)?;
        
        for row in rows {
            let time_str = &row[time_column_idx];
            let timestamp = Self::parse_timestamp(time_str)?;
            
            // Calculate time difference from the first timestamp
            let time_diff = timestamp - first_timestamp;
            
            // Find which interval this timestamp belongs to
            let group = boundaries
                .iter()
                .position(|&boundary| time_diff <= boundary as i64)
                .unwrap_or(boundaries.len()) as i64;
            
            groups.push(group);
        }
        
        Ok(())
    }

    /// Create groups based on time gaps (threshold-based)
    fn create_threshold_based_groups(
        rows: &[Vec<String>],
        time_column_idx: usize,
        threshold_seconds: u64,
        groups: &mut Vec<i64>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        
        let mut current_group = 0i64;
        let mut last_time: Option<i64> = None;
        
        for row in rows {
            let time_str = &row[time_column_idx];
            let timestamp = Self::parse_timestamp(time_str)?;
            
            if let Some(last) = last_time {
                let time_diff = timestamp - last;
                if time_diff > threshold_seconds as i64 {
                    current_group += 1;
                }
            }
            
            groups.push(current_group);
            last_time = Some(timestamp);
        }
        
        Ok(())
    }

    /// Create a new table with the grouping column added
    fn create_grouped_table(
        database: &Arc<crate::core::database::Database>,
        original_rows: &[Vec<String>],
        column_names: &[String],
        groups: &[i64],
        group_column_name: &str,
        output_table_name: &str,
        output_dir: &std::path::Path,
    ) -> Result<()> {
        // Create new rows with grouping column
        let mut new_rows = Vec::new();
        for (i, row) in original_rows.iter().enumerate() {
            let mut new_row = row.clone();
            new_row.push(groups[i].to_string());
            new_rows.push(new_row);
        }
        
        // Create new column names
        let mut new_column_names = column_names.to_vec();
        new_column_names.push(group_column_name.to_string());
        
        // Ensure output directory exists
        if !output_dir.exists() {
            std::fs::create_dir_all(output_dir)
                .map_err(|e| crate::core::error::LeafError::Custom(format!("Failed to create output directory: {}", e)))?;
        }
        
        // Create a new database instance for the output
        let mut new_db = crate::core::database::Database::open_writable(output_dir)?;
        
        // Create a temporary CSV file with the new data
        let temp_csv_path = output_dir.join(format!("{}.csv", output_table_name));
        let mut csv_writer = csv::Writer::from_path(&temp_csv_path)
            .map_err(|e| crate::core::error::LeafError::Custom(format!("Failed to create CSV writer: {}", e)))?;
        
        // Write header
        csv_writer.write_record(&new_column_names)
            .map_err(|e| crate::core::error::LeafError::Custom(format!("Failed to write CSV header: {}", e)))?;
        
        // Write data rows
        for row in &new_rows {
            csv_writer.write_record(row)
                .map_err(|e| crate::core::error::LeafError::Custom(format!("Failed to write CSV row: {}", e)))?;
        }
        
        csv_writer.flush()
            .map_err(|e| crate::core::error::LeafError::Custom(format!("Failed to flush CSV writer: {}", e)))?;
        
        // Import the CSV with automatic type inference
        new_db.stream_insert_csv(output_table_name, &temp_csv_path, ',', true)?;
        
        // Save the table as an Arrow file
        let output_path = output_dir.join(format!("{}.arrow", output_table_name));
        new_db.save_table_arrow_ipc(output_table_name, &output_path)?;
        
        // Clean up temporary CSV file
        let _ = std::fs::remove_file(&temp_csv_path);
        
        println!("Created time bin table '{}' with {} rows and {} columns", 
                output_table_name, new_rows.len(), new_column_names.len());
        println!("Saved to: {}", output_path.display());
        
        Ok(())
    }

    /// Parse timestamp string to seconds since epoch
    fn parse_timestamp(time_str: &str) -> Result<i64> {
        // Handle empty strings
        if time_str.trim().is_empty() {
            return Err(crate::core::error::LeafError::Custom("Empty timestamp string".to_string()));
        }
        
        // Try different timestamp formats
        if let Ok(timestamp) = time_str.parse::<i64>() {
            return Ok(timestamp);
        }
        
        // Try ISO 8601 format
        if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(time_str) {
            return Ok(datetime.timestamp());
        }
        
        // Try naive datetime formats
        let formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%H:%M:%S",
            "%H:%M",
        ];
        
        for format in &formats {
            if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(time_str, format) {
                return Ok(datetime.timestamp());
            }
        }
        
        // Try time-only format (assume today's date)
        if let Ok(time) = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S") {
            let today = chrono::Utc::now().date_naive();
            let datetime = today.and_time(time);
            return Ok(datetime.timestamp());
        }
        
        if let Ok(time) = chrono::NaiveTime::parse_from_str(time_str, "%H:%M") {
            let today = chrono::Utc::now().date_naive();
            let datetime = today.and_time(time);
            return Ok(datetime.timestamp());
        }
        
        Err(crate::core::error::LeafError::Custom(format!("Unable to parse timestamp: '{}'. Supported formats: Unix timestamp, ISO 8601, YYYY-MM-DD HH:MM:SS, HH:MM:SS, HH:MM", time_str)))
    }

    /// Parse time format string to seconds
    fn parse_time_format(time_str: &str) -> Result<u64> {
        // Parse HH:MM:SS format
        let parts: Vec<&str> = time_str.split(':').collect();
        match parts.len() {
            1 => time_str.parse::<u64>().map_err(|e| crate::core::error::LeafError::Custom(format!("Invalid time format: {}", e))),
            2 => {
                let minutes: u64 = parts[0].parse().map_err(|e| crate::core::error::LeafError::Custom(format!("Invalid minutes: {}", e)))?;
                let seconds: u64 = parts[1].parse().map_err(|e| crate::core::error::LeafError::Custom(format!("Invalid seconds: {}", e)))?;
                Ok(minutes * 60 + seconds)
            }
            3 => {
                let hours: u64 = parts[0].parse().map_err(|e| crate::core::error::LeafError::Custom(format!("Invalid hours: {}", e)))?;
                let minutes: u64 = parts[1].parse().map_err(|e| crate::core::error::LeafError::Custom(format!("Invalid minutes: {}", e)))?;
                let seconds: u64 = parts[2].parse().map_err(|e| crate::core::error::LeafError::Custom(format!("Invalid seconds: {}", e)))?;
                Ok(hours * 3600 + minutes * 60 + seconds)
            }
            _ => Err(crate::core::error::LeafError::Custom(format!("Invalid time format: {}", time_str))),
        }
    }
} 