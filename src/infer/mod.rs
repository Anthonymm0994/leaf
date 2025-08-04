use serde::{Deserialize, Serialize};
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Real,
    Text,
    Boolean,
    Date,
    DateTime,
    TimeSeconds,
    TimeMilliseconds,
    TimeMicroseconds,
    TimeNanoseconds,
    Blob,
}

impl ColumnType {
    pub fn to_sql_type(&self) -> &'static str {
        match self {
            ColumnType::Integer => "INTEGER",
            ColumnType::Real => "DOUBLE",
            ColumnType::Text => "VARCHAR",
            ColumnType::Boolean => "BOOLEAN",
            ColumnType::Date => "DATE",
            ColumnType::DateTime => "TIMESTAMP",
            ColumnType::TimeSeconds => "TIMESTAMP",
            ColumnType::TimeMilliseconds => "TIMESTAMP",
            ColumnType::TimeMicroseconds => "TIMESTAMP",
            ColumnType::TimeNanoseconds => "TIMESTAMP",
            ColumnType::Blob => "BLOB",
        }
    }

    pub fn to_arrow_type(&self) -> DataType {
        match self {
            ColumnType::Integer => DataType::Int64,
            ColumnType::Real => DataType::Float64,
            ColumnType::Text => DataType::Utf8,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Date => DataType::Date32,
            ColumnType::DateTime => DataType::Timestamp(TimeUnit::Second, None),
            ColumnType::TimeSeconds => DataType::Utf8,
            ColumnType::TimeMilliseconds => DataType::Utf8,
            ColumnType::TimeMicroseconds => DataType::Utf8,
            ColumnType::TimeNanoseconds => DataType::Utf8,
            ColumnType::Blob => DataType::Binary,
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            ColumnType::Integer => "Integer (64-bit)",
            ColumnType::Real => "Float (64-bit)",
            ColumnType::Text => "Text",
            ColumnType::Boolean => "Boolean",
            ColumnType::Date => "Date",
            ColumnType::DateTime => "Time",
            ColumnType::TimeSeconds => "Time (seconds)",
            ColumnType::TimeMilliseconds => "Time (milliseconds)",
            ColumnType::TimeMicroseconds => "Time (microseconds)",
            ColumnType::TimeNanoseconds => "Time (nanoseconds)",
            ColumnType::Blob => "Binary",
        }
    }

    pub fn is_time_type(&self) -> bool {
        matches!(self,
            ColumnType::TimeSeconds |
            ColumnType::TimeMilliseconds |
            ColumnType::TimeMicroseconds |
            ColumnType::TimeNanoseconds
        )
    }

    // Check if a value can be converted to this type
    pub fn can_parse_value(&self, value: &str) -> bool {
        if value.is_empty() || value.to_lowercase() == "null" {
            return true; // Null values are always valid
        }

        match self {
            ColumnType::Integer => value.parse::<i64>().is_ok(),
            ColumnType::Real => value.parse::<f64>().is_ok(),
            ColumnType::Text => true, // Any string is valid text
            ColumnType::Boolean => {
                let lower = value.to_lowercase();
                matches!(lower.as_str(), "true" | "false" | "1" | "0" | "yes" | "no" | "y" | "n")
            },
            ColumnType::Date => TypeInferrer::is_date(value),
            ColumnType::DateTime => TypeInferrer::is_datetime(value),
            ColumnType::TimeSeconds => TypeInferrer::is_timestamp_seconds(value),
            ColumnType::TimeMilliseconds => TypeInferrer::is_timestamp_milliseconds(value),
            ColumnType::TimeMicroseconds => TypeInferrer::is_timestamp_microseconds(value),
            ColumnType::TimeNanoseconds => TypeInferrer::is_timestamp_nanoseconds(value),
            ColumnType::Blob => true, // Assume any string can be binary
        }
    }
}

pub struct TypeInferrer;

impl TypeInferrer {
    pub fn infer_column_types(
        headers: &[String],
        samples: &[Vec<String>],
    ) -> Vec<(String, ColumnType)> {
        headers
            .iter()
            .enumerate()
            .map(|(idx, header)| {
                let column_type = Self::infer_column_type(header, samples, idx);
                (header.clone(), column_type)
            })
            .collect()
    }

    pub fn infer_column_types_with_nulls(
        headers: &[String],
        samples: &[Vec<String>],
        null_values: &[String],
    ) -> Vec<(String, ColumnType)> {
        headers
            .iter()
            .enumerate()
            .map(|(idx, header)| {
                let column_type = Self::infer_column_type_with_nulls(header, samples, idx, null_values);
                (header.clone(), column_type)
            })
            .collect()
    }
    
    fn infer_column_type(header: &str, samples: &[Vec<String>], col_idx: usize) -> ColumnType {
        // First check header for time unit hints
        if let Some(time_type) = Self::detect_time_unit_from_header(header) {
            return time_type;
        }

        let mut is_int = true;
        let mut is_float = true;
        let mut is_bool = true;
        let mut is_date = true;
        let mut is_datetime = true;
        let mut is_time = true;
        let mut non_empty_count = 0;
        let mut has_decimal = false;
        
        for row in samples {
            if let Some(value) = row.get(col_idx) {
                if value.is_empty() || value.to_lowercase() == "null" {
                    continue;
                }
                
                non_empty_count += 1;
                
                // Check boolean
                if is_bool {
                    let lower = value.to_lowercase();
                    if !matches!(lower.as_str(), "true" | "false" | "1" | "0" | "yes" | "no" | "y" | "n") {
                        is_bool = false;
                    }
                }
                
                // Check integer
                if is_int {
                    if value.parse::<i64>().is_err() {
                        is_int = false;
                    }
                }
                
                // Check float (but track if we see decimals)
                if is_float {
                    match value.parse::<f64>() {
                        Ok(_) => {
                            if value.contains('.') {
                                has_decimal = true;
                            }
                        }
                        Err(_) => is_float = false,
                    }
                }
                
                // Check date
                if is_date && !Self::is_date(value) {
                    is_date = false;
                }
                
                // Check datetime
                if is_datetime && !Self::is_datetime(value) {
                    is_datetime = false;
                }
                
                // Check time (HH:MM:SS format)
                if is_time && !Self::is_time(value) {
                    is_time = false;
                }
            }
        }
        
        // Return the most specific type that matches
        if non_empty_count == 0 {
            ColumnType::Text
        } else if is_bool {
            ColumnType::Boolean
        } else if is_int {
            ColumnType::Integer
        } else if is_float {
            // Only use Real if we actually saw decimal values
            if has_decimal {
                ColumnType::Real
            } else {
                ColumnType::Integer
            }
        } else if is_datetime {
            ColumnType::DateTime
        } else if is_date {
            ColumnType::Date
        } else if is_time {
            // Determine the appropriate time unit based on the data
            Self::detect_time_unit_from_data(samples, col_idx)
        } else {
            ColumnType::Text
        }
    }

    fn infer_column_type_with_nulls(header: &str, samples: &[Vec<String>], col_idx: usize, null_values: &[String]) -> ColumnType {
        // First check header for time unit hints
        if let Some(time_type) = Self::detect_time_unit_from_header(header) {
            return time_type;
        }

        let mut is_int = true;
        let mut is_float = true;
        let mut is_bool = true;
        let mut is_date = true;
        let mut is_datetime = true;
        let mut is_time = true;
        let mut non_empty_count = 0;
        let mut has_decimal = false;
        
        for row in samples {
            if let Some(value) = row.get(col_idx) {
                // Check if value is a null value (case-insensitive)
                let is_null = value.is_empty() || 
                    null_values.iter().any(|null_val| value.to_lowercase() == null_val.to_lowercase());
                
                if is_null {
                    continue;
                }
                
                non_empty_count += 1;
                
                // Check boolean
                if is_bool {
                    let lower = value.to_lowercase();
                    if !matches!(lower.as_str(), "true" | "false" | "1" | "0" | "yes" | "no" | "y" | "n") {
                        is_bool = false;
                    }
                }
                
                // Check integer
                if is_int {
                    if value.parse::<i64>().is_err() {
                        is_int = false;
                    }
                }
                
                // Check float (but track if we see decimals)
                if is_float {
                    match value.parse::<f64>() {
                        Ok(_) => {
                            if value.contains('.') {
                                has_decimal = true;
                            }
                        }
                        Err(_) => is_float = false,
                    }
                }
                
                // Check date
                if is_date && !Self::is_date(value) {
                    is_date = false;
                }
                
                // Check datetime
                if is_datetime && !Self::is_datetime(value) {
                    is_datetime = false;
                }
                
                // Check time (HH:MM:SS format)
                if is_time && !Self::is_time(value) {
                    is_time = false;
                }
            }
        }
        
        // Return the most specific type that matches
        if non_empty_count == 0 {
            ColumnType::Text
        } else if is_bool {
            ColumnType::Boolean
        } else if is_int {
            ColumnType::Integer
        } else if is_float {
            // Only use Real if we actually saw decimal values
            if has_decimal {
                ColumnType::Real
            } else {
                ColumnType::Integer
            }
        } else if is_datetime {
            ColumnType::DateTime
        } else if is_date {
            ColumnType::Date
        } else if is_time {
            // Determine the appropriate time unit based on the data
            Self::detect_time_unit_from_data(samples, col_idx)
        } else {
            ColumnType::Text
        }
    }

    fn detect_time_unit_from_header(header: &str) -> Option<ColumnType> {
        let header_lower = header.to_lowercase();
        
        // Check for time unit indicators in header
        if header_lower.contains("(s)") || header_lower.contains("(sec)") || 
           header_lower.contains("seconds") || header_lower.contains("sec") {
            return Some(ColumnType::TimeSeconds);
        }
        
        if header_lower.contains("(ms)") || header_lower.contains("(msec)") || 
           header_lower.contains("milliseconds") || header_lower.contains("msec") {
            return Some(ColumnType::TimeMilliseconds);
        }
        
        if header_lower.contains("(μs)") || header_lower.contains("(usec)") || 
           header_lower.contains("microseconds") || header_lower.contains("usec") {
            return Some(ColumnType::TimeMicroseconds);
        }
        
        if header_lower.contains("(ns)") || header_lower.contains("(nsec)") || 
           header_lower.contains("nanoseconds") || header_lower.contains("nsec") {
            return Some(ColumnType::TimeNanoseconds);
        }
        
        None
    }

    fn detect_time_unit_from_data(samples: &[Vec<String>], col_idx: usize) -> ColumnType {
        let mut max_fraction_digits = 0;
        
        for row in samples {
            if let Some(value) = row.get(col_idx) {
                if value.is_empty() || value.to_lowercase() == "null" {
                    continue;
                }
                
                // Check for fractional part in time format
                let parts: Vec<&str> = value.split(':').collect();
                if parts.len() == 3 {
                    let seconds_part = parts[2];
                    if let Some(dot_pos) = seconds_part.find('.') {
                        let fraction_str = &seconds_part[dot_pos + 1..];
                        max_fraction_digits = max_fraction_digits.max(fraction_str.len());
                    }
                }
            }
        }
        
        // Determine time unit based on fraction digits
        match max_fraction_digits {
            0 => ColumnType::TimeSeconds,      // No fraction
            1..=3 => ColumnType::TimeMilliseconds,  // 1-3 digits (e.g., .3, .32, .325)
            4..=6 => ColumnType::TimeMicroseconds,  // 4-6 digits (e.g., .3250, .325000)
            _ => ColumnType::TimeSeconds,      // Default fallback
        }
    }
    
    fn is_date(value: &str) -> bool {
        // Common date formats
        let formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%d-%m-%Y",
            "%m-%d-%Y",
        ];
        for format in &formats {
            if parse_date(value, format) {
                return true;
            }
        }
        false
    }

    fn is_datetime(value: &str) -> bool {
        // Common datetime formats
        let formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%.fZ",
            "%Y/%m/%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M",
        ];
        for format in &formats {
            if parse_datetime(value, format) {
                return true;
            }
        }
        // Check for Unix timestamp (seconds or milliseconds)
        if let Ok(ts) = value.parse::<i64>() {
            if ts > 946684800 && ts < 4102444800 {
                // Between 2000 and 2100 in seconds
                return true;
            }
            if ts > 946684800000 && ts < 4102444800000 {
                // Between 2000 and 2100 in milliseconds
                return true;
            }
        }
        false
    }

    fn is_time(value: &str) -> bool {
        // Check for HH:MM:SS format (with optional milliseconds/microseconds)
        let parts: Vec<&str> = value.split(':').collect();
        if parts.len() == 3 {
            // Should be HH:MM:SS or HH:MM:SS.mmm or HH:MM:SS.mmmmmm
            if let (Ok(hour), Ok(minute)) = (
                parts[0].parse::<u8>(),
                parts[1].parse::<u8>()
            ) {
                if hour >= 24 || minute >= 60 {
                    return false;
                }
                
                // Parse seconds part (may include milliseconds/microseconds)
                let seconds_part = parts[2];
                if let Some(dot_pos) = seconds_part.find('.') {
                    // Has fractional part
                    let seconds_str = &seconds_part[..dot_pos];
                    let fraction_str = &seconds_part[dot_pos + 1..];
                    
                    if let Ok(second) = seconds_str.parse::<u8>() {
                        if second >= 60 {
                            return false;
                        }
                        
                        // Check if fraction is valid (1-6 digits for milliseconds/microseconds)
                        if fraction_str.len() >= 1 && fraction_str.len() <= 6 && 
                           fraction_str.chars().all(|c| c.is_ascii_digit()) {
                            return true;
                        }
                    }
                } else {
                    // No fractional part
                    if let Ok(second) = seconds_part.parse::<u8>() {
                        return second < 60;
                    }
                }
            }
        } else if parts.len() == 2 {
            // Should be HH:MM
            if let (Ok(hour), Ok(minute)) = (
                parts[0].parse::<u8>(),
                parts[1].parse::<u8>()
            ) {
                return hour < 24 && minute < 60;
            }
        }
        false
    }

    fn is_timestamp_seconds(value: &str) -> bool {
        if let Ok(ts) = value.parse::<i64>() {
            // Reasonable range for seconds since epoch (1970-2100)
            ts >= 0 && ts < 4102444800
        } else {
            false
        }
    }

    fn is_timestamp_milliseconds(value: &str) -> bool {
        if let Ok(ts) = value.parse::<i64>() {
            // Reasonable range for milliseconds since epoch (1970-2100)
            ts >= 0 && ts < 4102444800000
        } else {
            false
        }
    }

    fn is_timestamp_microseconds(value: &str) -> bool {
        if let Ok(ts) = value.parse::<i64>() {
            // Reasonable range for microseconds since epoch (1970-2100)
            ts >= 0 && ts < 4102444800000000
        } else {
            false
        }
    }

    fn is_timestamp_nanoseconds(value: &str) -> bool {
        if let Ok(ts) = value.parse::<i64>() {
            // Reasonable range for nanoseconds since epoch (1970-2100)
            ts >= 0 && ts < 4102444800000000000
        } else {
            false
        }
    }

    // Validate that all values in a column can be converted to the target type
    pub fn validate_column_type_change(
        samples: &[Vec<String>],
        col_idx: usize,
        target_type: &ColumnType,
    ) -> Result<(), String> {
        let mut invalid_values = Vec::new();
        
        for (row_idx, row) in samples.iter().enumerate() {
            if let Some(value) = row.get(col_idx) {
                if !value.is_empty() && value.to_lowercase() != "null" {
                    if !target_type.can_parse_value(value) {
                        invalid_values.push((row_idx + 1, value.clone()));
                        if invalid_values.len() >= 5 {
                            // Limit to first 5 invalid values to avoid overwhelming the user
                            break;
                        }
                    }
                }
            }
        }
        
        if !invalid_values.is_empty() {
            let examples = invalid_values
                .iter()
                .map(|(row, val)| format!("row {}: '{}'", row, val))
                .collect::<Vec<_>>()
                .join(", ");
            
            Err(format!(
                "Cannot convert column to {}: invalid values found ({})",
                target_type.display_name(),
                examples
            ))
        } else {
            Ok(())
        }
    }
}

// Minimal date/datetime parser for common formats (no chrono)
fn parse_date(value: &str, format: &str) -> bool {
    // Only check length and digit/sep pattern for now
    let cleaned = value.replace(['-', '/', ' '], "");
    match format {
        "%Y-%m-%d" | "%Y/%m/%d" => cleaned.len() == 8 && cleaned.chars().all(|c| c.is_ascii_digit()),
        "%d/%m/%Y" | "%m/%d/%Y" | "%d-%m-%Y" | "%m-%d-%Y" => cleaned.len() == 8 && cleaned.chars().all(|c| c.is_ascii_digit()),
        _ => false,
    }
}

fn parse_datetime(value: &str, format: &str) -> bool {
    // Only check length and digit/sep pattern for now
    let cleaned = value.replace(['-', '/', ' ', ':', 'T', 'Z', '.'], "");
    match format {
        "%Y-%m-%d %H:%M:%S" | "%Y-%m-%dT%H:%M:%S" | "%Y/%m/%d %H:%M:%S" | "%d/%m/%Y %H:%M:%S" | "%m/%d/%Y %H:%M:%S" => cleaned.len() >= 14 && cleaned.chars().all(|c| c.is_ascii_digit()),
        _ => false,
    }
} 