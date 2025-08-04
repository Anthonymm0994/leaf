use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use datafusion::arrow::array::{StringArray, Int32Array, Int64Array, Float64Array, BooleanArray, TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray, TimestampNanosecondArray, Date32Array, UInt32Array, Array};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::compute;
use crate::core::error::Result;

/// Configuration for duplicate group detection
#[derive(Debug, Clone)]
pub struct DuplicateDetectionConfig {
    /// Column name to group by
    pub group_column: String,
    /// Column names to ignore during comparison
    pub ignore_columns: HashSet<String>,
    /// Whether to treat null values as equal
    pub null_equals_null: bool,
}

impl Default for DuplicateDetectionConfig {
    fn default() -> Self {
        Self {
            group_column: String::new(),
            ignore_columns: HashSet::new(),
            null_equals_null: true,
        }
    }
}

/// Represents a group of rows that are duplicates
#[derive(Debug, Clone)]
pub struct DuplicateGroup {
    /// The hash of this group
    pub group_hash: u64,
    /// Group ID this group belongs to
    pub group_id: String,
    /// Row indices where this group appears (multiple if duplicates exist)
    pub row_indices: Vec<Vec<usize>>,
    /// Number of rows in this group
    pub group_size: usize,
}

/// Results of duplicate detection
#[derive(Debug, Clone)]
pub struct DuplicateDetectionResult {
    /// All duplicate groups found
    pub duplicate_groups: Vec<DuplicateGroup>,
    /// Total number of duplicate groups found
    pub total_duplicates: usize,
    /// Total number of rows that are part of duplicate groups
    pub total_duplicate_rows: usize,
    /// Statistics about the detection
    pub stats: DetectionStats,
}

#[derive(Debug, Clone)]
pub struct DetectionStats {
    /// Number of groups processed
    pub groups_processed: usize,
    /// Total number of groups analyzed
    pub groups_analyzed: usize,
    /// Number of unique groups found
    pub unique_groups: usize,
}

/// Main duplicate detector implementation
pub struct DuplicateDetector {
    config: DuplicateDetectionConfig,
}

impl DuplicateDetector {
    pub fn new(config: DuplicateDetectionConfig) -> Self {
        Self { config }
    }

    /// Detect duplicate groups in a record batch using content-based comparison
    pub fn detect_duplicates(&self, batch: &RecordBatch) -> Result<DuplicateDetectionResult> {
        let mut duplicate_groups = Vec::new();
        let mut stats = DetectionStats {
            groups_processed: 0,
            groups_analyzed: 0,
            unique_groups: 0,
        };

        // Get the group column index
        let group_col_idx = batch.schema()
            .fields()
            .iter()
            .position(|field| field.name() == &self.config.group_column)
            .ok_or_else(|| crate::core::error::LeafError::Custom(
                format!("Group column '{}' not found", self.config.group_column)
            ))?;

        // Get column indices to ignore (including the group column itself)
        let mut ignore_indices: HashSet<usize> = batch.schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| self.config.ignore_columns.contains(field.name()))
            .map(|(idx, _)| idx)
            .collect();
        
        // Always ignore the group column itself for content comparison
        ignore_indices.insert(group_col_idx);

        // Group rows by group_id
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        
        // Get the group column
        let group_col = batch.column(group_col_idx);
        
        // Group rows by their group_id (support both string and integer columns)
        match group_col.data_type() {
            DataType::Utf8 => {
                let group_array = group_col.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to StringArray".to_string()))?;
                for (row_idx, group_id) in group_array.iter().enumerate() {
                    if let Some(group_id) = group_id {
                        groups.entry(group_id.to_string()).or_insert_with(Vec::new).push(row_idx);
                    }
                }
            }
            DataType::Int32 => {
                let group_array = group_col.as_any().downcast_ref::<Int32Array>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to Int32Array".to_string()))?;
                for row_idx in 0..group_array.len() {
                    if !group_array.is_null(row_idx) {
                        let group_id = group_array.value(row_idx).to_string();
                        groups.entry(group_id).or_insert_with(Vec::new).push(row_idx);
                    }
                }
            }
            DataType::Int64 => {
                let group_array = group_col.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to Int64Array".to_string()))?;
                for row_idx in 0..group_array.len() {
                    if !group_array.is_null(row_idx) {
                        let group_id = group_array.value(row_idx).to_string();
                        groups.entry(group_id).or_insert_with(Vec::new).push(row_idx);
                    }
                }
            }
            _ => {
                return Err(crate::core::error::LeafError::Custom(
                    format!("Group column must be string or integer type, found: {:?}", group_col.data_type())
                ));
            }
        }

        stats.groups_processed = groups.len();

        // Convert groups to sorted list for sequential comparison
        let mut group_list: Vec<(String, Vec<usize>)> = groups.into_iter().collect();
        group_list.sort_by(|a, b| a.0.cmp(&b.0)); // Sort by group_id for consistent ordering

        let mut processed_groups = HashSet::new();
        let mut unique_group_hashes = HashSet::new();
        let mut content_hash_to_groups: HashMap<u64, Vec<(String, Vec<usize>)>> = HashMap::new();

        // First pass: compute content hashes for all groups
        for (group_id, rows) in &group_list {
            let content_hash = self.compute_group_hash(batch, rows, &ignore_indices)?;
            content_hash_to_groups.entry(content_hash).or_insert_with(Vec::new).push((group_id.clone(), rows.clone()));
        }

        // Second pass: find groups with identical content
        for (content_hash, groups_with_same_content) in content_hash_to_groups {
            if groups_with_same_content.len() > 1 {
                // We found duplicate groups!
                stats.groups_analyzed += groups_with_same_content.len();
                unique_group_hashes.insert(content_hash);
                
                // Create duplicate group entry
                let first_group = &groups_with_same_content[0];
                let group_size = first_group.1.len();
                let total_duplicate_rows: usize = groups_with_same_content.iter().map(|(_, rows)| rows.len()).sum();
                
                let row_indices: Vec<Vec<usize>> = groups_with_same_content.iter()
                    .map(|(_, rows)| rows.clone())
                    .collect();
                
                duplicate_groups.push(DuplicateGroup {
                    group_hash: content_hash,
                    group_id: first_group.0.clone(),
                    row_indices,
                    group_size,
                });
                
                // Mark all groups as processed
                for (group_id, _) in &groups_with_same_content {
                    processed_groups.insert(group_id.clone());
                }
            } else {
                // Single group with this content hash
                stats.groups_analyzed += 1;
                unique_group_hashes.insert(content_hash);
            }
        }

        stats.unique_groups = unique_group_hashes.len();
        
        let total_duplicates = duplicate_groups.len();
        let total_duplicate_rows = duplicate_groups.iter()
            .map(|group| group.row_indices.iter().map(|rows| rows.len()).sum::<usize>())
            .sum();

        Ok(DuplicateDetectionResult {
            duplicate_groups,
            total_duplicates,
            total_duplicate_rows,
            stats,
        })
    }

    /// Compute hash for a group of rows
    fn compute_group_hash(
        &self,
        batch: &RecordBatch,
        row_indices: &[usize],
        ignore_indices: &HashSet<usize>,
    ) -> Result<u64> {
        // For duplicate detection, we want to compare groups based on their content signature,
        // not the exact number of rows. We'll use the first row as the representative signature.
        // This allows groups with the same values but different row counts to be detected as duplicates.
        
        if row_indices.is_empty() {
            return Ok(0);
        }
        
        // Sort row indices to ensure we always use the same first row
        let mut sorted_indices = row_indices.to_vec();
        sorted_indices.sort();
        
        // Use only the first row as the group's signature
        let first_row_idx = sorted_indices[0];
        self.compute_row_hash(batch, first_row_idx, ignore_indices)
    }

    /// Compute hash for a single row
    fn compute_row_hash(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        ignore_indices: &HashSet<usize>,
    ) -> Result<u64> {
        let mut hasher = DefaultHasher::new();
        
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            if ignore_indices.contains(&col_idx) {
                continue;
            }
            
            let value = self.get_cell_value(batch, row_idx, col_idx)?;
            value.hash(&mut hasher);
        }
        
        Ok(hasher.finish())
    }

    /// Get cell value as string for hashing
    fn get_cell_value(&self, batch: &RecordBatch, row_idx: usize, col_idx: usize) -> Result<String> {
        let column = batch.column(col_idx);
        
        match column.data_type() {
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to StringArray".to_string()))?;
                Ok(array.value(row_idx).to_string())
            }
            DataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to Int32Array".to_string()))?;
                if array.is_null(row_idx) {
                    Ok("NULL".to_string())
                } else {
                    Ok(array.value(row_idx).to_string())
                }
            }
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to Int64Array".to_string()))?;
                if array.is_null(row_idx) {
                    Ok("NULL".to_string())
                } else {
                    Ok(array.value(row_idx).to_string())
                }
            }
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to Float64Array".to_string()))?;
                if array.is_null(row_idx) {
                    Ok("NULL".to_string())
                } else {
                    Ok(array.value(row_idx).to_string())
                }
            }
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to BooleanArray".to_string()))?;
                if array.is_null(row_idx) {
                    Ok("NULL".to_string())
                } else {
                    Ok(array.value(row_idx).to_string())
                }
            }
            DataType::Timestamp(unit, _) => {
                let value = match unit {
                    TimeUnit::Second => {
                        let array = column.as_any().downcast_ref::<TimestampSecondArray>()
                            .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to TimestampSecondArray".to_string()))?;
                        if array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array.value(row_idx).to_string()
                        }
                    }
                    TimeUnit::Millisecond => {
                        let array = column.as_any().downcast_ref::<TimestampMillisecondArray>()
                            .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to TimestampMillisecondArray".to_string()))?;
                        if array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array.value(row_idx).to_string()
                        }
                    }
                    TimeUnit::Microsecond => {
                        let array = column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                            .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to TimestampMicrosecondArray".to_string()))?;
                        if array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array.value(row_idx).to_string()
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let array = column.as_any().downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to TimestampNanosecondArray".to_string()))?;
                        if array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array.value(row_idx).to_string()
                        }
                    }
                };
                Ok(value)
            }
            DataType::Date32 => {
                let array = column.as_any().downcast_ref::<Date32Array>()
                    .ok_or_else(|| crate::core::error::LeafError::Custom("Failed to cast to Date32Array".to_string()))?;
                if array.is_null(row_idx) {
                    Ok("NULL".to_string())
                } else {
                    Ok(array.value(row_idx).to_string())
                }
            }
            _ => {
                // For other types, convert to string representation
                if column.is_null(row_idx) {
                    Ok("NULL".to_string())
                } else {
                    Ok(format!("{:?}", column))
                }
            }
        }
    }

    /// Create a new Arrow file with duplicate groups removed
    pub fn create_clean_arrow_file(
        &self,
        batch: &RecordBatch,
        result: &DuplicateDetectionResult,
        output_path: &std::path::Path,
    ) -> Result<usize> {
        // Collect all row indices to keep
        let mut rows_to_keep = Vec::new();
        let mut duplicate_rows_to_remove = HashSet::new();
        
        // For each duplicate group, keep the first occurrence and mark subsequent ones for removal
        for group in &result.duplicate_groups {
            if group.row_indices.len() > 1 {
                // Keep the first occurrence (index 0)
                for &row_idx in &group.row_indices[0] {
                    rows_to_keep.push(row_idx);
                }
                
                // Mark subsequent occurrences for removal
                for occurrence in group.row_indices.iter().skip(1) {
                    for &row_idx in occurrence {
                        duplicate_rows_to_remove.insert(row_idx);
                    }
                }
            } else {
                // Single occurrence, keep all rows
                for &row_idx in &group.row_indices[0] {
                    rows_to_keep.push(row_idx);
                }
            }
        }
        
        // Add all non-duplicate rows
        for row_idx in 0..batch.num_rows() {
            if !duplicate_rows_to_remove.contains(&row_idx) {
                // Check if this row is not already in rows_to_keep
                if !rows_to_keep.contains(&row_idx) {
                    rows_to_keep.push(row_idx);
                }
            }
        }
        
        // Sort to maintain original order
        rows_to_keep.sort();
        
        // Convert to UInt32Array for compute::take
        let keep_indices: Vec<u32> = rows_to_keep.iter().map(|&x| x as u32).collect();
        let keep_array = UInt32Array::from(keep_indices);
        
        // Take only the rows we want to keep
        let columns: Vec<_> = batch.columns().iter().map(|col| {
            compute::take(col, &keep_array, None)
        }).collect();
        
        // Collect results and handle errors
        let mut new_columns = Vec::new();
        for result in columns {
            new_columns.push(result?);
        }
        
        let new_batch = RecordBatch::try_new(batch.schema().clone(), new_columns)?;
        
        // Write to Arrow file
        let file = std::fs::File::create(output_path)?;
        let mut writer = datafusion::arrow::ipc::writer::FileWriter::try_new(file, &new_batch.schema())?;
        writer.write(&new_batch)?;
        writer.finish()?;
        
        Ok(rows_to_keep.len())
    }

    /// Create a new Arrow file with duplicates removed and return the path
    pub fn create_clean_arrow_file_with_path(
        &self,
        batch: &RecordBatch,
        result: &DuplicateDetectionResult,
        base_path: &std::path::Path,
        original_table_name: &str,
    ) -> Result<(std::path::PathBuf, usize)> {
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("{}_clean_{}.arrow", original_table_name, timestamp);
        let output_path = base_path.join(filename);
        
        let kept_rows = self.create_clean_arrow_file(batch, result, &output_path)?;
        
        Ok((output_path, kept_rows))
    }
} 