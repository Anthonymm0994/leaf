use crate::core::database::Database;
use crate::core::error::Result;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

impl Database {
    /// Load a table as an Arrow RecordBatch with optional row limit
    pub fn load_table_arrow(&mut self, table_name: &str, limit: Option<usize>) -> Result<RecordBatch> {
        // Build query with optional limit
        let query = if let Some(row_limit) = limit {
            format!("SELECT * FROM {} LIMIT {}", table_name, row_limit)
        } else {
            format!("SELECT * FROM {}", table_name)
        };
        
        // Execute query and get first batch
        let ctx = self.ctx.clone();
        
        let result = self.runtime.block_on(async {
            ctx.sql(&query).await
        }).map_err(|e| crate::core::error::LeafError::Custom(format!("Failed to execute query: {}", e)))?;
        
        let record_batches = self.runtime.block_on(async {
            result.collect().await
        }).map_err(|e| crate::core::error::LeafError::Custom(format!("Failed to collect results: {}", e)))?;
        
        if record_batches.is_empty() {
            return Err(crate::core::error::LeafError::Custom("No data found in table".to_string()));
        }
        
        // Return the first batch
        Ok(record_batches[0].clone())
    }
}