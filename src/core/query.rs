use crate::core::{Database, error::{Result, LeafError}};
use std::sync::Arc;
use datafusion::arrow::datatypes::DataType;

pub struct QueryExecutor;

impl QueryExecutor {
    pub fn execute(db: &Arc<Database>, query: &str) -> Result<QueryResult> {
        // Validate query in read-only mode
        if db.is_readonly() {
            Self::validate_read_only(query)?;
        }
        
        // Execute the query
        let rows = db.execute_query(query)?;
        let columns = db.get_column_names(query)?;
        let column_types = db.get_column_types(query)?;
        
        Ok(QueryResult {
            columns,
            column_types,
            rows,
            total_rows: None,
        })
    }
    
    pub fn execute_with_pagination(
        db: &Arc<Database>,
        query: &str,
        page: usize,
        page_size: usize,
    ) -> Result<QueryResult> {
        // Validate query in read-only mode
        if db.is_readonly() {
            Self::validate_read_only(query)?;
        }

        // Special case: if the query is a SELECT COUNT(*) query, use only execute_count_query
        let query_trimmed = query.trim().to_uppercase();
        if query_trimmed.starts_with("SELECT COUNT(*)") {
            let count = db.execute_count_query(query)?;
            return Ok(QueryResult {
                columns: vec!["COUNT(*)".to_string()],
                column_types: vec![DataType::Int64],
                rows: vec![vec![count.to_string()]],
                total_rows: Some(1),
            });
        }
        
        // First, get the total count for pagination
        // For simple SELECT * FROM table queries, we can get count directly
        let count_query = if query_trimmed.starts_with("SELECT * FROM") {
            // Extract table name from SELECT * FROM "table_name"
            if let Some(table_name_part) = query.split("FROM").nth(1) {
                // Extract just the table name, ignoring any WHERE/ORDER BY/GROUP BY/LIMIT clause
                let table_name_upper = table_name_part.to_uppercase();
                let end_pos = table_name_upper
                    .find(" WHERE ")
                    .or_else(|| table_name_upper.find(" ORDER "))
                    .or_else(|| table_name_upper.find(" GROUP "))
                    .or_else(|| table_name_upper.find(" LIMIT "))
                    .or_else(|| table_name_upper.find(" HAVING "))
                    .unwrap_or(table_name_part.len());
                
                let table_name = &table_name_part[..end_pos];
                let table_name = table_name.trim().trim_matches('"').trim_matches('\'');

                // Check if table exists
                match db.table_exists(table_name) {
                    Ok(exists) => {
                        if !exists {
                            return Err(LeafError::Custom(format!("Table '{}' does not exist", table_name)));
                        }
                    }
                    Err(e) => {
                        return Err(LeafError::Custom(format!("Error checking if table exists: {}", e)));
                    }
                }

                format!("SELECT COUNT(*) FROM \"{}\"", table_name)
            } else {
                format!("SELECT COUNT(*) FROM ({})", query)
            }
        } else {
            format!("SELECT COUNT(*) FROM ({})", query)
        };
        let total_rows = db.execute_count_query(&count_query)? as usize;

        // Then get the paginated results
        let paginated_query = format!("{} LIMIT {} OFFSET {}", query, page_size, page * page_size);
        let mut result = Self::execute(db, &paginated_query)?;
        result.total_rows = Some(total_rows);

        Ok(result)
    }
    
    pub fn validate_read_only(query: &str) -> Result<()> {
        let query_upper = query.to_uppercase();
        let forbidden_keywords = [
            "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", 
            "TRUNCATE", "REPLACE", "ATTACH", "DETACH", "PRAGMA"
        ];
        
        for keyword in &forbidden_keywords {
            if query_upper.contains(keyword) {
                return Err(LeafError::Custom(
                    format!("Query contains forbidden keyword '{}' in read-only mode", keyword)
                ));
            }
        }
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub column_types: Vec<DataType>,  // Add column type information
    pub rows: Vec<Vec<String>>, 
    pub total_rows: Option<usize>,
} 