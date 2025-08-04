# Enhanced Grouping Feature Design

## Overview
Transform the current "Add Row ID Columns" feature into a powerful "Add Grouping & IDs" tool that can create hierarchical auto-incrementing IDs based on flexible grouping conditions.

## Current State Analysis

### Existing "Add Row ID Columns" Dialog
- Simple checkbox list for selecting grouping columns
- Creates basic row IDs and group IDs
- Limited to value-based grouping only
- No preview or statistics

### Proposed Enhancement
Replace with a rule-based system that supports:
- Multiple grouping strategies (value change, empty values, thresholds)
- Hierarchical ID generation (block_id, group_id, sequence_id)
- Live preview and statistics
- Save/load configurations

## Implementation Approach

### 1. Core Data Structures

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupingCondition {
    ValueChanges,        // New group when value differs from previous
    IsEmpty,            // New group when value is null/empty
    Equals(String),     // New group when value equals specific string
    TimeGap(f64),       // New group when time gap exceeds threshold
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupingRule {
    pub column: String,
    pub condition: GroupingCondition,
    pub level: GroupingLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupingLevel {
    Block,    // Top-level grouping
    Group,    // Mid-level grouping
    Sequence, // Row-level sequencing
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupingConfig {
    pub rules: Vec<GroupingRule>,
    pub output_columns: OutputColumns,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputColumns {
    pub block_id: Option<String>,    // Custom name or None to skip
    pub group_id: Option<String>,    
    pub sequence_id: Option<String>,
}
```

### 2. UI Components

#### Rule Builder
```rust
impl GroupingDialog {
    fn show_rule_builder(&mut self, ui: &mut egui::Ui) {
        ui.heading("Grouping Rules");
        
        for (idx, rule) in self.rules.iter_mut().enumerate() {
            ui.horizontal(|ui| {
                // Level selector
                egui::ComboBox::from_id_source(format!("level_{}", idx))
                    .selected_text(format!("{:?}", rule.level))
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut rule.level, GroupingLevel::Block, "Block");
                        ui.selectable_value(&mut rule.level, GroupingLevel::Group, "Group");
                    });
                
                // Column selector
                egui::ComboBox::from_id_source(format!("col_{}", idx))
                    .selected_text(&rule.column)
                    .show_ui(ui, |ui| {
                        for col in &self.available_columns {
                            ui.selectable_value(&mut rule.column, col.clone(), col);
                        }
                    });
                
                // Condition selector
                self.show_condition_selector(ui, &mut rule.condition, idx);
                
                // Remove button
                if ui.button("🗑️").clicked() {
                    self.rules_to_remove.push(idx);
                }
            });
        }
        
        if ui.button("+ Add Rule").clicked() {
            self.rules.push(GroupingRule::default());
        }
    }
}
```

#### Preview Panel
```rust
fn show_preview(&mut self, ui: &mut egui::Ui) {
    if let Some(preview) = &self.preview_result {
        ui.heading("Preview");
        
        // Statistics
        ui.label(format!("Blocks detected: {}", preview.block_count));
        ui.label(format!("Groups detected: {}", preview.group_count));
        ui.label(format!("Average group size: {:.1} rows", preview.avg_group_size));
        
        // Sample data
        egui::ScrollArea::vertical()
            .max_height(200.0)
            .show(ui, |ui| {
                self.show_preview_table(ui, &preview.sample_rows);
            });
    }
}
```

### 3. Processing Engine

```rust
impl GroupingEngine {
    pub fn apply_grouping(
        &self,
        batch: &RecordBatch,
        config: &GroupingConfig,
    ) -> Result<RecordBatch> {
        let mut processor = GroupingProcessor::new(batch.num_rows());
        
        // Process each row
        for row_idx in 0..batch.num_rows() {
            for rule in &config.rules {
                if self.should_start_new_group(batch, row_idx, rule)? {
                    processor.start_new_group(rule.level);
                }
            }
            processor.increment_sequence();
        }
        
        // Build output columns
        let mut new_columns = batch.columns().to_vec();
        let mut new_fields = batch.schema().fields().to_vec();
        
        if let Some(name) = &config.output_columns.block_id {
            new_columns.push(processor.get_block_ids());
            new_fields.push(Arc::new(Field::new(name, DataType::Int64, false)));
        }
        
        if let Some(name) = &config.output_columns.group_id {
            new_columns.push(processor.get_group_ids());
            new_fields.push(Arc::new(Field::new(name, DataType::Int64, false)));
        }
        
        if let Some(name) = &config.output_columns.sequence_id {
            new_columns.push(processor.get_sequence_ids());
            new_fields.push(Arc::new(Field::new(name, DataType::Int64, false)));
        }
        
        RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns)
    }
}
```

## Use Cases

### 1. Time-Based Sessions
```
Rule: good_time - Time gap > 3600 seconds
Output: session_id, event_sequence
Use: Identify user sessions with 1-hour inactivity timeout
```

### 2. Data Batch Detection
```
Rule 1: dumb_time - Is empty (Block level)
Rule 2: good_time - Changes (Group level)
Output: batch_id, measurement_id, reading_num
Use: Parse structured data with batch delimiters
```

### 3. State Change Tracking
```
Rule: status_column - Changes
Output: state_id, duration_counter
Use: Track how long system stays in each state
```

### 4. Hierarchical Categories
```
Rule 1: category_major - Changes (Block)
Rule 2: category_minor - Changes (Group)
Output: major_group, minor_group, item_sequence
Use: Multi-level product categorization
```

## Benefits Over Current System

1. **Flexibility**: Handle any grouping scenario without code changes
2. **Clarity**: Rules explicitly show grouping logic
3. **Power**: Create complex hierarchical structures
4. **Usability**: Preview results before applying
5. **Reusability**: Save and share configurations

## Integration Points

### 1. File Management
- Create new Arrow file with suffix indicating transformation
- Store configuration as metadata
- Enable transformation history

### 2. Query Integration
- New columns immediately available for queries
- Support for GROUP BY operations on generated IDs
- Enable window functions over groups

### 3. Export Options
- Include grouping columns in exports
- Preserve hierarchy in nested formats (JSON)
- Generate summary statistics

## Future Enhancements

1. **Expression Support**: Custom expressions for complex conditions
2. **Multi-Column Rules**: Group by combinations of columns
3. **Pattern Matching**: Regex or wildcard matching
4. **Aggregate Conditions**: Group when SUM/COUNT exceeds threshold
5. **Time Windows**: Rolling or tumbling time windows
6. **ML Integration**: Automatic group detection based on patterns

## Migration Path

1. Keep existing "Add Row ID" as "Simple Row IDs" option
2. Add new "Advanced Grouping" option
3. Gradually migrate users to new system
4. Deprecate old system after transition period

This enhanced grouping feature would significantly improve Leaf's data analysis capabilities, making it easier to identify patterns, create hierarchies, and prepare data for advanced analytics.