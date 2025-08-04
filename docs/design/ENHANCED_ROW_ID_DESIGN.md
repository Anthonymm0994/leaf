# Enhanced Row ID Feature Design

## Overview
Transform the current "Add Row ID" dialog into a powerful "Group & Sequence IDs" feature that can create auto-incrementing IDs based on various grouping conditions.

## Core Concepts

### 1. **Group Detection Methods**

#### Value-Based Grouping
- **Consecutive Same Values**: Group rows where a column has the same value consecutively
  - Example: `good_time` repeats 3 times → group_id=1, then changes → group_id=2
  - Use case: Detecting measurement batches, time-based groups

#### Change-Based Grouping  
- **Value Change Detection**: New group starts when column value changes
  - Example: Every time `category_3` changes from 'a' to 'b', increment group
  - Use case: State transitions, category changes

#### Condition-Based Grouping
- **Empty/Null Detection**: New group when a column is empty/null
  - Example: When `dumb_time` is empty, start new group
  - Use case: Data segments, record boundaries
  
- **Threshold-Based**: New group when numeric value exceeds threshold
  - Example: When time gap > 1 hour, start new group
  - Use case: Session detection, outlier grouping

#### Multi-Column Grouping
- **Composite Keys**: Group by combination of columns
  - Example: Group by `category_3` + `category_4`
  - Use case: Multi-dimensional grouping

### 2. **ID Generation Strategies**

#### Sequential IDs
- **Global Sequential**: 1, 2, 3, 4... across entire dataset
- **Group Sequential**: Reset to 1 for each group
- **Nested Sequential**: Group ID + sequence within group

#### Hierarchical IDs
- **Block ID**: Major grouping (e.g., by empty dumb_time)
- **Group ID**: Sub-grouping within blocks (e.g., by good_time)
- **Row ID**: Sequence within groups

## Proposed UI Design

```
┌─ Group & Sequence IDs ─────────────────────────────┐
│                                                     │
│ ID Type: [Dropdown: Sequential/Hierarchical/Custom] │
│                                                     │
│ ┌─ Grouping Rules ────────────────────────────────┐ │
│ │ Rule 1: [Column ▼] [Condition ▼] [Value/Config] │ │
│ │         └─ Start new group when:                │ │
│ │            • Value changes                       │ │
│ │            • Value equals: [___]                 │ │
│ │            • Value is empty/null                 │ │
│ │            • Gap exceeds: [___] seconds          │ │
│ │                                                  │ │
│ │ [+ Add Rule]                                     │ │
│ └──────────────────────────────────────────────────┘ │
│                                                     │
│ ┌─ ID Configuration ──────────────────────────────┐ │
│ │ □ Block ID (top level)                          │ │
│ │   Column name: [block_id_________]              │ │
│ │   Reset on: [Rule 1 ▼]                          │ │
│ │                                                  │ │
│ │ ☑ Group ID (within blocks)                      │ │
│ │   Column name: [group_id_________]              │ │
│ │   Reset on: [Rule 2 ▼]                          │ │
│ │                                                  │ │
│ │ ☑ Sequence ID (within groups)                   │ │
│ │   Column name: [row_id___________]              │ │
│ │   □ Reset for each group                        │ │
│ └──────────────────────────────────────────────────┘ │
│                                                     │
│ Preview:                                            │
│ ┌─────────────┬───────┬───────┬────────┐          │
│ │ good_time   │ block │ group │ row_id │          │
│ ├─────────────┼───────┼───────┼────────┤          │
│ │ 00:00:00.1  │   1   │   1   │   1    │          │
│ │ 00:00:00.1  │   1   │   1   │   2    │          │
│ │ 00:00:00.2  │   1   │   2   │   1    │          │
│ │ 00:00:00.2  │   1   │   2   │   2    │          │
│ └─────────────┴───────┴───────┴────────┘          │
│                                                     │
│ [Apply] [Cancel]                                    │
└─────────────────────────────────────────────────────┘
```

## Implementation Plan

### Phase 1: Core Infrastructure
1. Create `GroupingRule` enum:
   ```rust
   pub enum GroupingRule {
       ValueChange { column: String },
       ValueEquals { column: String, value: String },
       ValueEmpty { column: String },
       TimeGap { column: String, threshold_seconds: f64 },
       Composite { columns: Vec<String> },
   }
   ```

2. Create `IdGenerationConfig`:
   ```rust
   pub struct IdGenerationConfig {
       pub rules: Vec<GroupingRule>,
       pub id_columns: Vec<IdColumnConfig>,
   }
   
   pub struct IdColumnConfig {
       pub name: String,
       pub level: IdLevel,
       pub reset_on_rule: Option<usize>,
   }
   
   pub enum IdLevel {
       Block,
       Group, 
       Sequence,
   }
   ```

### Phase 2: Detection Engine
1. **Rule Evaluator**: Process rules to detect group boundaries
2. **ID Generator**: Create hierarchical IDs based on detected groups
3. **Preview Generator**: Show sample results before applying

### Phase 3: UI Integration
1. Replace current row ID dialog with enhanced version
2. Add rule builder interface
3. Implement live preview
4. Save/load common configurations

## Example Use Cases

### 1. Time-Based Session Detection
```
Rule 1: good_time - Time gap > 3600 seconds
→ Creates session_id that increments after 1-hour gaps
```

### 2. Measurement Batch Detection
```
Rule 1: dumb_time - Value is empty
Rule 2: good_time - Value changes
→ Creates batch_id (increments on empty dumb_time)
→ Creates measurement_id (increments when good_time changes)
```

### 3. Category Transition Tracking
```
Rule 1: category_3 - Value changes
→ Creates transition_id that increments on category changes
→ Creates row_num that counts within each transition
```

### 4. Multi-Level Grouping
```
Rule 1: category_3 + category_4 - Composite value changes
Rule 2: good_time - Value changes
→ Creates category_group_id
→ Creates time_group_id within category groups
→ Creates sequence_id within time groups
```

## Benefits

1. **Flexibility**: Handle any grouping scenario
2. **Power**: Create complex hierarchical IDs
3. **Usability**: Visual rule builder with preview
4. **Reusability**: Save and share configurations
5. **Performance**: Efficient single-pass algorithm

## Technical Considerations

1. **Memory Efficiency**: Process in streaming fashion
2. **Type Safety**: Validate rules against column types
3. **Null Handling**: Consistent behavior for empty values
4. **Performance**: Optimize for large datasets
5. **Undo/Redo**: Support for reverting changes

## Future Enhancements

1. **Rule Templates**: Pre-built rules for common scenarios
2. **Expression Support**: Custom expressions for complex conditions
3. **Statistics**: Show group counts and distributions
4. **Export Rules**: Save as JSON/YAML for automation
5. **Validation**: Ensure ID uniqueness within scope