# Leaf Transformation System Redesign

## Overview
Redesign the transformation tools to be more intuitive and consistent, with proper data management through Arrow file versioning.

## Core Principles

### 1. **Non-Destructive Transformations**
- All transformations create new Arrow files in the data folder
- Original data is never modified
- Each transformation creates a versioned file (e.g., `data_v1.arrow`, `data_v2.arrow`)
- Transformation history is preserved

### 2. **Consistent UI/UX**
- All transformation dialogs follow similar patterns
- Clear preview of changes before applying
- Consistent naming and terminology

## Proposed Tool Structure

### Tools Menu Reorganization

```
Tools
├── 🔍 Detect Duplicate Blocks (keep as-is)
├── ➕ Add Columns
│   ├── Computed Columns (formerly "Add Derived Field")
│   │   ├── Delta (difference between rows)
│   │   ├── Cumulative Sum
│   │   ├── Percentage
│   │   ├── Ratio
│   │   └── Custom Expression
│   ├── Time-Based Columns
│   │   ├── Time Bins (current "Add Time Bin Column")
│   │   ├── Time Components (extract hour, minute, etc.)
│   │   └── Time Differences
│   └── Grouping & IDs
│       ├── Simple Row IDs
│       ├── Group IDs (enhanced version)
│       └── Hierarchical IDs
└── 🔄 Transform Data
    ├── Filter Rows
    ├── Sort Data
    └── Aggregate Groups
```

## Detailed Window Designs

### 1. **Computed Columns Window**

```
┌─ Add Computed Column ─────────────────────────────────┐
│                                                        │
│ Computation Type: [Delta ▼]                            │
│                                                        │
│ ┌─ Delta Configuration ──────────────────────────────┐ │
│ │ Source Column: [width ▼]                           │ │
│ │ □ Only numeric columns shown                       │ │
│ │                                                    │ │
│ │ Delta Type:                                        │ │
│ │ ⚪ Row-to-Row (current - previous)                 │ │
│ │ ⚪ From First Row (current - first)                │ │
│ │ ⚪ Custom Offset: [1] rows                         │ │
│ │                                                    │ │
│ │ Output Column Name: [width_delta___________]      │ │
│ │                                                    │ │
│ │ Null Handling:                                     │ │
│ │ ⚪ Skip nulls (use last valid value)              │ │
│ │ ⚪ Propagate nulls                                 │ │
│ └────────────────────────────────────────────────────┘ │
│                                                        │
│ Preview:                                               │
│ ┌──────────┬──────────┬─────────────┐                │
│ │ Row      │ width    │ width_delta │                │
│ ├──────────┼──────────┼─────────────┤                │
│ │ 1        │ 63.78    │ NULL        │                │
│ │ 2        │ 116.97   │ 53.19       │                │
│ │ 3        │ 194.03   │ 77.06       │                │
│ └──────────┴──────────┴─────────────┘                │
│                                                        │
│ Output: test_data_300k_v2.arrow                       │
│                                                        │
│ [Apply] [Cancel]                                       │
└────────────────────────────────────────────────────────┘
```

### 2. **Enhanced Group IDs Window**

```
┌─ Add Grouping & IDs ───────────────────────────────────┐
│                                                         │
│ ID Type: [Hierarchical Groups ▼]                       │
│                                                         │
│ ┌─ Grouping Rules ──────────────────────────────────┐ │
│ │ Define when to start new groups:                   │ │
│ │                                                    │ │
│ │ Level 1 (Blocks):                                  │ │
│ │ [dumb_time ▼] [is empty ▼] ──────── [🗑️]         │ │
│ │                                                    │ │
│ │ Level 2 (Groups):                                  │ │
│ │ [good_time ▼] [changes ▼] ──────── [🗑️]          │ │
│ │                                                    │ │
│ │ [+ Add Rule]                                       │ │
│ │                                                    │ │
│ │ Conditions:                                        │ │
│ │ • changes         - Value different from previous  │ │
│ │ • is empty        - Value is null or empty string  │ │
│ │ • equals [value]  - Value matches specific value   │ │
│ │ • gap > [seconds] - Time gap exceeds threshold     │ │
│ └────────────────────────────────────────────────────┘ │
│                                                         │
│ ┌─ Output Columns ──────────────────────────────────┐ │
│ │ ☑ block_id    - Increments on Level 1 rules       │ │
│ │ ☑ group_id    - Increments on Level 2 rules       │ │
│ │ ☑ sequence_id - Row number within groups          │ │
│ │ □ Custom prefix: [____________]                   │ │
│ └────────────────────────────────────────────────────┘ │
│                                                         │
│ Statistics:                                             │
│ • 32 blocks detected                                    │
│ • 3,355 groups detected                                 │
│ • Average group size: 3.0 rows                         │
│                                                         │
│ Output: test_data_300k_groups.arrow                    │
│                                                         │
│ [Apply] [Save Config] [Cancel]                          │
└─────────────────────────────────────────────────────────┘
```

### 3. **Time Bins Window** (Current design is good, minor updates)

```
┌─ Add Time Bin Column ──────────────────────────────────┐
│                                                         │
│ Select Table: [test_data_300k ▼]                       │
│                                                         │
│ Select Time Column:                                     │
│ [good_time ▼]                                          │
│                                                         │
│ Time Bin Strategy:                                      │
│ ⚪ Fixed Interval   ⚪ Manual Intervals   ⚪ Threshold  │
│                                                         │
│ Fixed interval time bins:                               │
│ Enter interval in seconds or HH:MM:SS format:           │
│ Interval: [10_______]                                   │
│ Current interval: 10 seconds                            │
│                                                         │
│ Output Configuration:                                   │
│ Output column name: [time_bin_10s________]             │
│ □ Add bin start time column                            │
│ □ Add bin end time column                              │
│                                                         │
│ Preview: 1,847 unique bins detected                     │
│                                                         │
│ Output: test_data_300k_time_binned.arrow               │
│                                                         │
│ [Add Time Bin Column] [Cancel]                          │
└─────────────────────────────────────────────────────────┘
```

## File Management Strategy

### Naming Convention
```
Original: test_data_300k.arrow
After transformations:
├── test_data_300k_delta_width.arrow
├── test_data_300k_groups_v1.arrow
├── test_data_300k_time_bins_10s.arrow
└── test_data_300k_filtered_cat_a.arrow
```

### Metadata Storage
Each transformation should store metadata:
```json
{
  "source_file": "test_data_300k.arrow",
  "transformation": {
    "type": "delta",
    "config": {
      "column": "width",
      "method": "row-to-row"
    }
  },
  "timestamp": "2024-01-15T10:30:00Z",
  "row_count": 300000,
  "columns_added": ["width_delta"]
}
```

## Implementation Priorities

### Phase 1: Core Infrastructure
1. Update "Add Derived Field" to "Computed Columns" with dropdown
2. Implement file versioning system
3. Add preview functionality to all dialogs

### Phase 2: Enhanced Group IDs
1. Replace current row ID dialog with rule-based system
2. Add hierarchical ID generation
3. Implement save/load configurations

### Phase 3: Extended Transformations
1. Add more computation types (cumulative, percentage, ratio)
2. Add time component extraction
3. Add custom expression support

## Benefits

1. **Data Safety**: Original data never modified
2. **Traceability**: Clear transformation history
3. **Flexibility**: Easy to try different transformations
4. **Consistency**: Similar UI patterns across all tools
5. **Power**: Advanced grouping and computation options

## UI/UX Guidelines

1. **Preview First**: Always show preview before applying
2. **Clear Naming**: Use descriptive names for outputs
3. **Smart Defaults**: Suggest column names based on operation
4. **Validation**: Check for name conflicts and invalid operations
5. **Progress**: Show progress for long operations

## Future Enhancements

1. **Transformation Pipeline**: Chain multiple transformations
2. **Undo/Redo**: Revert to previous versions
3. **Templates**: Save common transformation patterns
4. **Batch Processing**: Apply same transformation to multiple files
5. **Visual Builder**: Drag-and-drop transformation designer