# Leaf Project Summary

## Project Overview

Leaf is a data analysis application built in Rust that provides powerful CSV processing, time-series analysis, and data transformation capabilities. The project uses the egui framework for its user interface and focuses on handling large datasets efficiently.

## Architecture & Key Components

### Core Systems

1. **CSV Handler** (`src/core/csv_handler.rs`)
   - Handles CSV import/export operations
   - Manages data parsing and validation
   - Key insight: Uses Rust's type system for robust data handling

2. **Database System** (`src/core/database.rs`)
   - In-memory data storage using Rust's efficient data structures
   - Supports multiple data types: Integer, Real, Text, Boolean, Date, DateTime, Time variants, Blob
   - Key insight: Designed for performance with large datasets

3. **Type Inference Engine** (`src/infer/mod.rs`)
   - Sophisticated type detection system
   - Handles multiple date/time formats
   - Supports precision levels from seconds to nanoseconds
   - Key insight: Critical for automatic data type detection in CSV imports

4. **Time Grouping System** (`src/core/time_grouping.rs`)
   - Groups data by time intervals
   - Handles time-based aggregations
   - Key insight: Essential for time-series analysis

5. **Duplicate Detection** (`src/core/duplicate_detector.rs`)
   - Identifies duplicate rows in datasets
   - Configurable detection strategies
   - Key insight: Important for data quality assurance

### UI Components

1. **Home Screen** - Main navigation hub
2. **CSV Import** - Single and multi-file import capabilities
3. **Query Window** - Data exploration interface
4. **Data Transformation** - Apply various transformations
5. **Time Binning Dialog** - Configure time-based grouping
6. **Export Dialog** - Export processed data

## Test Data Generation System

### What We Built

We created a comprehensive test data generation system with three Python scripts:

1. **`generate_test_data.py`** - Generates synthetic CSV data with:
   - Time columns with specific patterns (duplicates, hour gaps)
   - Numerical data with precise decimal places
   - Categorical columns with varying cardinality
   - Boolean columns with 50/50 distribution
   - Inference stress test columns (empty vs "-" values)
   - Statistical distribution columns (normal, bimodal, exponential, etc.)
   - Group-based data generation with duplication patterns

2. **`validate_test_data.py`** - Validates generated data for:
   - Time format correctness
   - Value ranges and data types
   - Group structure integrity
   - Missing value patterns
   - Distribution characteristics

3. **`analyze_datasets.py`** - Generates comprehensive statistics reports

### Test Datasets Created

| Dataset | Rows | File Size | Groups | Hour Gaps | Purpose |
|---------|------|-----------|--------|-----------|---------|
| `test_data_10k.csv` | 10,000 | 4.1 MB | 32 | 2 (6.5%) | Quick testing & development |
| `test_data_300k.csv` | 300,000 | 122 MB | 855 | 69 (8.1%) | Performance testing |
| `test_data_3m.csv` | 3,000,000 | 1.2 GB | 8,593 | 689 (8.0%) | Stress testing & scalability |

### Key Data Characteristics

1. **46 Columns Total**:
   - 2 time columns (good_time, dumb_time)
   - 3 numerical columns (width, height, angle)
   - 8 categorical columns (category_3 through category_10)
   - 5 boolean columns
   - 22 inference test columns (11 types × 2 variants)
   - 1 multi-value column (tags)
   - 5 distribution columns

2. **Special Features**:
   - ~10% missing values in inference columns
   - ~25% empty values in tags column
   - Hour-long gaps after ~10% of groups
   - Time values that wrap within 24-hour period
   - Groups of 200-500 rows each

## Key Learnings & Insights

### 1. Type Inference Complexity
- The system needs to handle 11 different data types
- Missing values can be represented as empty strings or specific markers like "-"
- Time precision varies from seconds to nanoseconds
- Robust inference is critical for user experience

### 2. Time Handling Challenges
- 24-hour time format requires careful handling of midnight wraparounds
- Time-based grouping is a core feature requiring efficient algorithms
- Hour gaps in data are important for testing time binning features

### 3. Performance Considerations
- The application needs to handle millions of rows efficiently
- Memory usage scales significantly with dataset size (3M rows ≈ 8.5 GB in memory)
- File I/O is a potential bottleneck for large datasets

### 4. Data Quality Features
- Duplicate detection is essential for data cleaning
- Missing value handling must be flexible (empty vs. marked as missing)
- Data validation should provide clear feedback

### 5. UI/UX Requirements
- Multi-file import capability suggests batch processing needs
- Query window implies need for filtering/searching capabilities
- Export dialog indicates multiple output format support

## Recommendations Going Forward

### 1. Performance Optimization
- **Priority**: Implement streaming/chunked processing for files > 1GB
- **Rationale**: Current memory usage (8.5 GB for 3M rows) won't scale to larger datasets
- **Test with**: The 3M row dataset for benchmarking improvements

### 2. Type Inference Enhancement
- **Priority**: Add confidence scores to type inference
- **Rationale**: Some columns may have ambiguous types
- **Test with**: Inference stress test columns with mixed empty/dash values

### 3. Time Handling Improvements
- **Priority**: Better support for multi-day datasets
- **Rationale**: Current 24-hour limitation may be restrictive
- **Test with**: Datasets with explicit date columns

### 4. Validation Framework
- **Priority**: Fix midnight wraparound validation
- **Rationale**: Current validator incorrectly flags valid time progressions
- **Implementation**: Update time comparison logic to handle 24-hour wraparounds

### 5. Memory Management
- **Priority**: Implement data paging/virtualization
- **Rationale**: Loading entire datasets into memory isn't sustainable
- **Benchmark**: Memory usage with different dataset sizes

### 6. Export Capabilities
- **Priority**: Support for chunked exports
- **Rationale**: Large datasets may exceed memory during export
- **Test with**: Exporting transformed versions of the 3M dataset

## Testing Strategy

### Unit Testing
- Use small subsets of the 10k dataset for quick iteration
- Focus on edge cases (midnight wraparound, empty values, etc.)

### Integration Testing
- Use the 300k dataset for full workflow testing
- Validate transformations maintain data integrity

### Performance Testing
- Use the 3M dataset for stress testing
- Monitor memory usage and processing time
- Identify bottlenecks in data processing pipeline

### Data Validation Testing
- Verify type inference accuracy across all column types
- Test missing value handling strategies
- Validate time-based grouping with hour gaps

## Project Status

### Completed
- ✅ Comprehensive test data generation system
- ✅ Three scaled datasets (10k, 300k, 3M rows)
- ✅ Validation framework (with known limitations)
- ✅ Statistical analysis of test data
- ✅ Documentation of data format and requirements

### Known Issues
- ⚠️ Validator incorrectly flags midnight time wraparounds
- ⚠️ Group duplication randomness doesn't always match target percentages
- ⚠️ Memory usage scales linearly with dataset size

### Next Steps
1. Test the Leaf application with generated datasets
2. Benchmark performance with each dataset size
3. Identify and fix any data processing issues
4. Optimize based on performance results
5. Extend test data generator for additional scenarios

## Conclusion

The Leaf project is a sophisticated data analysis tool with strong foundations in Rust's type system and performance characteristics. The test data generation system we've built provides comprehensive datasets for validating all aspects of the application, from basic CSV import to complex time-based transformations.

The key to success will be maintaining performance while handling increasingly large datasets, and ensuring the type inference system correctly identifies the diverse data types present in real-world CSV files. The test datasets provide an excellent foundation for continued development and optimization.

---

*Generated: August 2, 2024*
*Test Data Version: 1.0*
*Total Test Data: 3.31 million rows across 3 files*