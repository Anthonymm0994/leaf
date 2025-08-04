# Test Dataset Summary

Generated: 2025-08-03

## Overview

Three synthetic CSV datasets for testing the Leaf application, all constrained to a single 24-hour period.

## Dataset Specifications

### test_data_10k.csv
- **Rows**: 10,000
- **Size**: 4.1 MB
- **Time Span**: 00:00:00.000 to 23:53:54.372 (23.9 hours)
- **Groups**: 32 (avg 312.5 rows/group)
- **Hour Gaps**: 2 (full 1-hour gaps)

### test_data_300k.csv
- **Rows**: 300,000
- **Size**: 121.9 MB
- **Time Span**: 00:00:00.000 to 23:55:49.959 (23.9 hours)
- **Groups**: 867 (avg 346.0 rows/group)
- **Hour Gaps**: 21 (10-minute gaps)

### test_data_3m.csv
- **Rows**: 3,000,000
- **Size**: 1.2 GB
- **Time Span**: 00:00:00.000 to 23:59:06.584 (24.0 hours)
- **Groups**: 8,594 (avg 349.1 rows/group)
- **Hour Gaps**: 687 (2-minute gaps)

## Column Structure (46 columns)

### Time Columns (2)
- `good_time`: HH:MM:SS.sss format, strictly increasing
- `dumb_time`: 1-5 minutes after good_time (empty for first row of each group)

### Numerical Columns (3)
- `width`: 1.00-200.00 (2 decimal places)
- `height`: 0.2-4.8 (1 decimal place)
- `angle`: 0.00-360.00 (2 decimal places)

### Categorical Columns (8)
- `category_3` through `category_10`: Values from 'a' to nth letter

### Boolean Columns (5)
- `isGood`, `isOld`, `isWhat`, `isEnabled`, `isFlagged`

### Inference Test Columns (22)
- 11 data types × 2 missing patterns (blank/dash)
- ~10% missing values in each column

### Other Columns (6)
- `tags`: Multi-value column with "", "a", "a,b", "a,b,c"
- `bimodal`: Bimodal distribution around 30 and 70
- `linear_over_time`: Linear increase over dataset
- `exponential`: Exponential distribution
- `uniform`: Uniform distribution 0-100
- `normal`: Normal distribution (mean=50, std=15)

## Key Features

✅ **24-Hour Constraint**: All timestamps stay within a single day
✅ **Time Duplicates**: 1-5 duplicates per unique timestamp
✅ **Group Structure**: 200-500 rows per group
✅ **Missing Values**: Strategic placement for inference testing
✅ **Distribution Variety**: Multiple statistical distributions for testing

## Usage

These datasets are designed to test:
- Data type inference
- Time series processing
- Missing value handling
- Large-scale data performance
- Statistical analysis capabilities

## Scripts

- `generate_test_data.py`: Generate new datasets
- `validate_test_data.py`: Validate dataset requirements
- `analyze_datasets.py`: Analyze dataset characteristics