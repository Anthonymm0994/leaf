# Scripts

This folder contains utility scripts for the Leaf application, including test data generation and validation tools.

## Test Data Generator (`generate_test_data.py`)

This script generates a synthetic CSV file (`test_data.csv`) with specific requirements for testing data inference and processing capabilities.

### Features

- **Time Columns**: 
  - `good_time`: HH:MM:SS.sss format, strictly increasing, duplicated 1-5 times
  - `dumb_time`: 1-5 minutes after good_time, empty for first row of each group
- **Numerical Columns**: width (1-200), height (0.2-4.8), angle (0-360)
- **Categorical Columns**: category_3 through category_10 with corresponding unique values
- **Boolean Columns**: isGood, isOld, isWhat, isEnabled, isFlagged
- **Inference Stress Test Columns**: 22 columns testing various data types with empty and "-" values
- **Multi-value Column**: tags with comma-separated values
- **Distribution Columns**: bimodal, linear_over_time, exponential, uniform, normal

### Group Behavior

- Data generated in groups of 200-500 rows
- 80% unique groups, 15% duplicated once, 5% duplicated twice
- Duplicated groups have identical data except time columns
- ~10% of groups have 1-hour gaps after them (for testing time-based grouping)

### Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Generate default 10,000 rows
python generate_test_data.py

# Generate custom number of rows
python generate_test_data.py --rows 5000
```

## Test Data Validator (`validate_test_data.py`)

This script validates the generated test_data.csv file to ensure it meets all requirements.

### Validation Checks

- Time format and ordering validation
- Group structure and duplication patterns
- Time gaps between groups (including hour-long gaps)
- Value ranges and data types
- Inference column patterns
- Distribution characteristics

### Usage

```bash
# Validate default test_data.csv
python validate_test_data.py

# Validate specific file
python validate_test_data.py --file my_data.csv
```

## Quick Test

To generate and validate test data:

```bash
# Generate test data
python generate_test_data.py --rows 1000

# Validate the generated data
python validate_test_data.py
```

## Requirements

See `requirements.txt` for dependencies:
- pandas>=1.5.0
- numpy>=1.21.0

## Generated Test Data Files

The following test data files are available:

| File | Rows | Size | Description |
|------|------|------|-------------|
| `test_data_10k.csv` | 10,000 | 4.1 MB | Small dataset for quick testing |
| `test_data_300k.csv` | 300,000 | 122 MB | Medium dataset for performance testing |
| `test_data_3m.csv` | 3,000,000 | 1.2 GB | Large dataset for stress testing |

See `dataset_statistics.md` for comprehensive statistics on each dataset including:
- Missing value patterns
- Distribution analysis
- Time gap analysis
- Column-by-column breakdowns 