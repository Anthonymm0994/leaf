# Test Data Generation Scripts

This folder contains scripts for generating synthetic CSV test data for the Leaf application.

## Generated Datasets

All datasets are constrained to a single 24-hour period (00:00:00.000 to 23:59:59.999):

- **test_data_10k.csv**: 10,000 rows (4.1 MB)
- **test_data_300k.csv**: 300,000 rows (122 MB)
- **test_data_3m.csv**: 3,000,000 rows (1.2 GB)

## Scripts

### generate_test_data.py
Generates synthetic test data with the following characteristics:
- Time columns with strictly increasing timestamps
- Numerical columns with specific ranges and precision
- Categorical columns with varying cardinality
- Boolean columns
- Inference stress test columns with missing values
- Distribution columns (bimodal, linear, exponential, uniform, normal)

Usage:
```bash
python generate_test_data.py --rows 10000 --output test_data_10k.csv
```

### validate_test_data.py
Validates that generated datasets meet all requirements:
- Correct column structure and data types
- Time constraints and relationships
- Value ranges and distributions
- Missing value patterns

Usage:
```bash
python validate_test_data.py --file test_data_10k.csv
```

### analyze_datasets.py
Analyzes datasets to verify they stay within 24-hour constraints and provides statistics.

Usage:
```bash
python analyze_datasets.py
```

## Requirements

Install dependencies:
```bash
pip install -r requirements.txt
```

## Archive

The `archive/` folder contains previous versions of scripts and documentation.