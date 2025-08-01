# Scripts

This folder contains utility scripts for the Leaf application.

## Test Data Generator

The `generate_test_data.py` script creates comprehensive test datasets with various distributions, time columns, and data types for testing the Leaf application.

### Features

- **Multiple Distributions**: Normal, exponential, uniform, log-normal, Poisson, gamma, beta, and Weibull distributions
- **Time Data**: Sequential timestamps, non-sequential timestamps with gaps, Unix timestamps, and time-only data
- **Categorical Data**: Categories with uneven distributions, binary flags, status codes, and regions
- **Sequential Data**: Linear trends, cyclical patterns, and step functions
- **Outlier Data**: Data with intentional outliers and missing values
- **Multiple Datasets**: 6 different CSV files for various testing scenarios

### Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Test the script with a small dataset
python generate_test_data.py --test

# Generate 1000 rows of test data
python generate_test_data.py --rows 1000

# Generate 5000 rows and save to custom directory
python generate_test_data.py --rows 5000 --output-dir my_test_data
```

### Generated Datasets

1. **comprehensive_test_data.csv** - Full dataset with all features
2. **time_series_data.csv** - Focused on time series analysis
3. **data_with_gaps.csv** - Time data with intentional gaps for grouping tests
4. **simple_test_data.csv** - Basic dataset for simple testing
5. **data_with_nulls.csv** - Data with missing values and null patterns
6. **high_frequency_data.csv** - High-frequency time data (1-minute intervals)

### Data Types Included

- **Time Columns**: Sequential, non-sequential, Unix timestamps, time-only
- **Numeric Distributions**: Normal, exponential, uniform, log-normal, Poisson, gamma, beta, Weibull
- **Categorical**: Categories, binary flags, status codes, regions
- **Sequential Patterns**: Linear trends, cyclical data, step functions
- **Special Cases**: Outliers, missing values, null patterns

### Testing the Leaf Application

These datasets are perfect for testing:
- CSV import functionality
- Time-based grouping features
- Delta transformations
- Data visualization
- Query functionality
- Error handling with various data types 