# Duplicate Group Detection and Removal Scripts

## Overview

This document describes the scripts available for detecting and removing duplicate major groups from CSV files in the Leaf project format.

## Main Script: `remove_duplicate_groups.py`

**Location**: `scripts/remove_duplicate_groups.py`

This is the primary script for removing duplicate major groups from CSV files.

### Features

- Identifies major groups (sequences that start with a NULL dumb_time)
- Detects duplicates based on non-time column values
- Removes duplicate groups while preserving the first occurrence
- Provides detailed analysis of duplication patterns

### Usage

```bash
# Basic usage - removes duplicates and saves to new file
python scripts/remove_duplicate_groups.py input.csv

# Specify output file
python scripts/remove_duplicate_groups.py input.csv -o output_cleaned.csv

# Analyze only (no removal)
python scripts/remove_duplicate_groups.py input.csv --analyze-only

# Verbose mode for detailed information
python scripts/remove_duplicate_groups.py input.csv -v
```

### Example

```bash
# Remove duplicates from the test data
python scripts/remove_duplicate_groups.py test_data_300k_correct.csv

# Output:
# Loading test_data_300k_correct.csv...
# Loaded 300,000 rows
# 
# Identifying major groups...
# Found 855 major groups
# 
# Finding duplicate groups...
# 
# === Duplicate Analysis ===
# Total major groups: 855
# Unique major groups: 704
# Duplicate major groups: 151
# Duplication rate: 17.7%
# 
# Saving cleaned data to test_data_300k_correct_cleaned.csv...
# 
# === Summary ===
# Original rows: 300,000
# Cleaned rows: 247,000
# Rows removed: 53,000 (17.7%)
```

## Analysis Scripts

### 1. `analyze_correct_duplicates.py`
**Location**: `scripts/analyze_correct_duplicates.py`

Analyzes the duplication patterns in the dataset without removing them. Useful for understanding:
- Mini group structure (same good_time, different data)
- Major group patterns
- Duplication statistics

### 2. `test_dataset_simple.py`
**Location**: `scripts/test_dataset_simple.py`

Quick validation script that checks:
- Time ordering (dumb_time > good_time)
- Mini group uniqueness
- Major group identification
- Basic duplication stats

### 3. `validate_dataset_comprehensive.py`
**Location**: `scripts/validate_dataset_comprehensive.py`

Comprehensive validation including:
- Time format validation
- Data integrity checks
- Detailed duplication analysis
- Missing value patterns

## Test Scripts

Several test scripts demonstrate duplicate detection using Leaf's built-in functionality:

- `test_duplicate_detection_300k_correct.py` - Tests Leaf's duplicate detector
- `test_major_group_duplicates.py` - Tests major group duplicate detection
- `debug_duplicate_detection.py` - Debug tool for duplicate detection

## Data Generation

The test data with controlled duplicates can be generated using:

```bash
python scripts/generate_test_data_correct.py -r 300000 -o test_data.csv
```

This creates data with:
- 85% unique major groups
- 10% duplicated once (2x total)
- 5% duplicated twice (3x total)