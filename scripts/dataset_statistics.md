# Test Dataset Statistics Report

Generated: 2025-08-02 01:15:51

## Overview

| Dataset | Rows | Columns | File Size | Groups | Hour Gaps |
|---------|------|---------|-----------|--------|-----------|
| 10K | 10,000 | 46 | 4.1 MB | 32 | 2 (6.5%) |
| 300K | 300,000 | 46 | 121.9 MB | 855 | 69 (8.1%) |
| 3M | 3,000,000 | 46 | 1218.8 MB | 8,593 | 689 (8.0%) |

## 10K Dataset Details

### Basic Information
- **File**: `test_data_10k.csv`
- **Total Rows**: 10,000
- **Total Columns**: 46
- **File Size**: 4.06 MB
- **Memory Usage**: 28.61 MB
- **Total Groups**: 32

### Time Information
- **Time Range**: 00:00:00.000 to 06:28:33.256
- **Hour Gaps**: 2 (6.5% of gaps)

### Time Duplicate Distribution
| Duplicates | Count |
|------------|-------|
| 1 | 680 |
| 2 | 692 |
| 3 | 664 |
| 4 | 651 |
| 5 | 668 |

### Missing Values Summary
| Column | Empty | Dash | Total | Percentage |
|--------|-------|------|-------|------------|
| blob_infer_blank | 987 | 0 | 987 | 9.87% |
| blob_infer_dash | 0 | 987 | 987 | 9.87% |
| boolean_infer_blank | 987 | 0 | 987 | 9.87% |
| boolean_infer_dash | 0 | 987 | 987 | 9.87% |
| date_infer_blank | 987 | 0 | 987 | 9.87% |
| date_infer_dash | 0 | 987 | 987 | 9.87% |
| datetime_infer_blank | 987 | 0 | 987 | 9.87% |
| datetime_infer_dash | 0 | 987 | 987 | 9.87% |
| dumb_time | 32 | 0 | 32 | 0.32% |
| integer_infer_blank | 987 | 0 | 987 | 9.87% |
| integer_infer_dash | 0 | 987 | 987 | 9.87% |
| real_infer_blank | 987 | 0 | 987 | 9.87% |
| real_infer_dash | 0 | 987 | 987 | 9.87% |
| tags | 2,453 | 0 | 2,453 | 24.53% |
| text_infer_blank | 987 | 0 | 987 | 9.87% |
| text_infer_dash | 0 | 987 | 987 | 9.87% |
| timemicroseconds_infer_blank | 987 | 0 | 987 | 9.87% |
| timemicroseconds_infer_dash | 0 | 987 | 987 | 9.87% |
| timemilliseconds_infer_blank | 987 | 0 | 987 | 9.87% |
| timemilliseconds_infer_dash | 0 | 987 | 987 | 9.87% |
| timenanoseconds_infer_blank | 987 | 0 | 987 | 9.87% |
| timenanoseconds_infer_dash | 0 | 987 | 987 | 9.87% |
| timeseconds_infer_blank | 987 | 0 | 987 | 9.87% |
| timeseconds_infer_dash | 0 | 987 | 987 | 9.87% |

### Numerical Columns Statistics
| Column | Min | Max | Mean | Std Dev |
|--------|-----|-----|------|---------|
| width | 1.00 | 199.99 | 99.96 | 57.12 |
| height | 0.20 | 4.80 | 2.50 | 1.33 |
| angle | 0.02 | 359.89 | 179.18 | 103.43 |

### Boolean Columns Distribution
| Column | True | False | True % |
|--------|------|-------|--------|
| isGood | 4,948 | 5,052 | 49.5% |
| isOld | 4,907 | 5,093 | 49.1% |
| isWhat | 4,913 | 5,087 | 49.1% |
| isEnabled | 5,001 | 4,999 | 50.0% |
| isFlagged | 5,050 | 4,950 | 50.5% |

### Distribution Columns Statistics
| Column | Min | Max | Mean | Std Dev |
|--------|-----|-----|------|---------|
| bimodal | 14.01 | 85.32 | 50.61 | 20.63 |
| linear_over_time | 5.48 | 94.75 | 49.86 | 23.17 |
| exponential | 0.00 | 217.83 | 19.98 | 20.37 |
| uniform | 0.00 | 99.93 | 49.85 | 28.98 |
| normal | -9.56 | 109.91 | 50.04 | 15.01 |

## 300K Dataset Details

### Basic Information
- **File**: `test_data_300k.csv`
- **Total Rows**: 300,000
- **Total Columns**: 46
- **File Size**: 121.87 MB
- **Memory Usage**: 858.30 MB
- **Total Groups**: 855

### Time Information
- **Time Range**: 00:00:00.000 to 02:52:00.319
- **Hour Gaps**: 69 (8.1% of gaps)

### Time Duplicate Distribution
| Duplicates | Count |
|------------|-------|
| 1 | 20,287 |
| 2 | 20,344 |
| 3 | 20,301 |
| 4 | 19,874 |
| 5 | 19,675 |
| 6 | 11 |
| 7 | 6 |
| 8 | 11 |
| 9 | 5 |
| 10 | 1 |

### Missing Values Summary
| Column | Empty | Dash | Total | Percentage |
|--------|-------|------|-------|------------|
| blob_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| blob_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| boolean_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| boolean_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| date_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| date_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| datetime_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| datetime_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| dumb_time | 855 | 0 | 855 | 0.29% |
| integer_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| integer_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| real_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| real_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| tags | 75,119 | 0 | 75,119 | 25.04% |
| text_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| text_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| timemicroseconds_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| timemicroseconds_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| timemilliseconds_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| timemilliseconds_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| timenanoseconds_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| timenanoseconds_infer_dash | 0 | 29,634 | 29,634 | 9.88% |
| timeseconds_infer_blank | 29,634 | 0 | 29,634 | 9.88% |
| timeseconds_infer_dash | 0 | 29,634 | 29,634 | 9.88% |

### Numerical Columns Statistics
| Column | Min | Max | Mean | Std Dev |
|--------|-----|-----|------|---------|
| width | 1.00 | 200.00 | 100.68 | 57.48 |
| height | 0.20 | 4.80 | 2.50 | 1.33 |
| angle | 0.00 | 360.00 | 179.73 | 103.90 |

### Boolean Columns Distribution
| Column | True | False | True % |
|--------|------|-------|--------|
| isGood | 150,392 | 149,608 | 50.1% |
| isOld | 149,577 | 150,423 | 49.9% |
| isWhat | 150,223 | 149,777 | 50.1% |
| isEnabled | 149,800 | 150,200 | 49.9% |
| isFlagged | 150,185 | 149,815 | 50.1% |

### Distribution Columns Statistics
| Column | Min | Max | Mean | Std Dev |
|--------|-----|-----|------|---------|
| bimodal | 6.04 | 90.68 | 49.97 | 20.61 |
| linear_over_time | 3.90 | 97.45 | 49.89 | 23.18 |
| exponential | 0.00 | 232.31 | 19.94 | 19.89 |
| uniform | 0.00 | 100.00 | 50.03 | 28.88 |
| normal | -15.19 | 114.36 | 49.96 | 14.99 |

## 3M Dataset Details

### Basic Information
- **File**: `test_data_3m.csv`
- **Total Rows**: 3,000,000
- **Total Columns**: 46
- **File Size**: 1218.76 MB
- **Memory Usage**: 8583.03 MB
- **Total Groups**: 8,593

### Time Information
- **Time Range**: 00:00:00.000 to 23:11:38.264
- **Hour Gaps**: 689 (8.0% of gaps)

### Time Duplicate Distribution
| Duplicates | Count |
|------------|-------|
| 1 | 200,614 |
| 2 | 199,948 |
| 3 | 199,190 |
| 4 | 197,754 |
| 5 | 197,181 |
| 6 | 1,117 |
| 7 | 922 |
| 8 | 676 |
| 9 | 437 |
| 10 | 240 |
| 11 | 6 |
| 12 | 3 |

### Missing Values Summary
| Column | Empty | Dash | Total | Percentage |
|--------|-------|------|-------|------------|
| blob_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| blob_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| boolean_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| boolean_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| date_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| date_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| datetime_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| datetime_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| dumb_time | 8,593 | 0 | 8,593 | 0.29% |
| integer_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| integer_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| real_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| real_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| tags | 750,463 | 0 | 750,463 | 25.02% |
| text_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| text_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| timemicroseconds_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| timemicroseconds_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| timemilliseconds_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| timemilliseconds_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| timenanoseconds_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| timenanoseconds_infer_dash | 0 | 296,102 | 296,102 | 9.87% |
| timeseconds_infer_blank | 296,102 | 0 | 296,102 | 9.87% |
| timeseconds_infer_dash | 0 | 296,102 | 296,102 | 9.87% |

### Numerical Columns Statistics
| Column | Min | Max | Mean | Std Dev |
|--------|-----|-----|------|---------|
| width | 1.00 | 200.00 | 100.48 | 57.45 |
| height | 0.20 | 4.80 | 2.50 | 1.33 |
| angle | 0.00 | 360.00 | 179.97 | 103.86 |

### Boolean Columns Distribution
| Column | True | False | True % |
|--------|------|-------|--------|
| isGood | 1,500,347 | 1,499,653 | 50.0% |
| isOld | 1,500,458 | 1,499,542 | 50.0% |
| isWhat | 1,500,190 | 1,499,810 | 50.0% |
| isEnabled | 1,499,111 | 1,500,889 | 50.0% |
| isFlagged | 1,499,337 | 1,500,663 | 50.0% |

### Distribution Columns Statistics
| Column | Min | Max | Mean | Std Dev |
|--------|-----|-----|------|---------|
| bimodal | 4.81 | 96.57 | 50.00 | 20.62 |
| linear_over_time | 1.90 | 97.73 | 49.89 | 23.18 |
| exponential | 0.00 | 313.16 | 20.02 | 20.02 |
| uniform | 0.00 | 100.00 | 49.97 | 28.87 |
| normal | -21.68 | 121.04 | 50.00 | 15.00 |