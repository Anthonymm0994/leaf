# Test Data 300k

The `test_data_300k.csv` file is compressed as `test_data_300k.tar.gz` due to GitHub's file size limitations.

## Extracting the file

To extract the CSV file:
```bash
cd data_gen_scripts
tar -xzf test_data_300k.tar.gz
```

## File Description

This 300k row CSV file contains comprehensive test data with:
- Time columns: good_time, dumb_time (HH:MM:SS.sss format)
- Numeric columns: width, height, angle
- Category columns: category_3 through category_10
- Boolean columns: isGood, isOld, isWhat, isEnabled, isFlagged
- Columns with null handling: *_infer_blank and *_infer_dash variants
- Statistical distribution columns: bimodal, linear_over_time, exponential, uniform, normal

The file is used for testing all transformation features in Leaf.
