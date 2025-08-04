# Test Data 300k

The `test_data_300k.csv` file is compressed as `test_data_300k.tar.gz` due to GitHub's file size limitations.

## Extracting the file

### Option 1: Direct CSV file (via Git LFS)
The uncompressed `test_data_300k.csv` is now available directly via Git LFS. 
Just pull the repository and the file will be downloaded automatically.

### Option 2: From compressed file

**On Windows (PowerShell or Git Bash):**
```bash
cd data_gen_scripts
tar -xzf test_data_300k.tar.gz
```

**Alternative for Windows:**
1. Right-click on `test_data_300k.tar.gz`
2. Use 7-Zip, WinRAR, or Windows built-in extraction
3. Extract to current directory

## File Description

This 300k row CSV file contains comprehensive test data with:
- Time columns: good_time, dumb_time (HH:MM:SS.sss format)
- Numeric columns: width, height, angle
- Category columns: category_3 through category_10
- Boolean columns: isGood, isOld, isWhat, isEnabled, isFlagged
- Columns with null handling: *_infer_blank and *_infer_dash variants
- Statistical distribution columns: bimodal, linear_over_time, exponential, uniform, normal

The file is used for testing all transformation features in Leaf.
