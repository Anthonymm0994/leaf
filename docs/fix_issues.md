# Quick Fix Instructions

## To fix BOTH issues:

### 1. Reimport your CSV data to fix the periods

The periods are in the stored data. You need to reimport the CSV file:

```bash
# Option A: Delete the old arrow file and reimport through the UI
rm test_data/test_data_300k.arrow
cargo run --bin leaf
# Then use the UI to import data_gen_scripts/test_data_300k.csv again

# Option B: Use the test import script
cargo run --bin test_csv_import
# This will create test_import_result.arrow with the fixed data
```

### 2. For the pagination issue

The pagination code is correct. The issue might be:

1. **Check if total_rows is being set**: The "Page 1 of 12000" should show if it's working
2. **Check the button state**: The Next button should be enabled if you're not on the last page
3. **Try clicking where the button should be**: Sometimes UI rendering issues make buttons invisible but clickable

### 3. Alternative: Debug in the UI

Add this debug line to see what's happening:

In `src/ui/query_window.rs` around line 170, temporarily add:
```rust
ui.label(format!("DEBUG: page={}, total_pages={}, next_enabled={}", self.page, total_pages, next_enabled));
```

This will show you the actual values being used.

## The Real Issue

Looking at your screenshot, you're viewing `test_data_300k` which was imported BEFORE the fixes. You need to:

1. Close the application
2. Delete or rename the old arrow file
3. Restart and reimport the CSV
4. The periods should be gone and pagination should work