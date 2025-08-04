# Leaf Comprehensive Test Checklist - test_data_300k

## Dataset Column Summary

### Time Columns
- `good_time` - HH:MM:SS.sss format (non-nullable)
- `dumb_time` - HH:MM:SS.sss format (nullable - first row of each group is empty)
- `timeseconds_infer_blank` - HH:MM:SS format (nullable with blanks)
- `timeseconds_infer_dash` - HH:MM:SS format (nullable with dashes)
- `timemilliseconds_infer_blank` - HH:MM:SS.sss format (nullable with blanks)
- `timemilliseconds_infer_dash` - HH:MM:SS.sss format (nullable with dashes)
- `timemicroseconds_infer_blank` - HH:MM:SS.ssssss format (nullable with blanks)
- `timemicroseconds_infer_dash` - HH:MM:SS.ssssss format (nullable with dashes)
- `timenanoseconds_infer_blank` - HH:MM:SS.sssssssss format (nullable with blanks)
- `timenanoseconds_infer_dash` - HH:MM:SS.sssssssss format (nullable with dashes)

### Numeric Columns
- `width` - Float/Real (non-nullable)
- `height` - Float/Real (non-nullable)
- `angle` - Float/Real (non-nullable)
- `integer_infer_blank` - Integer (nullable with blanks)
- `integer_infer_dash` - Integer (nullable with dashes)
- `real_infer_blank` - Float/Real (nullable with blanks)
- `real_infer_dash` - Float/Real (nullable with dashes)
- `bimodal` - Float (non-nullable, bimodal distribution)
- `linear_over_time` - Float (non-nullable, linear distribution)
- `exponential` - Float (non-nullable, exponential distribution)
- `uniform` - Float (non-nullable, uniform distribution)
- `normal` - Float (non-nullable, normal distribution)

### Text/Categorical Columns
- `category_3` through `category_10` - Text (non-nullable, varying cardinality)
- `text_infer_blank` - Text (nullable with blanks)
- `text_infer_dash` - Text (nullable with dashes)
- `tags` - Text (multi-value, comma-separated)
- `blob_infer_blank` - Text/Blob (nullable with blanks)
- `blob_infer_dash` - Text/Blob (nullable with dashes)

### Boolean Columns
- `isGood` - Boolean (non-nullable)
- `isOld` - Boolean (non-nullable)
- `isWhat` - Boolean (non-nullable)
- `isEnabled` - Boolean (non-nullable)
- `isFlagged` - Boolean (non-nullable)
- `boolean_infer_blank` - Boolean (nullable with blanks)
- `boolean_infer_dash` - Boolean (nullable with dashes)

### Date/DateTime Columns
- `date_infer_blank` - Date YYYY-MM-DD (nullable with blanks)
- `date_infer_dash` - Date YYYY-MM-DD (nullable with dashes)
- `datetime_infer_blank` - DateTime YYYY-MM-DD HH:MM:SS (nullable with blanks)
- `datetime_infer_dash` - DateTime YYYY-MM-DD HH:MM:SS (nullable with dashes)

---

## 1. Add Time Bin Column - Testing Checklist

### Fixed Interval Strategy
#### Time Columns
- [ ] `good_time` (HH:MM:SS.sss) - 30s interval
- [ ] `good_time` (HH:MM:SS.sss) - 1m interval
- [ ] `good_time` (HH:MM:SS.sss) - 5m interval
- [ ] `good_time` (HH:MM:SS.sss) - 1h interval
- [ ] `dumb_time` (HH:MM:SS.sss with nulls) - 1m interval
- [ ] `timeseconds_infer_blank` (HH:MM:SS with nulls) - 30s interval
- [ ] `timeseconds_infer_dash` (HH:MM:SS with dashes) - 30s interval
- [ ] `timemilliseconds_infer_blank` (HH:MM:SS.sss with nulls) - 1m interval
- [ ] `timemilliseconds_infer_dash` (HH:MM:SS.sss with dashes) - 1m interval
- [ ] `timemicroseconds_infer_blank` (HH:MM:SS.ssssss with nulls) - 30s interval
- [ ] `timemicroseconds_infer_dash` (HH:MM:SS.ssssss with dashes) - 30s interval
- [ ] `timenanoseconds_infer_blank` (HH:MM:SS.sssssssss with nulls) - 30s interval
- [ ] `timenanoseconds_infer_dash` (HH:MM:SS.sssssssss with dashes) - 30s interval

### Manual Intervals Strategy
- [ ] `good_time` - intervals at 00:00:00, 01:00:00, 02:00:00
- [ ] `dumb_time` - intervals at 00:05:00, 00:10:00, 00:15:00
- [ ] `timemilliseconds_infer_blank` - custom intervals

### Threshold-Based Strategy
- [ ] `good_time` - 30m threshold (should detect hour gaps)
- [ ] `good_time` - 5m threshold
- [ ] `dumb_time` - 10m threshold
- [ ] `timeseconds_infer_blank` - 1h threshold

---

## 2. Add Computed Columns - Testing Checklist

### Delta Transformation
#### Numeric Columns (Non-nullable)
- [ ] `width` (Float)
- [ ] `height` (Float)
- [ ] `angle` (Float)
- [ ] `bimodal` (Float)
- [ ] `linear_over_time` (Float)
- [ ] `exponential` (Float)
- [ ] `uniform` (Float)
- [ ] `normal` (Float)

#### Numeric Columns (Nullable)
- [ ] `integer_infer_blank` (Integer with nulls)
- [ ] `integer_infer_dash` (Integer with dashes)
- [ ] `real_infer_blank` (Float with nulls)
- [ ] `real_infer_dash` (Float with dashes)

### Cumulative Sum
#### Non-nullable
- [ ] `width` (Float)
- [ ] `height` (Float)
- [ ] `linear_over_time` (Float)

#### Nullable
- [ ] `integer_infer_blank` (with SkipNulls)
- [ ] `integer_infer_blank` (with FillWithZero)
- [ ] `real_infer_blank` (with SkipNulls)

### Percentage of Total
- [ ] `width` (Float)
- [ ] `exponential` (Float)
- [ ] `integer_infer_blank` (with nulls)

### Ratio
- [ ] `width` / `height`
- [ ] `angle` / `width`
- [ ] `integer_infer_blank` / `real_infer_blank` (both nullable)

### Moving Average
- [ ] `width` (window=3)
- [ ] `width` (window=5)
- [ ] `linear_over_time` (window=10)
- [ ] `real_infer_blank` (window=5, with nulls)

### Z-Score
- [ ] `width`
- [ ] `normal` (should be ~N(0,1) after transformation)
- [ ] `bimodal`
- [ ] `integer_infer_blank` (with nulls)

---

## 3. Add Group ID Columns - Testing Checklist

### ValueChange Rule
#### Text Columns
- [ ] `category_3` (3 unique values)
- [ ] `category_10` (10 unique values)
- [ ] `text_infer_blank` (with nulls)
- [ ] `text_infer_dash` (with dashes)
- [ ] `tags` (multi-value)

#### Numeric Columns
- [ ] `width` (Float)
- [ ] `height` (Float)
- [ ] `integer_infer_blank` (with nulls)
- [ ] `real_infer_dash` (with dashes)

#### Boolean Columns
- [ ] `isGood`
- [ ] `isEnabled`
- [ ] `boolean_infer_blank` (with nulls)

#### Date/Time Columns
- [ ] `good_time`
- [ ] `dumb_time` (with empty values)
- [ ] `date_infer_blank` (with nulls)
- [ ] `datetime_infer_dash` (with dashes)

### IsEmpty Rule
- [ ] `dumb_time` (first row of each group is empty)
- [ ] `text_infer_blank`
- [ ] `integer_infer_blank`
- [ ] `real_infer_blank`
- [ ] `boolean_infer_blank`
- [ ] `date_infer_blank`
- [ ] `datetime_infer_blank`
- [ ] `timeseconds_infer_blank`
- [ ] `blob_infer_blank`

### ValueEquals Rule
#### Text Values
- [ ] `category_3` = 'a'
- [ ] `category_3` = 'b'
- [ ] `tags` = '' (empty)
- [ ] `tags` = 'a,b,c'

#### Boolean Values
- [ ] `isGood` = true
- [ ] `isGood` = false
- [ ] `boolean_infer_dash` = true (handling dashes)

#### Numeric Values
- [ ] `height` = 3.5
- [ ] `integer_infer_dash` = 100 (if exists)

---

## Test Execution Plan

### Setup
1. Build Leaf: `cargo build --release`
2. Run Leaf: `./target/release/leaf.exe`
3. Load data: File → Import → `data_gen_scripts/test_data_300k.csv`
4. Set output directory: Create `test_output` folder

### Phase 1: Time Binning - CRITICAL TESTS
**Goal: Verify all time formats parse correctly**

#### Test 1.1: Basic Time Parsing
- [ ] Load table with 300k rows shows in sidebar
- [ ] Open "Add Time Bin Column"
- [ ] Time columns auto-detected (only timestamp columns shown)
- [ ] Select `good_time` → Preview shows "300000 total rows → X bins"
- [ ] NO "Invalid: 300000 rows" error

#### Test 1.2: Fixed Interval - Core Time Columns
- [ ] `good_time` + Fixed 30s → Preview shows ~20-30 bins → Apply
- [ ] `good_time` + Fixed 1m → Preview shows ~10-15 bins → Apply
- [ ] `good_time` + Fixed 1h → Preview shows ~4-5 bins → Apply
- [ ] `dumb_time` + Fixed 1m → Handles empty values → Apply

#### Test 1.3: Nullable Time Columns
- [ ] `timeseconds_infer_blank` + Fixed 30s → Handles blanks → Apply
- [ ] `timemilliseconds_infer_dash` + Fixed 1m → Handles dashes → Apply

### Phase 2: Computed Columns - CRITICAL TESTS
**Goal: Verify numeric computations with nulls**

#### Test 2.1: Delta on Core Columns
- [ ] Open "Add Computed Columns"
- [ ] `width` + Delta → First row null → Apply
- [ ] `integer_infer_blank` + Delta + SkipNulls → Apply
- [ ] `real_infer_dash` + Delta + PropagateNulls → Apply

#### Test 2.2: Other Computations
- [ ] `width` + Cumulative Sum → Apply
- [ ] `width` / `height` + Ratio → Apply
- [ ] `normal` + Z-Score → Apply

### Phase 3: Group ID - CRITICAL TESTS
**Goal: Verify all data types handled**

#### Test 3.1: ValueChange on Different Types
- [ ] Open "Add Group ID Columns"
- [ ] `category_3` + ValueChange → ~3 groups → Apply
- [ ] `good_time` + ValueChange → Many groups → Apply
- [ ] `isGood` + ValueChange → Groups on true/false → Apply

#### Test 3.2: IsEmpty Rule
- [ ] `dumb_time` + IsEmpty → Groups empty values → Apply
- [ ] `text_infer_blank` + IsEmpty → Apply

### Validation After Each Test
1. **Preview Works**: Shows meaningful statistics, not errors
2. **Apply Works**: No error dialog, success message appears
3. **Output Created**: New table appears in sidebar
4. **Data Correct**: Query window shows expected results

### Performance Criteria
- [ ] Preview generates in < 2 seconds
- [ ] Apply completes in < 5 seconds for 300k rows
- [ ] No memory errors or crashes

---

## Notes
- The test_data_300k has ~300,000 rows with complex patterns
- Time columns have hour-long gaps after ~10% of groups
- All inference columns have ~10% missing values (blanks or dashes)
- Boolean columns are stored as true/false strings
- Multi-value column (tags) uses comma separation