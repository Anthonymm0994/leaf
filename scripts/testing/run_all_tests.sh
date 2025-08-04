#!/bin/bash
echo "=== Running All Transformation Tests ==="
echo ""

# Build all test binaries
echo "Building test binaries..."
cargo build --release --bin test_time_parsing 2>&1 | grep -E "(Finished|error)" || echo "✓ test_time_parsing built"
cargo build --release --bin test_all_data_types 2>&1 | grep -E "(Finished|error)" || echo "✓ test_all_data_types built"

echo ""
echo "--- Running Time Parsing Test ---"
./target/release/test_time_parsing.exe 2>&1 | head -30

echo ""
echo "--- Running Data Types Test ---"
./target/release/test_all_data_types.exe 2>&1

echo ""
echo "--- Test CSV Files Created ---"
ls -la *.csv 2>/dev/null | grep -E "(time_test|mixed_types|numeric_test|time_series)"

echo ""
echo "✅ All tests completed!"
echo ""
echo "To test in the Leaf GUI:"
echo "1. Run: ./target/release/leaf.exe"
echo "2. Load the test CSV files created above"
echo "3. Try the transformations as described in the test output"