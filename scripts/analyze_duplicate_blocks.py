#!/usr/bin/env python3
"""
Analyze the duplicate blocks in the generated test data.
"""

import pandas as pd
import sys
from collections import Counter

def analyze_duplicate_blocks(filename):
    """Analyze duplicate blocks in the dataset."""
    print(f"Analyzing {filename}...")
    
    # Load the data
    df = pd.read_csv(filename)
    print(f"Total rows: {len(df):,}")
    
    # Non-time columns
    non_time_cols = [col for col in df.columns if 'time' not in col.lower()]
    print(f"Non-time columns: {len(non_time_cols)}")
    
    # Find blocks of consecutive identical rows
    print("\nFinding consecutive blocks...")
    blocks = []
    i = 0
    while i < len(df):
        # Find end of current block
        j = i + 1
        while j < len(df) and all(df.iloc[j][col] == df.iloc[i][col] for col in non_time_cols):
            j += 1
        
        if j - i > 1:  # Found a block
            blocks.append({
                'start': i,
                'end': j-1,
                'size': j-i,
                'signature': tuple(df.iloc[i][non_time_cols].values)
            })
        i = j
    
    print(f"Found {len(blocks)} blocks of consecutive identical rows")
    
    # Analyze block sizes
    block_sizes = [b['size'] for b in blocks]
    if block_sizes:
        print(f"\nBlock size statistics:")
        print(f"  Min size: {min(block_sizes)}")
        print(f"  Max size: {max(block_sizes)}")
        print(f"  Avg size: {sum(block_sizes)/len(block_sizes):.1f}")
        print(f"  Total rows in blocks: {sum(block_sizes):,}")
    
    # Find duplicate blocks (same signature appearing multiple times)
    print("\nFinding duplicate blocks...")
    signature_counts = Counter(b['signature'] for b in blocks)
    duplicate_signatures = {sig: count for sig, count in signature_counts.items() if count > 1}
    
    print(f"\nFound {len(duplicate_signatures)} unique block patterns that appear multiple times")
    
    # Calculate statistics
    total_duplicate_blocks = sum(count for count in duplicate_signatures.values())
    duplicate_once = sum(1 for count in duplicate_signatures.values() if count == 2)
    duplicate_twice = sum(1 for count in duplicate_signatures.values() if count == 3)
    
    print(f"\nDuplication statistics:")
    print(f"  Blocks duplicated once (appear 2 times): {duplicate_once}")
    print(f"  Blocks triplicated (appear 3 times): {duplicate_twice}")
    print(f"  Total duplicate block instances: {total_duplicate_blocks}")
    
    # Calculate percentages
    if blocks:
        pct_duplicated = (duplicate_once * 2) / len(blocks) * 100
        pct_triplicated = (duplicate_twice * 3) / len(blocks) * 100
        print(f"\nPercentages (of all blocks):")
        print(f"  Duplicated blocks: {pct_duplicated:.1f}%")
        print(f"  Triplicated blocks: {pct_triplicated:.1f}%")
    
    # Show examples
    if duplicate_signatures:
        print("\nExample duplicate blocks:")
        for i, (sig, count) in enumerate(list(duplicate_signatures.items())[:5]):
            print(f"\n{i+1}. Block appears {count} times:")
            matching_blocks = [b for b in blocks if b['signature'] == sig]
            for b in matching_blocks[:3]:  # Show up to 3 occurrences
                print(f"   Rows {b['start']}-{b['end']} (size: {b['size']})")
            # Show sample values
            first_block = matching_blocks[0]
            row_data = df.iloc[first_block['start']]
            print(f"   Sample values: width={row_data['width']}, category_3={row_data['category_3']}, isGood={row_data['isGood']}")
    
    return blocks, duplicate_signatures

if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else "test_data_300k_blocks.csv"
    analyze_duplicate_blocks(filename)