Run through and give a general concensus of the project. I'm curious what areas need more attention or could be optimized/cleaned up. Put your observations in a markdown file at the root level of the project. I want to walk through this project starting from the entry point of the application.

Remove the transformed_data folder. Any data generated from the tools should go into the folder that was loaded with the data in it.

Remove the plotting functionality for now. I just want a tool for ingesting arrow files.

Why do we have Add Time Bin Column and Time-Based Grouping? Remove the Time-Based Grouping. It's the same as Add Time Bin Column.

Change Fresh to leaf, make the theme match the name.

Shrinking the size of query windows crashes the application. Console message when this occurs below:
thread 'main' panicked at C:\Users\antho\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\egui-0.29.1\src\layout.rs:601:9:
assertion failed: child_size.x >= 0.0 && child_size.y >= 0.0
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
error: process didn't exit successfully: `target\debug\leaf.exe` (exit code: 101)

The number of total rows doesn't seem accurate for the data tables.