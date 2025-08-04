use leaf::ui::time_bin_dialog::TimeBinDialog;

fn main() {
    println!("=== Testing Time Parsing ===\n");
    
    // Create a dialog instance to access the parse_timestamp method
    let dialog = TimeBinDialog::default();
    
    // Test various time formats
    let test_times = vec![
        // HH:MM:SS.sss format (what test_data_300k likely uses)
        "00:00:00.000",
        "00:00:30.500",
        "01:30:45.123",
        "12:34:56.789",
        "23:59:59.999",
        
        // HH:MM:SS format
        "00:00:00",
        "12:00:00",
        "23:59:59",
        
        // HH:MM format
        "00:00",
        "12:30",
        "23:59",
        
        // Unix timestamps
        "1234567890",
        
        // ISO format
        "2024-01-01T12:00:00",
        "2024-01-01T12:00:00.123",
        
        // Invalid formats
        "not a time",
        "25:00:00", // Invalid hour
        "",
    ];
    
    println!("Testing time parsing:");
    println!("{:<25} | {:<15} | Notes", "Input", "Result");
    println!("{}", "-".repeat(60));
    
    for time_str in test_times {
        // We can't call parse_timestamp directly as it's private
        // So let's test the parsing logic inline
        let result = parse_time(time_str);
        match result {
            Ok(seconds) => {
                println!("{:<25} | {:<15} | OK", time_str, seconds);
            }
            Err(e) => {
                println!("{:<25} | ERROR          | {}", time_str, e);
            }
        }
    }
    
    // Test binning logic with HH:MM:SS.sss format
    println!("\n\nTesting binning with HH:MM:SS.sss format:");
    test_binning_logic();
}

fn parse_time(time_str: &str) -> Result<u64, String> {
    // Try to parse as seconds since epoch
    if let Ok(timestamp) = time_str.parse::<u64>() {
        return Ok(timestamp);
    }
    
    // Try ISO format
    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(time_str) {
        return Ok(datetime.timestamp() as u64);
    }
    
    // Try other formats
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%H:%M:%S%.f",
        "%H:%M:%S",
        "%H:%M",
    ];
    
    for format in &formats {
        if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(time_str, format) {
            return Ok(datetime.and_utc().timestamp() as u64);
        }
    }
    
    // Try time-only formats (for HH:MM:SS.sss)
    let time_formats = ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"];
    for format in &time_formats {
        if let Ok(time) = chrono::NaiveTime::parse_from_str(time_str, format) {
            // Convert time to seconds since midnight
            let datetime = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap().and_time(time);
            let seconds = datetime.timestamp() % 86400; // seconds in a day
            return Ok(seconds as u64);
        }
    }
    
    Err(format!("Unable to parse timestamp: {}", time_str))
}

fn test_binning_logic() {
    // Simulate the data from test_data_300k
    let times = vec![
        "00:00:00.000",
        "00:00:00.100",
        "00:00:00.200",
        "00:00:01.000",
        "00:00:01.100",
        "00:00:30.000",
        "00:01:00.000",
        "00:01:30.000",
        "00:02:00.000",
        "00:30:00.000",
        "01:00:00.000",
        "02:00:00.000",
        "03:00:00.000",
    ];
    
    println!("\nFixed interval binning (30 seconds):");
    println!("{:<20} | {:<10} | Bin", "Time", "Seconds");
    println!("{}", "-".repeat(45));
    
    let interval = 30; // 30 seconds
    for time_str in &times {
        if let Ok(seconds) = parse_time(time_str) {
            let bin = seconds / interval;
            println!("{:<20} | {:<10} | {}", time_str, seconds, bin);
        }
    }
    
    println!("\nFixed interval binning (1 hour = 3600 seconds):");
    println!("{:<20} | {:<10} | Bin", "Time", "Seconds");
    println!("{}", "-".repeat(45));
    
    let interval = 3600; // 1 hour
    for time_str in &times {
        if let Ok(seconds) = parse_time(time_str) {
            let bin = seconds / interval;
            println!("{:<20} | {:<10} | {}", time_str, seconds, bin);
        }
    }
}