use anyhow::Result;

fn main() -> Result<()> {
    println!("=== Direct Time Parsing Test ===\n");
    
    // Test values from the actual data
    let test_values = vec![
        "00:00:00.000",
        "00:00:00.365", 
        "00:03:20.121",
        "10:06:58",
        "21:43:45",
        "",  // Empty string (null)
        "23:59:59.999",
    ];
    
    println!("Testing parse_timestamp function from time_grouping.rs:\n");
    
    for value in &test_values {
        print!("Parsing '{}': ", value);
        match parse_timestamp(value) {
            Ok(timestamp) => println!("Success! Timestamp: {}", timestamp),
            Err(e) => println!("Failed: {}", e),
        }
    }
    
    println!("\n\nTesting chrono parsing directly:\n");
    
    for value in &test_values {
        if value.is_empty() {
            println!("Skipping empty string");
            continue;
        }
        
        print!("Parsing '{}' with chrono: ", value);
        
        // Try different formats
        if let Ok(time) = chrono::NaiveTime::parse_from_str(value, "%H:%M:%S%.f") {
            println!("✓ Parsed with %H:%M:%S%.f format: {:?}", time);
        } else if let Ok(time) = chrono::NaiveTime::parse_from_str(value, "%H:%M:%S%.3f") {
            println!("✓ Parsed with %H:%M:%S%.3f format: {:?}", time);
        } else if let Ok(time) = chrono::NaiveTime::parse_from_str(value, "%H:%M:%S") {
            println!("✓ Parsed with %H:%M:%S format: {:?}", time);
        } else {
            println!("✗ Failed to parse with any time format");
        }
    }
    
    println!("\n\nTesting fixed interval binning logic:\n");
    
    // Test the binning calculation
    let interval_seconds = 3600; // 1 hour
    let test_timestamps = vec![
        0,      // 00:00:00
        1800,   // 00:30:00
        3600,   // 01:00:00
        5400,   // 01:30:00
        7200,   // 02:00:00
    ];
    
    for ts in &test_timestamps {
        let bin = ts / interval_seconds;
        let hours = ts / 3600;
        let minutes = (ts % 3600) / 60;
        let seconds = ts % 60;
        println!("Timestamp {} ({:02}:{:02}:{:02}) -> Bin {}", ts, hours, minutes, seconds, bin);
    }
    
    Ok(())
}

// Copy of the parse_timestamp function from time_grouping.rs for testing
fn parse_timestamp(time_str: &str) -> Result<i64> {
    // Handle empty strings
    if time_str.trim().is_empty() {
        return Err(anyhow::anyhow!("Empty timestamp string"));
    }
    
    // Try different timestamp formats
    if let Ok(timestamp) = time_str.parse::<i64>() {
        return Ok(timestamp);
    }
    
    // Try ISO 8601 format
    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(time_str) {
        return Ok(datetime.timestamp());
    }
    
    // Try naive datetime formats
    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%H:%M:%S%.f",
        "%H:%M:%S",
        "%H:%M",
    ];
    
    for format in &formats {
        if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(time_str, format) {
            return Ok(datetime.and_utc().timestamp());
        }
    }
    
    // Try time-only format (assume today's date)
    // Try with milliseconds first
    if let Ok(time) = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f") {
        let today = chrono::Utc::now().date_naive();
        let datetime = today.and_time(time);
        return Ok(datetime.and_utc().timestamp());
    }
    
    if let Ok(time) = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S%.3f") {
        let today = chrono::Utc::now().date_naive();
        let datetime = today.and_time(time);
        return Ok(datetime.and_utc().timestamp());
    }
    
    if let Ok(time) = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S") {
        let today = chrono::Utc::now().date_naive();
        let datetime = today.and_time(time);
        return Ok(datetime.and_utc().timestamp());
    }
    
    if let Ok(time) = chrono::NaiveTime::parse_from_str(time_str, "%H:%M") {
        let today = chrono::Utc::now().date_naive();
        let datetime = today.and_time(time);
        return Ok(datetime.and_utc().timestamp());
    }
    
    Err(anyhow::anyhow!("Unable to parse timestamp: '{}'. Supported formats: Unix timestamp, ISO 8601, YYYY-MM-DD HH:MM:SS, HH:MM:SS.sss, HH:MM:SS, HH:MM", time_str))
}