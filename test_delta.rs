use leaf::core::transformations::DataTransformer;

fn main() {
    let transformer = DataTransformer::new();
    
    println!("Testing delta null handling...");
    match transformer.test_delta_null_handling() {
        Ok(()) => println!("✅ Test passed! Null values are working correctly."),
        Err(e) => println!("❌ Test failed: {}", e),
    }
} 