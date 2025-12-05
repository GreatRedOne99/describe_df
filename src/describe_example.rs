//! Example usage of the describe functionality

use anyhow::Result;
use polars::prelude::*;

// Include the describe module
mod describe;
use describe::Describable;

fn main() -> Result<()> {
    // Create a sample DataFrame with different data types
    let df = df! {
        "integers" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "floats" => [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5],
        "strings" => ["apple", "banana", "cherry", "date", "elderberry",
                      "fig", "grape", "honeydew", "kiwi", "lemon"],
        "booleans" => [true, false, true, false, true,
                       false, true, false, true, false],
        "nullables" => [Some(1), None, Some(3), None, Some(5),
                        Some(6), None, Some(8), Some(9), Some(10)],
    }?;

    println!("Original DataFrame:");
    println!("{}\n", df);

    // Example 1: Basic describe with default percentiles (25%, 50%, 75%)
    println!("=== Basic describe() with default percentiles ===");
    let basic_stats = df.describe(None)?;
    println!("{}\n", basic_stats);

    // Example 2: Custom percentiles
    println!("=== describe() with custom percentiles (10%, 50%, 90%) ===");
    let custom_stats = df.describe(Some(vec![0.1, 0.5, 0.9]))?;
    println!("{}\n", custom_stats);

    // Example 3: Using with LazyFrame for efficiency
    println!("=== Using with LazyFrame (no unnecessary data collection) ===");
    let lazy_df = df.lazy();
    let lazy_stats = lazy_df.describe(Some(vec![0.25, 0.5, 0.75]))?;
    println!("{}\n", lazy_stats);

    // Example 4: Demonstrating with a larger dataset
    println!("=== Large dataset example ===");
    let large_df = df! {
        "values" => (0..10000).collect::<Vec<_>>(),
        "randoms" => (0..10000).map(|i| (i as f64) * 0.1).collect::<Vec<_>>(),
    }?;

    let large_stats = large_df.describe(Some(vec![0.05, 0.25, 0.5, 0.75, 0.95]))?;
    println!("Stats for 10,000 row DataFrame:");
    println!("{}\n", large_stats);

    // Example 5: Working with time series data
    use chrono::NaiveDate;

    let dates_df = df! {
        "dates" => vec![
            NaiveDate::from_ymd_opt(2024, 1, 1),
            NaiveDate::from_ymd_opt(2024, 2, 1),
            NaiveDate::from_ymd_opt(2024, 3, 1),
            NaiveDate::from_ymd_opt(2024, 4, 1),
            NaiveDate::from_ymd_opt(2024, 5, 1),
        ].into_iter().flatten().collect::<Vec<_>>(),
        "values" => [100, 150, 120, 180, 200],
    }?;

    println!("=== Time series data ===");
    let time_stats = dates_df.describe(None)?;
    println!("{}", time_stats);

    Ok(())
}