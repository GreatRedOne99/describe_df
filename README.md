### this is from this GIST - https://gist.github.com/nazq/f3fc6e6b15c895fb8b474013c4932d62 

## But requrired fixes to work with Polars 0.51.0 for Rust 


# Polars DataFrame describe() for Rust

A Rust implementation of the `describe()` function for Polars DataFrames and LazyFrames, providing summary statistics similar to pandas/polars in Python.

## Why?

Polars for Rust doesn't include a built-in `describe()` method like its Python counterpart. This implementation fills that gap, providing:

- Summary statistics for DataFrames and LazyFrames
- Efficient computation without unnecessary data collection
- Support for numeric, string, boolean, and temporal columns
- Customizable percentiles
- Follows the Python Polars implementation pattern

## Features

- ✅ Works with both `DataFrame` and `LazyFrame`
- ✅ Computes: count, null_count, mean, std, min, percentiles, max
- ✅ Efficient single-pass aggregation for LazyFrames
- ✅ Handles mixed column types gracefully
- ✅ Customizable percentiles (default: 25%, 50%, 75%)

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
polars = { version = "0.51", features = ["lazy"] }
anyhow = "1.0"
```

Then use it in your code:

```rust
use polars::prelude::*;
use your_module::Describable;

fn main() -> Result<()> {
    // Create a DataFrame
    let df = df! {
        "ints" => [1, 2, 3, 4, 5],
        "floats" => [1.0, 2.5, 3.0, 4.5, 5.0],
        "strings" => ["a", "b", "c", "d", "e"],
        "bools" => [true, false, true, false, true],
    }?;

    // Get summary statistics with default percentiles (25%, 50%, 75%)
    let stats = df.describe(None)?;
    println!("{}", stats);

    // Or use custom percentiles
    let custom_stats = df.describe(Some(vec![0.1, 0.5, 0.9]))?;
    println!("{}", custom_stats);

    // Also works with LazyFrames (efficient - no unnecessary collection!)
    let lf = df.lazy();
    let lazy_stats = lf.describe(None)?;
    println!("{}", lazy_stats);

    Ok(())
}
```

## Output Example

```
shape: (9, 5)
┌────────────┬──────┬─────────┬──────────┬───────┐
│ statistic  ┆ ints ┆ floats  ┆ strings  ┆ bools │
│ ---        ┆ ---  ┆ ---     ┆ ---      ┆ ---   │
│ str        ┆ str  ┆ str     ┆ str      ┆ str   │
╞════════════╪══════╪═════════╪══════════╪═══════╡
│ count      ┆ 5    ┆ 5       ┆ 5        ┆ 5     │
│ null_count ┆ 0    ┆ 0       ┆ 0        ┆ 0     │
│ mean       ┆ 3.0  ┆ 3.2     ┆ null     ┆ 0.6   │
│ std        ┆ 1.58 ┆ 1.48    ┆ null     ┆ null  │
│ min        ┆ 1    ┆ 1.0     ┆ a        ┆ false │
│ 25%        ┆ 2.0  ┆ 2.0     ┆ null     ┆ null  │
│ 50%        ┆ 3.0  ┆ 3.0     ┆ null     ┆ null  │
│ 75%        ┆ 4.0  ┆ 4.0     ┆ null     ┆ null  │
│ max        ┆ 5    ┆ 5.0     ┆ e        ┆ true  │
└────────────┴──────┴─────────┴──────────┴───────┘
```

## Implementation Details

- **Efficient LazyFrame handling**: Uses `collect_schema()` to get column information without collecting data
- **Single-pass aggregation**: All statistics are computed in one query
- **Type-aware statistics**:
  - Numeric columns: all statistics
  - Boolean columns: count, null_count, mean (as 0/1), min (false), max (true)
  - String/Categorical: count, null_count, min, max
  - Temporal columns: count, null_count, mean, min, percentiles, max
- **Follows Python pattern**: Implementation closely mirrors the Python Polars `describe()` method

## Requirements

- Rust 1.70+
- Polars 0.51+ with `lazy` feature

## License

MIT OR Apache-2.0 (same as Polars)

## Contributing

Feel free to use this code and adapt it to your needs. If you find bugs or have improvements, please share them!

## Credits

Based on the Python Polars implementation, adapted for Rust by the community.