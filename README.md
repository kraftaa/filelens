# filelens

`filelens` is a focused CLI for messy tabular files:

- inspect structure and quality issues
- infer a simple schema
- convert to clean parquet

## Supported inputs (v1)

- `.csv`
- `.xlsx` (`.xlsm` / `.xls` also accepted)

## Build

```bash
cargo build --release
```

Binary path:

```bash
./target/release/filelens
```

## How to call it

Inspect:

```bash
./target/release/filelens inspect data/file.xlsx
```

Schema:

```bash
./target/release/filelens schema data/file.xlsx
```

Convert:

```bash
./target/release/filelens convert data/file.xlsx --out data/file.parquet
```

## What conversion does

- detects likely header row
- skips metadata rows above header
- normalizes column names (`Sample ID` -> `sample_id`)
- infers types (`string`, `int`, `float`, `bool`, `date`)
- drops fully empty columns
- writes parquet

## Notes

- Header and type inference are deterministic heuristics.
- For best results, keep one logical table per file/sheet.
