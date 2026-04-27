#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

INPUT_DIR="$ROOT_DIR/examples"
OUTPUT_DIR="$ROOT_DIR/output"
BIN_PATH="$ROOT_DIR/target/release/filelens"
STRICT=0

usage() {
  cat <<'USAGE'
Usage: scripts/convert_inputs.sh [options]

Options:
  --input-dir <dir>   Input directory to scan recursively (default: examples)
  --output-dir <dir>  Output directory for parquet files (default: output)
  --bin <path>        filelens binary path (default: ./target/release/filelens)
  --strict            Fail on first conversion error (default: skip and continue)
  -h, --help          Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input-dir)
      INPUT_DIR="${2:-}"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --bin)
      BIN_PATH="${2:-}"
      shift 2
      ;;
    --strict)
      STRICT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! -d "$INPUT_DIR" ]]; then
  echo "Input directory not found: $INPUT_DIR" >&2
  exit 1
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "Binary not found or not executable: $BIN_PATH" >&2
  echo "Build it first: cargo build --release" >&2
  exit 1
fi

FILES=()
while IFS= read -r file; do
  FILES+=("$file")
done < <(find "$INPUT_DIR" -type f \
  \( -name '*.csv' -o -name '*.tsv' -o -name '*.psv' -o -name '*.txt' \
  -o -name '*.json' -o -name '*.ndjson' -o -name '*.hl7' -o -name '*.msg' \
  -o -name '*.ttl' -o -name '*.rdf' -o -name '*.ttl.html' \
  -o -name '*.xlsx' -o -name '*.xlsm' -o -name '*.xls' \
  -o -name '*.cxml' -o -name '*.xcml' -o -name '*.xml' \
  -o -name '*.gz' \) | sort)

if [[ "${#FILES[@]}" -eq 0 ]]; then
  echo "No supported input files found in: $INPUT_DIR" >&2
  exit 1
fi

echo "Converting ${#FILES[@]} files from $INPUT_DIR -> $OUTPUT_DIR"
converted=0
failed=0
for f in "${FILES[@]}"; do
  rel="${f#"$INPUT_DIR"/}"
  out="$OUTPUT_DIR/$rel.parquet"
  mkdir -p "$(dirname "$out")"
  if "$BIN_PATH" convert "$f" --out "$out"; then
    converted=$((converted + 1))
  else
    failed=$((failed + 1))
    echo "WARN: failed to convert $f" >&2
    if [[ "$STRICT" -eq 1 ]]; then
      exit 1
    fi
  fi
done

if [[ "$failed" -gt 0 ]]; then
  echo "Done with warnings. Converted: $converted, Failed: $failed"
else
  echo "Done. Converted: $converted"
fi
