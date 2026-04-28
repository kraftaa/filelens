use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use calamine::{Reader, open_workbook_auto};
use chrono::{NaiveDate, NaiveDateTime};
use clap::{Parser, Subcommand, ValueEnum};
use flate2::read::GzDecoder;
use polars::prelude::*;
use quick_xml::events::{BytesStart, Event as XmlEvent};
use quick_xml::{Reader as XmlReader, name::QName};
use rio_api::model::{Literal as RioLiteral, Subject as RioSubject, Term as RioTerm};
use rio_api::parser::TriplesParser;
use rio_turtle::TurtleParser;
use serde::Serialize;
use serde_json::Value as JsonValue;

const DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m/%d/%y", "%d-%b-%Y", "%d-%B-%Y",
];

const DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
];

#[derive(Debug, Parser)]
#[command(
    name = "filelens",
    version,
    about = "Inspect and normalize messy tabular data files"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Inspect tabular file structure and quality signals
    Inspect {
        /// Input file (.csv/.tsv/.psv/.txt/.xlsx/.xlsm/.xls/.cxml/.xcml/.xml/.json/.ndjson/.hl7/.msg/.ttl/.rdf/.html, plus .gz variants)
        input: PathBuf,
        /// Force parser mode (auto, tabular, cxml, json, fhir, hl7, cda, rdf)
        #[arg(long, value_enum, default_value_t = ParseMode::Auto)]
        parser: ParseMode,
        /// cXML extraction mode (mapped = curated fields, auto = path-based fields, both = mapped + path-based)
        #[arg(long, value_enum, default_value_t = CxmlMode::Mapped)]
        cxml_mode: CxmlMode,
    },
    /// Print inferred schema as JSON
    Schema {
        /// Input file (.csv/.tsv/.psv/.txt/.xlsx/.xlsm/.xls/.cxml/.xcml/.xml/.json/.ndjson/.hl7/.msg/.ttl/.rdf/.html, plus .gz variants)
        input: PathBuf,
        /// Force parser mode (auto, tabular, cxml, json, fhir, hl7, cda, rdf)
        #[arg(long, value_enum, default_value_t = ParseMode::Auto)]
        parser: ParseMode,
        /// cXML extraction mode (mapped = curated fields, auto = path-based fields, both = mapped + path-based)
        #[arg(long, value_enum, default_value_t = CxmlMode::Mapped)]
        cxml_mode: CxmlMode,
    },
    /// Convert a messy file into clean parquet
    Convert {
        /// Input file (.csv/.tsv/.psv/.txt/.xlsx/.xlsm/.xls/.cxml/.xcml/.xml/.json/.ndjson/.hl7/.msg/.ttl/.rdf/.html, plus .gz variants)
        input: PathBuf,
        /// Output parquet path
        #[arg(long)]
        out: PathBuf,
        /// Force parser mode (auto, tabular, cxml, json, fhir, hl7, cda, rdf)
        #[arg(long, value_enum, default_value_t = ParseMode::Auto)]
        parser: ParseMode,
        /// cXML extraction mode (mapped = curated fields, auto = path-based fields, both = mapped + path-based)
        #[arg(long, value_enum, default_value_t = CxmlMode::Mapped)]
        cxml_mode: CxmlMode,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ParseMode {
    Auto,
    Tabular,
    Cxml,
    Json,
    Fhir,
    Hl7,
    Cda,
    Rdf,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CxmlMode {
    Mapped,
    Auto,
    Both,
}

#[derive(Debug, Clone)]
struct Profile {
    header_row: usize,
    metadata_rows: usize,
    total_columns: usize,
    rows: Vec<Vec<String>>,
    columns: Vec<ColumnStats>,
}

#[derive(Debug, Clone)]
struct ColumnStats {
    index: usize,
    name: String,
    inferred_type: ColumnType,
    null_count: usize,
    non_null_count: usize,
    numeric_count: usize,
    bool_count: usize,
    date_count: usize,
    string_count: usize,
}

#[derive(Debug, Clone)]
enum ColumnType {
    String,
    Int,
    Float,
    Bool,
    Date,
}

impl ColumnType {
    fn as_schema_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Date => "date",
        }
    }
}

#[derive(Debug, Serialize)]
struct SchemaDoc {
    columns: Vec<SchemaColumn>,
}

#[derive(Debug, Serialize)]
struct SchemaColumn {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Inspect {
            input,
            parser,
            cxml_mode,
        } => run_inspect(&input, parser, cxml_mode),
        Commands::Schema {
            input,
            parser,
            cxml_mode,
        } => run_schema(&input, parser, cxml_mode),
        Commands::Convert {
            input,
            out,
            parser,
            cxml_mode,
        } => run_convert(&input, &out, parser, cxml_mode),
    }
}

fn run_inspect(input: &Path, parser: ParseMode, cxml_mode: CxmlMode) -> Result<()> {
    let rows = read_rows(input, parser, cxml_mode)?;
    let profile = build_profile(rows)?;

    let possible_numeric = profile
        .columns
        .iter()
        .filter(|c| ratio(c.numeric_count, c.non_null_count) >= 0.70)
        .count();

    println!("Detected:");
    println!("- header row: {}", profile.header_row + 1);
    if profile.metadata_rows > 0 {
        println!("- metadata rows: 1-{}", profile.metadata_rows);
    } else {
        println!("- metadata rows: none");
    }
    println!("- columns: {}", profile.total_columns);
    println!("- possible numeric columns: {possible_numeric}");
    println!();

    let mut warnings: Vec<String> = Vec::new();
    let empty_cols: Vec<String> = profile
        .columns
        .iter()
        .filter(|c| c.non_null_count == 0)
        .map(|c| c.name.clone())
        .collect();
    if !empty_cols.is_empty() {
        warnings.push(format!("empty columns detected: {}", empty_cols.join(", ")));
    }

    for col in &profile.columns {
        if col.non_null_count == 0 {
            continue;
        }

        let null_pct = ratio(col.null_count, col.null_count + col.non_null_count);
        if null_pct >= 0.30 {
            warnings.push(format!(
                "{}% nulls in column \"{}\"",
                (null_pct * 100.0).round() as i32,
                col.name
            ));
        }

        if has_mixed_types(col) {
            warnings.push(format!("mixed types in column \"{}\"", col.name));
        }
    }

    println!("Warnings:");
    if warnings.is_empty() {
        println!("- none");
    } else {
        for warning in warnings {
            println!("- {warning}");
        }
    }

    Ok(())
}

fn run_schema(input: &Path, parser: ParseMode, cxml_mode: CxmlMode) -> Result<()> {
    let rows = read_rows(input, parser, cxml_mode)?;
    let profile = build_profile(rows)?;

    let schema = SchemaDoc {
        columns: profile
            .columns
            .iter()
            .map(|col| SchemaColumn {
                name: col.name.clone(),
                col_type: col.inferred_type.as_schema_str().to_string(),
            })
            .collect(),
    };

    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}

fn run_convert(input: &Path, out: &Path, parser: ParseMode, cxml_mode: CxmlMode) -> Result<()> {
    let rows = read_rows(input, parser, cxml_mode)?;
    let profile = build_profile(rows)?;

    let mut output_columns = Vec::new();
    for col in &profile.columns {
        if col.non_null_count == 0 {
            continue;
        }
        let values: Vec<String> = profile
            .rows
            .iter()
            .map(|row| row.get(col.index).cloned().unwrap_or_default())
            .collect();
        let series = build_series(&col.name, &col.inferred_type, &values)?;
        output_columns.push(series.into_column());
    }

    let mut df =
        DataFrame::new(output_columns).context("unable to create dataframe from parsed columns")?;

    let writer = File::create(out)
        .with_context(|| format!("failed to create output parquet file: {}", out.display()))?;
    ParquetWriter::new(writer)
        .finish(&mut df)
        .context("failed to write parquet file")?;

    println!(
        "Converted {} -> {} (rows: {}, columns: {})",
        input.display(),
        out.display(),
        df.height(),
        df.width()
    );

    Ok(())
}

fn read_rows(input: &Path, parser: ParseMode, cxml_mode: CxmlMode) -> Result<Vec<Vec<String>>> {
    let ext = input
        .extension()
        .and_then(|x| x.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    let rows = if ext == "gz" {
        read_rows_from_gzip(input, parser, cxml_mode)?
    } else if matches!(parser, ParseMode::Auto) {
        read_rows_auto_from_extension(input, &ext, cxml_mode)?
    } else {
        read_rows_with_mode_from_path(input, parser, &ext, cxml_mode)?
    };

    finalize_rows(rows)
}

fn finalize_rows(mut rows: Vec<Vec<String>>) -> Result<Vec<Vec<String>>> {
    let max_cols = rows.iter().map(|r| r.len()).max().unwrap_or(0);
    if max_cols == 0 {
        bail!("file has no tabular content");
    }
    for row in &mut rows {
        row.resize(max_cols, String::new());
    }

    while rows
        .last()
        .is_some_and(|row| row.iter().all(|v| v.trim().is_empty()))
    {
        rows.pop();
    }

    if rows.is_empty() {
        bail!("file has no non-empty rows");
    }

    Ok(rows)
}

fn read_rows_auto_from_extension(
    input: &Path,
    ext: &str,
    cxml_mode: CxmlMode,
) -> Result<Vec<Vec<String>>> {
    match ext {
        "csv" => read_delimited_rows(input, None),
        "tsv" => read_delimited_rows(input, Some(b'\t')),
        "psv" => read_delimited_rows(input, Some(b'|')),
        "txt" => read_delimited_rows(input, None),
        "xlsx" | "xlsm" | "xls" => read_excel_rows(input),
        "hl7" | "msg" => {
            let content = read_text_file_lossy(input, "hl7 message file")?;
            read_rows_from_content(&content, ParseMode::Auto, Some(ext), cxml_mode)
        }
        "json" | "ndjson" => {
            let content = read_text_file_lossy(input, "json file")?;
            read_rows_from_content(&content, ParseMode::Auto, Some(ext), cxml_mode)
        }
        "ttl" | "rdf" => {
            let content = read_text_file_lossy(input, "rdf turtle file")?;
            read_rows_from_content(&content, ParseMode::Auto, Some(ext), cxml_mode)
        }
        "html" | "htm" => {
            let content = read_text_file_lossy(input, "html file")?;
            read_rows_from_content(&content, ParseMode::Auto, Some(ext), cxml_mode)
        }
        "xml" | "cxml" | "xcml" => {
            let content = read_text_file_lossy(input, "xml/cxml file")?;
            read_rows_from_content(&content, ParseMode::Auto, Some(ext), cxml_mode)
        }
        _ => bail!(
            "unsupported file extension \"{}\". supported: .csv, .tsv, .psv, .txt, .xlsx, .xlsm, .xls, .cxml, .xcml, .xml, .json, .ndjson, .hl7, .msg, .ttl, .rdf, .html, and .gz variants",
            ext
        ),
    }
}

fn read_rows_with_mode_from_path(
    input: &Path,
    parser: ParseMode,
    ext: &str,
    cxml_mode: CxmlMode,
) -> Result<Vec<Vec<String>>> {
    match parser {
        ParseMode::Auto => read_rows_auto_from_extension(input, ext, cxml_mode),
        ParseMode::Tabular => match ext {
            "xlsx" | "xlsm" | "xls" => read_excel_rows(input),
            "tsv" => read_delimited_rows(input, Some(b'\t')),
            "psv" => read_delimited_rows(input, Some(b'|')),
            "csv" | "txt" => read_delimited_rows(input, None),
            _ => {
                let content = read_text_file_lossy(input, "tabular text file")?;
                read_delimited_content(&content, None)
            }
        },
        ParseMode::Cxml => {
            let content = read_text_file_lossy(input, "xml/cxml file")?;
            read_cxml_content_from_text(content, cxml_mode)
        }
        ParseMode::Json => {
            let content = read_text_file_lossy(input, "json file")?;
            parse_json_content(&content)
        }
        ParseMode::Fhir => {
            let content = read_text_file_lossy(input, "fhir json file")?;
            parse_fhir_content(&content)
        }
        ParseMode::Hl7 => {
            let content = read_text_file_lossy(input, "hl7 message file")?;
            parse_hl7_content(&content)
        }
        ParseMode::Cda => {
            let content = read_text_file_lossy(input, "cda xml file")?;
            parse_cda_content(&content)
        }
        ParseMode::Rdf => {
            let content = read_text_file_lossy(input, "rdf turtle file")?;
            parse_rdf_content(&content)
        }
    }
}

fn read_rows_from_content(
    content: &str,
    parser: ParseMode,
    hint_ext: Option<&str>,
    cxml_mode: CxmlMode,
) -> Result<Vec<Vec<String>>> {
    match parser {
        ParseMode::Auto => {
            if let Some(ext) = hint_ext {
                match ext {
                    "csv" | "txt" => return read_delimited_content(content, None),
                    "tsv" => return read_delimited_content(content, Some(b'\t')),
                    "psv" => return read_delimited_content(content, Some(b'|')),
                    "cxml" | "xcml" => {
                        return read_cxml_content_from_text(content.to_string(), cxml_mode);
                    }
                    "hl7" | "msg" => return parse_hl7_content(content),
                    "ttl" | "rdf" => return parse_rdf_content(content),
                    "json" | "ndjson" => {
                        if looks_fhir_json_content(content) {
                            return parse_fhir_content(content);
                        }
                        return parse_json_content(content);
                    }
                    "html" | "htm" => {
                        return parse_html_content(content);
                    }
                    "xml" => {
                        if content.contains("<ClinicalDocument") {
                            return parse_cda_content(content);
                        }
                        if content.contains("<cXML") || content.contains("<NaaccrData") {
                            return read_cxml_content_from_text(content.to_string(), cxml_mode);
                        }
                    }
                    _ => {}
                }
            }

            let trimmed = content.trim_start();
            if looks_turtle_content(trimmed) {
                return parse_rdf_content(content);
            }
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                if looks_fhir_json_content(content) {
                    return parse_fhir_content(content);
                }
                return parse_json_content(content);
            }
            if trimmed.starts_with('<') {
                if looks_html_content(trimmed) {
                    return parse_html_content(content);
                }
                if content.contains("<ClinicalDocument") {
                    return parse_cda_content(content);
                }
                let cxml_attempt = read_cxml_content_from_text(content.to_string(), cxml_mode);
                if cxml_attempt.is_ok() {
                    return cxml_attempt;
                }
                let cda_attempt = parse_cda_content(content);
                if cda_attempt.is_ok() {
                    return cda_attempt;
                }
                return cxml_attempt;
            }
            if looks_hl7_message(content) {
                return parse_hl7_content(content);
            }

            read_delimited_content(content, None)
        }
        ParseMode::Tabular => match hint_ext {
            Some("tsv") => read_delimited_content(content, Some(b'\t')),
            Some("psv") => read_delimited_content(content, Some(b'|')),
            Some("csv") => read_delimited_content(content, Some(b',')),
            _ => read_delimited_content(content, None),
        },
        ParseMode::Cxml => read_cxml_content_from_text(content.to_string(), cxml_mode),
        ParseMode::Json => parse_json_content(content),
        ParseMode::Fhir => parse_fhir_content(content),
        ParseMode::Hl7 => parse_hl7_content(content),
        ParseMode::Cda => parse_cda_content(content),
        ParseMode::Rdf => parse_rdf_content(content),
    }
}

fn read_rows_from_gzip(
    input: &Path,
    parser: ParseMode,
    cxml_mode: CxmlMode,
) -> Result<Vec<Vec<String>>> {
    let raw_gz = fs::read(input)
        .with_context(|| format!("failed to open gzip file: {}", input.display()))?;
    if raw_gz.is_empty() {
        bail!("file has no non-empty rows");
    }

    let decoded = decompress_gzip_bytes(&raw_gz)
        .with_context(|| format!("failed to decompress gzip input: {}", input.display()))?;
    if decoded.is_empty() {
        bail!("gzip archive is empty after decompression");
    }

    let text = decode_text_bytes(decoded)?;
    let inner_ext = infer_inner_extension_from_gz_path(input);
    read_rows_from_content(&text, parser, inner_ext.as_deref(), cxml_mode)
}

fn infer_inner_extension_from_gz_path(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_string_lossy().to_string();
    Path::new(&stem)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
}

fn decompress_gzip_bytes(raw_gz: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(raw_gz);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

fn read_delimited_rows(input: &Path, forced_delimiter: Option<u8>) -> Result<Vec<Vec<String>>> {
    let content = read_text_file_lossy(input, "delimited file")?;
    read_delimited_content(&content, forced_delimiter)
}

fn read_delimited_content(content: &str, forced_delimiter: Option<u8>) -> Result<Vec<Vec<String>>> {
    let delimiter = forced_delimiter.unwrap_or_else(|| detect_delimiter(content));
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(delimiter)
        .from_reader(content.as_bytes());

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        rows.push(record.iter().map(ToString::to_string).collect());
    }
    Ok(rows)
}

fn strip_utf8_bom(content: &mut String) {
    if content.starts_with('\u{feff}') {
        let _ = content.remove(0);
    }
}

fn read_text_file_lossy(input: &Path, label: &str) -> Result<String> {
    let raw_bytes =
        fs::read(input).with_context(|| format!("failed to open {label}: {}", input.display()))?;
    decode_text_bytes(raw_bytes)
}

fn decode_text_bytes(raw_bytes: Vec<u8>) -> Result<String> {
    if raw_bytes.is_empty() {
        bail!("file has no non-empty rows");
    }

    let mut content = match String::from_utf8(raw_bytes) {
        Ok(valid) => valid,
        Err(err) => {
            // Fallback keeps parser resilient for legacy exports with mixed encodings.
            String::from_utf8_lossy(err.as_bytes()).into_owned()
        }
    };
    strip_utf8_bom(&mut content);

    if content.trim().is_empty() {
        bail!("file has no non-empty rows");
    }

    Ok(content)
}

fn read_excel_rows(input: &Path) -> Result<Vec<Vec<String>>> {
    let mut workbook = open_workbook_auto(input)
        .with_context(|| format!("failed to open spreadsheet: {}", input.display()))?;
    let sheet_names = workbook.sheet_names().to_vec();
    if sheet_names.is_empty() {
        bail!("spreadsheet has no sheets");
    }

    let mut first_readable: Option<Vec<Vec<String>>> = None;
    for sheet_name in &sheet_names {
        let range = match workbook.worksheet_range(sheet_name) {
            Ok(range) => range,
            Err(_) => continue,
        };
        let rows: Vec<Vec<String>> = range
            .rows()
            .map(|row| row.iter().map(ToString::to_string).collect())
            .collect();

        if first_readable.is_none() {
            first_readable = Some(rows.clone());
        }

        if rows
            .iter()
            .any(|row| row.iter().any(|cell| !cell.trim().is_empty()))
        {
            return Ok(rows);
        }
    }

    first_readable.context("failed to read any worksheet content")
}

fn read_cxml_content_from_text(
    mut content: String,
    cxml_mode: CxmlMode,
) -> Result<Vec<Vec<String>>> {
    normalize_smart_quotes(&mut content);

    if content.contains("<NaaccrData") {
        return parse_naaccr_content(&content);
    }

    parse_cxml_content(&content, cxml_mode)
}

fn normalize_smart_quotes(content: &mut String) {
    if content.contains(['“', '”', '‘', '’']) {
        *content = content
            .replace('“', "\"")
            .replace('”', "\"")
            .replace('‘', "'")
            .replace('’', "'");
    }
}

fn parse_cxml_content(content: &str, cxml_mode: CxmlMode) -> Result<Vec<Vec<String>>> {
    let include_mapped = !matches!(cxml_mode, CxmlMode::Auto);
    let include_auto = !matches!(cxml_mode, CxmlMode::Mapped);
    let mut reader = XmlReader::from_str(content);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();

    let mut order_id = String::new();
    let mut order_date = String::new();
    let mut payload_id = String::new();
    let mut payload_timestamp = String::new();
    let mut ship_to_name = String::new();
    let mut bill_to_name = String::new();
    let mut invoice_purpose = String::new();
    let mut notice_id = String::new();
    let mut quote_id = String::new();
    let mut quote_date = String::new();
    let mut header_extrinsics: HashMap<String, String> = HashMap::new();

    let mut current_item: Option<HashMap<String, String>> = None;
    let mut current_item_tag: Option<String> = None;
    let mut current_item_extrinsic: Option<String> = None;
    let mut current_header_extrinsic: Option<String> = None;
    let mut rows: Vec<HashMap<String, String>> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            XmlEvent::Start(event) => {
                let name = qname_to_string(event.name());
                path.push(name.clone());
                match name.as_str() {
                    "cXML" => {
                        if let Some(v) = attr_value(&reader, &event, "payloadID") {
                            payload_id = v;
                        }
                        if let Some(v) = attr_value(&reader, &event, "timestamp") {
                            payload_timestamp = v;
                        }
                    }
                    "OrderRequestHeader" => {
                        if let Some(v) = attr_value(&reader, &event, "orderID") {
                            order_id = v;
                        }
                        if order_id.is_empty()
                            && let Some(v) = attr_value(&reader, &event, "orderRequestID")
                        {
                            order_id = v;
                        }
                        if let Some(v) = attr_value(&reader, &event, "orderDate") {
                            order_date = v;
                        }
                    }
                    "ShipNoticeHeader" => {
                        if let Some(v) = attr_value(&reader, &event, "noticeID") {
                            notice_id = v;
                        }
                    }
                    "InvoiceRequest" => {
                        if let Some(v) = attr_value(&reader, &event, "purpose") {
                            invoice_purpose = v;
                        }
                    }
                    tag if is_cxml_line_item(tag, &path) => {
                        let mut item = seed_cxml_item_row(CxmlSeed {
                            order_id: &order_id,
                            order_date: &order_date,
                            payload_id: &payload_id,
                            payload_timestamp: &payload_timestamp,
                            invoice_purpose: &invoice_purpose,
                            notice_id: &notice_id,
                            quote_id: &quote_id,
                            quote_date: &quote_date,
                            ship_to_name: &ship_to_name,
                            bill_to_name: &bill_to_name,
                            header_extrinsics: &header_extrinsics,
                        });
                        if let Some(v) = attr_value(&reader, &event, "lineNumber") {
                            item.insert("line_number".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "quantity") {
                            item.insert("quantity".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "requestedDeliveryDate") {
                            item.insert("requested_delivery_date".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "itemClassification") {
                            item.insert("item_classification".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "addressID") {
                            item.insert("address_id".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "addressName") {
                            item.insert("address_name".to_string(), v);
                        }
                        current_item = Some(item);
                        current_item_tag = Some(tag.to_string());
                    }
                    "Extrinsic" => {
                        if current_item.is_some()
                            && (path_ends_with(&path, &["ItemDetail", "Extrinsic"])
                                || path_ends_with(&path, &["BlanketItemDetail", "Extrinsic"])
                                || path_ends_with(&path, &["InvoiceDetailItem", "Extrinsic"])
                                || path_ends_with(&path, &["ShipNoticeItem", "Extrinsic"]))
                            && let Some(ex_name) = attr_value(&reader, &event, "name")
                        {
                            current_item_extrinsic =
                                Some(format!("extrinsic_{}", normalize_header(&ex_name)));
                        } else if path_ends_with(&path, &["InvoiceDetailRequest", "Extrinsic"])
                            || path_ends_with(&path, &["ShipNoticeHeader", "Extrinsic"])
                            || path_ends_with(&path, &["OrderRequestHeader", "Extrinsic"])
                            || path_ends_with(&path, &["PunchOutSetupRequest", "Extrinsic"])
                        {
                            if let Some(ex_name) = attr_value(&reader, &event, "name") {
                                current_header_extrinsic = Some(format!(
                                    "header_extrinsic_{}",
                                    normalize_header(&ex_name)
                                ));
                            }
                        }
                    }
                    "Shipping" => {
                        if let Some(item) = current_item.as_mut() {
                            if path_ends_with(&path, &["ItemOut", "Shipping"])
                                && let Some(title) = attr_value(&reader, &event, "title")
                            {
                                item.insert("shipping_title".to_string(), title);
                            }
                        }
                    }
                    "Discount" => {
                        if let Some(item) = current_item.as_mut()
                            && path_ends_with(&path, &["ItemOut", "Discount"])
                            && let Some(title) = attr_value(&reader, &event, "title")
                        {
                            item.insert("discount_title".to_string(), title);
                        }
                    }
                    "Money" => {
                        if let Some(item) = current_item.as_mut() {
                            if (path_ends_with(&path, &["ItemDetail", "UnitPrice", "Money"])
                                || path_ends_with(
                                    &path,
                                    &["BlanketItemDetail", "UnitPrice", "Money"],
                                ))
                                && !item.contains_key("currency")
                                && let Some(currency) = attr_value(&reader, &event, "currency")
                            {
                                item.insert("currency".to_string(), currency);
                            }
                        }
                    }
                    "Classification" => {
                        if let Some(item) = current_item.as_mut() {
                            if (path_ends_with(&path, &["ItemDetail", "Classification"])
                                || path_ends_with(&path, &["BlanketItemDetail", "Classification"])
                                || path_ends_with(&path, &["ItemID", "Classification"])
                                || path_ends_with(&path, &["ItemOut", "Classification"]))
                                && !item.contains_key("classification_domain")
                                && let Some(domain) = attr_value(&reader, &event, "domain")
                            {
                                item.insert("classification_domain".to_string(), domain);
                            }
                        }
                    }
                    _ => {}
                }
                if include_auto && let Some(item) = current_item.as_mut() {
                    capture_cxml_auto_attrs(item, &path, &reader, &event);
                }
            }
            XmlEvent::Empty(event) => {
                let name = qname_to_string(event.name());
                path.push(name.clone());

                match name.as_str() {
                    tag if is_cxml_line_item(tag, &path) => {
                        let mut item = seed_cxml_item_row(CxmlSeed {
                            order_id: &order_id,
                            order_date: &order_date,
                            payload_id: &payload_id,
                            payload_timestamp: &payload_timestamp,
                            invoice_purpose: &invoice_purpose,
                            notice_id: &notice_id,
                            quote_id: &quote_id,
                            quote_date: &quote_date,
                            ship_to_name: &ship_to_name,
                            bill_to_name: &bill_to_name,
                            header_extrinsics: &header_extrinsics,
                        });
                        if let Some(v) = attr_value(&reader, &event, "lineNumber") {
                            item.insert("line_number".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "quantity") {
                            item.insert("quantity".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "requestedDeliveryDate") {
                            item.insert("requested_delivery_date".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "itemClassification") {
                            item.insert("item_classification".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "addressID") {
                            item.insert("address_id".to_string(), v);
                        }
                        if let Some(v) = attr_value(&reader, &event, "addressName") {
                            item.insert("address_name".to_string(), v);
                        }
                        rows.push(item);
                    }
                    "Extrinsic" => {
                        if let Some(item) = current_item.as_mut() {
                            if (path_ends_with(&path, &["ItemDetail", "Extrinsic"])
                                || path_ends_with(&path, &["BlanketItemDetail", "Extrinsic"])
                                || path_ends_with(&path, &["InvoiceDetailItem", "Extrinsic"])
                                || path_ends_with(&path, &["ShipNoticeItem", "Extrinsic"]))
                                && let Some(ex_name) = attr_value(&reader, &event, "name")
                            {
                                item.insert(
                                    format!("extrinsic_{}", normalize_header(&ex_name)),
                                    String::new(),
                                );
                            }
                        } else if (path_ends_with(&path, &["InvoiceDetailRequest", "Extrinsic"])
                            || path_ends_with(&path, &["ShipNoticeHeader", "Extrinsic"])
                            || path_ends_with(&path, &["OrderRequestHeader", "Extrinsic"])
                            || path_ends_with(&path, &["PunchOutSetupRequest", "Extrinsic"]))
                            && let Some(ex_name) = attr_value(&reader, &event, "name")
                        {
                            header_extrinsics.insert(
                                format!("header_extrinsic_{}", normalize_header(&ex_name)),
                                String::new(),
                            );
                        }
                    }
                    "Money" => {
                        if let Some(item) = current_item.as_mut() {
                            if (path_ends_with(&path, &["ItemDetail", "UnitPrice", "Money"])
                                || path_ends_with(
                                    &path,
                                    &["BlanketItemDetail", "UnitPrice", "Money"],
                                ))
                                && !item.contains_key("currency")
                                && let Some(currency) = attr_value(&reader, &event, "currency")
                            {
                                item.insert("currency".to_string(), currency);
                            }
                        }
                    }
                    _ => {}
                }
                if include_auto && let Some(item) = current_item.as_mut() {
                    capture_cxml_auto_attrs(item, &path, &reader, &event);
                }
                let _ = path.pop();
            }
            XmlEvent::Text(event) => {
                let text = event.decode()?.trim().to_string();
                if text.is_empty() {
                    buf.clear();
                    continue;
                }

                if include_auto && let Some(item) = current_item.as_mut() {
                    capture_cxml_auto_text(item, &path, &text);
                }

                if path_ends_with(&path, &["ShipTo", "Address", "Name"]) && ship_to_name.is_empty()
                {
                    ship_to_name = text.clone();
                } else if path_ends_with(&path, &["BillTo", "Address", "Name"])
                    && bill_to_name.is_empty()
                {
                    bill_to_name = text.clone();
                } else if path_ends_with(&path, &["QuoteRequestHeader", "QuoteID"]) {
                    quote_id = text.clone();
                } else if path_ends_with(&path, &["QuoteRequestHeader", "QuoteDate"]) {
                    quote_date = text.clone();
                }

                if let Some(header_key) = current_header_extrinsic.clone() {
                    header_extrinsics.insert(header_key, text.clone());
                }

                if let Some(item) = current_item.as_mut() {
                    if path_ends_with(&path, &["ItemID", "SupplierPartID"]) {
                        item.insert("supplier_part_id".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemID", "SupplierPartAuxiliaryID"]) {
                        item.insert("supplier_part_auxiliary_id".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemDetail", "Description"])
                        || path_ends_with(&path, &["BlanketItemDetail", "Description"])
                        || path_ends_with(&path, &["ItemID", "Description"])
                        || path_ends_with(&path, &["ItemOut", "Description"])
                    {
                        item.insert("description".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemDetail", "UnitPrice", "Money"])
                        || path_ends_with(&path, &["BlanketItemDetail", "UnitPrice", "Money"])
                        || path_ends_with(&path, &["QuoteOrderItem", "UnitPrice"])
                        || path_ends_with(&path, &["ShipNoticeItem", "UnitPrice"])
                        || path_ends_with(&path, &["ItemOut", "UnitPrice"])
                    {
                        item.insert("unit_price".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemDetail", "UnitOfMeasure"])
                        || path_ends_with(&path, &["BlanketItemDetail", "UnitOfMeasure"])
                        || path_ends_with(&path, &["QuoteOrderItem", "UnitOfMeasure"])
                        || path_ends_with(&path, &["ShipNoticeItem", "UnitOfMeasure"])
                        || path_ends_with(&path, &["ItemOut", "UnitOfMeasure"])
                    {
                        item.insert("unit_of_measure".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemDetail", "Classification"])
                        || path_ends_with(&path, &["BlanketItemDetail", "Classification"])
                        || path_ends_with(&path, &["ItemID", "Classification"])
                        || path_ends_with(&path, &["ItemOut", "Classification"])
                    {
                        item.insert("classification".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemDetail", "ManufacturerName"]) {
                        item.insert("manufacturer_name".to_string(), text.clone());
                    } else if (path_ends_with(&path, &["ItemDetail", "Extrinsic"])
                        || path_ends_with(&path, &["BlanketItemDetail", "Extrinsic"])
                        || path_ends_with(&path, &["InvoiceDetailItem", "Extrinsic"])
                        || path_ends_with(&path, &["ShipNoticeItem", "Extrinsic"]))
                        && let Some(ex_key) = current_item_extrinsic.clone()
                    {
                        item.insert(ex_key, text.clone());
                    } else if path_ends_with(&path, &["InvoiceDetailItem", "Quantity"]) {
                        item.insert("quantity".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ShipNoticeItem", "Quantity"])
                        || path_ends_with(&path, &["QuoteOrderItem", "Quantity"])
                    {
                        item.insert("quantity".to_string(), text.clone());
                    } else if path_ends_with(&path, &["QuoteOrderItem", "LineNumber"]) {
                        item.insert("line_number".to_string(), text.clone());
                    } else if path_ends_with(&path, &["InvoiceDetailItem", "UnitPrice"]) {
                        item.insert("unit_price".to_string(), text.clone());
                    } else if path_ends_with(&path, &["InvoiceDetailItem", "UnitOfMeasure"]) {
                        item.insert("unit_of_measure".to_string(), text.clone());
                    } else if path_ends_with(&path, &["InvoiceDetailItem", "LineTotal"]) {
                        item.insert("line_total".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ShipNoticeItem", "Shipping", "Amount"]) {
                        item.insert("shipping_amount".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ShipNoticeItem", "Shipping", "Title"]) {
                        item.insert("shipping_title".to_string(), text.clone());
                    } else if path_ends_with(&path, &["InvoiceDetailItem", "Shipping", "Amount"]) {
                        item.insert("shipping_amount".to_string(), text.clone());
                    } else if path_ends_with(&path, &["InvoiceDetailItem", "Shipping", "Title"]) {
                        item.insert("shipping_title".to_string(), text.clone());
                    } else if path_ends_with(&path, &["InvoiceDetailItem", "Comments", "Comment"]) {
                        if let Some(existing) = item.get_mut("line_comment") {
                            if !existing.is_empty() {
                                existing.push_str(" | ");
                            }
                            existing.push_str(&text);
                        } else {
                            item.insert("line_comment".to_string(), text.clone());
                        }
                    } else if path_ends_with(&path, &["ShipNoticeItem", "Comments", "Comment"])
                        || path_ends_with(&path, &["ItemOut", "Comments"])
                    {
                        if let Some(existing) = item.get_mut("line_comment") {
                            if !existing.is_empty() {
                                existing.push_str(" | ");
                            }
                            existing.push_str(&text);
                        } else {
                            item.insert("line_comment".to_string(), text.clone());
                        }
                    } else if path_ends_with(&path, &["Item", "ItemType"]) {
                        item.insert("item_type".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemOut", "Shipping"]) {
                        item.insert("shipping_amount".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemOut", "Discount"]) {
                        item.insert("discount_amount".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemOut", "Tax"]) {
                        item.insert("tax_amount".to_string(), text.clone());
                    } else if path_ends_with(&path, &["ItemOut", "Total"]) {
                        item.insert("line_total".to_string(), text.clone());
                    }
                }
            }
            XmlEvent::CData(event) => {
                let text = event.decode()?.trim().to_string();
                if include_auto
                    && let Some(item) = current_item.as_mut()
                    && !text.is_empty()
                {
                    capture_cxml_auto_text(item, &path, &text);
                }
                if let Some(header_key) = current_header_extrinsic.clone() {
                    if !text.is_empty() {
                        header_extrinsics.insert(header_key, text.clone());
                    }
                }
                if let Some(item) = current_item.as_mut() {
                    if (path_ends_with(&path, &["ItemDetail", "Description"])
                        || path_ends_with(&path, &["BlanketItemDetail", "Description"])
                        || path_ends_with(&path, &["InvoiceDetailItem", "Description"])
                        || path_ends_with(&path, &["ItemID", "Description"])
                        || path_ends_with(&path, &["ItemOut", "Description"]))
                        && !text.is_empty()
                    {
                        item.insert("description".to_string(), text);
                    }
                }
            }
            XmlEvent::End(event) => {
                let name = qname_to_string(event.name());
                if current_item_tag.as_deref() == Some(name.as_str()) {
                    if let Some(item) = current_item.take() {
                        rows.push(item);
                    }
                    current_item_tag = None;
                } else if name == "Extrinsic" {
                    current_item_extrinsic = None;
                    current_header_extrinsic = None;
                }
                let _ = path.pop();
            }
            XmlEvent::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if rows.is_empty() {
        bail!(
            "cxml has no supported line items to parse (expected ItemOut, InvoiceDetailItem, ItemIn, ShipNoticeItem, QuoteOrderItem, or Items/Item)"
        );
    }

    if !include_mapped {
        for row in &mut rows {
            row.retain(|k, _| k.starts_with("x_"));
        }
    }

    let mut all_headers: HashSet<String> = HashSet::new();
    for row in &rows {
        for key in row.keys() {
            all_headers.insert(key.clone());
        }
    }

    let preferred = [
        "order_id",
        "order_date",
        "notice_id",
        "quote_id",
        "quote_date",
        "payload_id",
        "payload_timestamp",
        "invoice_purpose",
        "line_number",
        "quantity",
        "requested_delivery_date",
        "item_classification",
        "item_type",
        "supplier_part_id",
        "supplier_part_auxiliary_id",
        "description",
        "unit_price",
        "line_total",
        "currency",
        "unit_of_measure",
        "classification",
        "classification_domain",
        "manufacturer_name",
        "shipping_amount",
        "shipping_title",
        "discount_amount",
        "discount_title",
        "tax_amount",
        "line_comment",
        "address_id",
        "address_name",
        "ship_to_name",
        "bill_to_name",
    ];

    let mut headers: Vec<String> = preferred
        .iter()
        .filter(|h| all_headers.contains(**h))
        .map(|h| h.to_string())
        .collect();

    let mut extra_headers: Vec<String> = all_headers
        .into_iter()
        .filter(|h| !headers.iter().any(|existing| existing == h))
        .collect();
    extra_headers.sort();
    headers.extend(extra_headers);

    let mut tabular_rows = Vec::with_capacity(rows.len() + 1);
    tabular_rows.push(headers.clone());
    for row in rows {
        let values = headers
            .iter()
            .map(|header| row.get(header).cloned().unwrap_or_default())
            .collect();
        tabular_rows.push(values);
    }

    Ok(tabular_rows)
}

fn capture_cxml_auto_attrs(
    item: &mut HashMap<String, String>,
    path: &[String],
    reader: &XmlReader<&[u8]>,
    start: &BytesStart<'_>,
) {
    for attr in start.attributes().with_checks(false).flatten() {
        let Ok(value) = attr.decode_and_unescape_value(reader.decoder()) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        let attr_name = String::from_utf8_lossy(attr.key.as_ref());
        let key = cxml_auto_key(path, Some(&attr_name));
        insert_or_append(item, &key, value);
    }
}

fn capture_cxml_auto_text(item: &mut HashMap<String, String>, path: &[String], text: &str) {
    let value = text.trim();
    if value.is_empty() {
        return;
    }
    let key = cxml_auto_key(path, None);
    insert_or_append(item, &key, value);
}

fn cxml_auto_key(path: &[String], attr: Option<&str>) -> String {
    let start_idx = cxml_item_path_start(path);
    let mut parts: Vec<String> = path[start_idx..]
        .iter()
        .map(|p| normalize_header(p))
        .filter(|p| !p.is_empty())
        .collect();
    if let Some(attr_name) = attr {
        let norm = normalize_header(attr_name);
        if !norm.is_empty() {
            parts.push("attr".to_string());
            parts.push(norm);
        }
    }
    if parts.is_empty() {
        return "x_value".to_string();
    }
    format!("x_{}", parts.join("_"))
}

fn cxml_item_path_start(path: &[String]) -> usize {
    path.iter()
        .position(|p| {
            matches!(
                p.as_str(),
                "ItemOut"
                    | "InvoiceDetailItem"
                    | "ItemIn"
                    | "ShipNoticeItem"
                    | "QuoteOrderItem"
                    | "Item"
            )
        })
        .unwrap_or(0)
}

fn insert_or_append(item: &mut HashMap<String, String>, key: &str, value: &str) {
    match item.get_mut(key) {
        Some(existing) => {
            if existing == value {
                return;
            }
            if !existing.is_empty() {
                existing.push_str(" | ");
            }
            existing.push_str(value);
        }
        None => {
            item.insert(key.to_string(), value.to_string());
        }
    }
}

fn qname_to_string(name: QName<'_>) -> String {
    String::from_utf8_lossy(name.as_ref()).to_string()
}

fn attr_value(reader: &XmlReader<&[u8]>, start: &BytesStart<'_>, key: &str) -> Option<String> {
    for attr in start.attributes().with_checks(false).flatten() {
        if attr.key.as_ref() == key.as_bytes()
            && let Ok(value) = attr.decode_and_unescape_value(reader.decoder())
        {
            return Some(value.into_owned());
        }
    }
    None
}

fn path_ends_with(path: &[String], suffix: &[&str]) -> bool {
    if path.len() < suffix.len() {
        return false;
    }
    path[path.len() - suffix.len()..]
        .iter()
        .map(String::as_str)
        .eq(suffix.iter().copied())
}

fn is_cxml_line_item(tag: &str, path: &[String]) -> bool {
    match tag {
        "ItemOut" | "InvoiceDetailItem" | "ItemIn" | "ShipNoticeItem" | "QuoteOrderItem" => true,
        "Item" => path_ends_with(path, &["Items", "Item"]),
        _ => false,
    }
}

struct CxmlSeed<'a> {
    order_id: &'a str,
    order_date: &'a str,
    payload_id: &'a str,
    payload_timestamp: &'a str,
    invoice_purpose: &'a str,
    notice_id: &'a str,
    quote_id: &'a str,
    quote_date: &'a str,
    ship_to_name: &'a str,
    bill_to_name: &'a str,
    header_extrinsics: &'a HashMap<String, String>,
}

fn seed_cxml_item_row(seed: CxmlSeed<'_>) -> HashMap<String, String> {
    let mut item = HashMap::new();
    if !seed.order_id.is_empty() {
        item.insert("order_id".to_string(), seed.order_id.to_string());
    }
    if !seed.order_date.is_empty() {
        item.insert("order_date".to_string(), seed.order_date.to_string());
    }
    if !seed.notice_id.is_empty() {
        item.insert("notice_id".to_string(), seed.notice_id.to_string());
    }
    if !seed.quote_id.is_empty() {
        item.insert("quote_id".to_string(), seed.quote_id.to_string());
    }
    if !seed.quote_date.is_empty() {
        item.insert("quote_date".to_string(), seed.quote_date.to_string());
    }
    if !seed.payload_id.is_empty() {
        item.insert("payload_id".to_string(), seed.payload_id.to_string());
    }
    if !seed.payload_timestamp.is_empty() {
        item.insert(
            "payload_timestamp".to_string(),
            seed.payload_timestamp.to_string(),
        );
    }
    if !seed.invoice_purpose.is_empty() {
        item.insert(
            "invoice_purpose".to_string(),
            seed.invoice_purpose.to_string(),
        );
    }
    if !seed.ship_to_name.is_empty() {
        item.insert("ship_to_name".to_string(), seed.ship_to_name.to_string());
    }
    if !seed.bill_to_name.is_empty() {
        item.insert("bill_to_name".to_string(), seed.bill_to_name.to_string());
    }
    for (k, v) in seed.header_extrinsics {
        item.insert(k.clone(), v.clone());
    }
    item
}

fn parse_naaccr_content(content: &str) -> Result<Vec<Vec<String>>> {
    let mut reader = XmlReader::from_str(content);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();

    let mut global_fields: HashMap<String, String> = HashMap::new();
    let mut patient_fields: HashMap<String, String> = HashMap::new();
    let mut tumor_fields: HashMap<String, String> = HashMap::new();
    let mut rows: Vec<HashMap<String, String>> = Vec::new();

    let mut in_patient = false;
    let mut in_tumor = false;
    let mut patient_has_tumor = false;
    let mut current_item_key: Option<(bool, String)> = None; // (is_tumor, key)

    loop {
        match reader.read_event_into(&mut buf)? {
            XmlEvent::Start(event) => {
                let name = qname_to_string(event.name());
                path.push(name.clone());
                match name.as_str() {
                    "NaaccrData" => {
                        for attr in event.attributes().with_checks(false).flatten() {
                            if let Ok(value) = attr.decode_and_unescape_value(reader.decoder()) {
                                let key = format!(
                                    "naaccr_{}",
                                    normalize_header(&String::from_utf8_lossy(attr.key.as_ref()))
                                );
                                global_fields.insert(key, value.into_owned());
                            }
                        }
                    }
                    "Patient" => {
                        in_patient = true;
                        in_tumor = false;
                        patient_has_tumor = false;
                        patient_fields.clear();
                    }
                    "Tumor" => {
                        in_tumor = true;
                        patient_has_tumor = true;
                        tumor_fields.clear();
                    }
                    "Item" => {
                        if let Some(naaccr_id) = attr_value(&reader, &event, "naaccrId") {
                            let key = if in_tumor {
                                format!("tumor_{}", normalize_header(&naaccr_id))
                            } else {
                                format!("patient_{}", normalize_header(&naaccr_id))
                            };
                            current_item_key = Some((in_tumor, key));
                        }
                    }
                    _ => {}
                }
            }
            XmlEvent::Empty(event) => {
                let name = qname_to_string(event.name());
                path.push(name.clone());
                if name == "Item"
                    && let Some(naaccr_id) = attr_value(&reader, &event, "naaccrId")
                {
                    let key = if in_tumor {
                        format!("tumor_{}", normalize_header(&naaccr_id))
                    } else {
                        format!("patient_{}", normalize_header(&naaccr_id))
                    };
                    if in_tumor {
                        tumor_fields.entry(key).or_default();
                    } else if in_patient {
                        patient_fields.entry(key).or_default();
                    }
                }
                let _ = path.pop();
            }
            XmlEvent::Text(event) => {
                let text = event.decode()?.trim().to_string();
                if text.is_empty() {
                    buf.clear();
                    continue;
                }

                if let Some((is_tumor, key)) = current_item_key.clone() {
                    if is_tumor {
                        tumor_fields.insert(key, text);
                    } else if in_patient {
                        patient_fields.insert(key, text);
                    }
                }
            }
            XmlEvent::CData(event) => {
                let text = event.decode()?.trim().to_string();
                if text.is_empty() {
                    buf.clear();
                    continue;
                }

                if let Some((is_tumor, key)) = current_item_key.clone() {
                    if is_tumor {
                        tumor_fields.insert(key, text);
                    } else if in_patient {
                        patient_fields.insert(key, text);
                    }
                }
            }
            XmlEvent::End(event) => {
                let name = qname_to_string(event.name());
                match name.as_str() {
                    "Item" => {
                        current_item_key = None;
                    }
                    "Tumor" => {
                        let mut row = global_fields.clone();
                        for (k, v) in &patient_fields {
                            row.insert(k.clone(), v.clone());
                        }
                        for (k, v) in &tumor_fields {
                            row.insert(k.clone(), v.clone());
                        }
                        rows.push(row);
                        tumor_fields.clear();
                        in_tumor = false;
                    }
                    "Patient" => {
                        if !patient_has_tumor && !patient_fields.is_empty() {
                            let mut row = global_fields.clone();
                            for (k, v) in &patient_fields {
                                row.insert(k.clone(), v.clone());
                            }
                            rows.push(row);
                        }
                        patient_fields.clear();
                        in_patient = false;
                    }
                    _ => {}
                }
                let _ = path.pop();
            }
            XmlEvent::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if rows.is_empty() {
        bail!("naaccr xml has no patient/tumor records");
    }

    let mut all_headers: HashSet<String> = HashSet::new();
    for row in &rows {
        for key in row.keys() {
            all_headers.insert(key.clone());
        }
    }

    let preferred = [
        "naaccr_recordtype",
        "naaccr_specificationversion",
        "naaccr_timegenerated",
        "patient_patientidnumber",
        "patient_dateofbirth",
        "patient_sex",
        "patient_race1",
        "patient_dateoflastcontact",
        "tumor_tumorrecordnumber",
        "tumor_primarysite",
        "tumor_histologictypeicdo3",
        "tumor_behaviorcodeicdo3",
        "tumor_laterality",
        "tumor_ageatdiagnosis",
        "tumor_dateofdiagnosis",
    ];
    let mut headers: Vec<String> = preferred
        .iter()
        .filter(|h| all_headers.contains(**h))
        .map(|h| h.to_string())
        .collect();
    let mut extra_headers: Vec<String> = all_headers
        .into_iter()
        .filter(|h| !headers.iter().any(|existing| existing == h))
        .collect();
    extra_headers.sort();
    headers.extend(extra_headers);

    let mut tabular_rows = Vec::with_capacity(rows.len() + 1);
    tabular_rows.push(headers.clone());
    for row in rows {
        let values = headers
            .iter()
            .map(|header| row.get(header).cloned().unwrap_or_default())
            .collect();
        tabular_rows.push(values);
    }

    Ok(tabular_rows)
}

fn parse_json_content(content: &str) -> Result<Vec<Vec<String>>> {
    let docs = parse_json_documents(content)?;
    if docs.is_empty() {
        bail!("json has no records");
    }

    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    for (idx, doc) in docs.iter().enumerate() {
        let mut row = HashMap::new();
        if docs.len() > 1 {
            row.insert("record_index".to_string(), (idx + 1).to_string());
        }
        flatten_json_value(doc, None, &mut row);
        if row.is_empty() {
            row.insert(
                "value".to_string(),
                json_scalar_to_string(doc).unwrap_or_default(),
            );
        }
        rows.push(row);
    }

    map_rows_to_tabular(rows, &["record_index"], "json has no records")
}

fn parse_fhir_content(content: &str) -> Result<Vec<Vec<String>>> {
    let trimmed = content.trim_start();
    if looks_turtle_content(trimmed) {
        return parse_rdf_content(content);
    }
    if looks_html_content(trimmed) {
        return parse_html_content(content);
    }

    let docs = parse_json_documents(content)?;
    let mut resources: Vec<JsonValue> = Vec::new();
    for doc in &docs {
        collect_fhir_resources(doc, &mut resources);
    }

    if resources.is_empty() {
        bail!("fhir has no resources to parse");
    }

    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    for resource in &resources {
        let JsonValue::Object(obj) = resource else {
            continue;
        };

        let mut row = HashMap::new();
        insert_if_non_empty(
            &mut row,
            "resource_type",
            json_path_str(resource, &["resourceType"]),
        );
        insert_if_non_empty(&mut row, "resource_id", json_path_str(resource, &["id"]));
        insert_if_non_empty(&mut row, "status", json_path_str(resource, &["status"]));
        insert_if_non_empty(&mut row, "intent", json_path_str(resource, &["intent"]));
        insert_if_non_empty(
            &mut row,
            "subject_reference",
            json_path_str(resource, &["subject", "reference"]),
        );
        insert_if_non_empty(
            &mut row,
            "patient_reference",
            json_path_str(resource, &["patient", "reference"]),
        );
        insert_if_non_empty(
            &mut row,
            "encounter_reference",
            json_path_str(resource, &["encounter", "reference"]),
        );
        insert_if_non_empty(
            &mut row,
            "authored_on",
            json_path_str(resource, &["authoredOn"]),
        );
        insert_if_non_empty(
            &mut row,
            "effective_datetime",
            json_path_str(resource, &["effectiveDateTime"])
                .or_else(|| json_path_str(resource, &["effectiveInstant"]))
                .or_else(|| json_path_str(resource, &["effectivePeriod", "start"])),
        );
        insert_if_non_empty(
            &mut row,
            "performed_datetime",
            json_path_str(resource, &["performedDateTime"])
                .or_else(|| json_path_str(resource, &["performedPeriod", "start"])),
        );
        insert_if_non_empty(
            &mut row,
            "onset_datetime",
            json_path_str(resource, &["onsetDateTime"]),
        );
        insert_if_non_empty(
            &mut row,
            "recorded_date",
            json_path_str(resource, &["recordedDate"]),
        );
        insert_if_non_empty(&mut row, "issued", json_path_str(resource, &["issued"]));
        insert_if_non_empty(
            &mut row,
            "value_quantity_value",
            json_path_str(resource, &["valueQuantity", "value"]),
        );
        insert_if_non_empty(
            &mut row,
            "value_quantity_unit",
            json_path_str(resource, &["valueQuantity", "unit"]),
        );
        insert_if_non_empty(
            &mut row,
            "value_string",
            json_path_str(resource, &["valueString"]),
        );
        insert_if_non_empty(
            &mut row,
            "value_boolean",
            json_path_str(resource, &["valueBoolean"]),
        );
        insert_if_non_empty(
            &mut row,
            "value_integer",
            json_path_str(resource, &["valueInteger"]),
        );

        let code_obj = obj.get("code");
        insert_if_non_empty(
            &mut row,
            "code_code",
            fhir_first_coding_field(code_obj, "code"),
        );
        insert_if_non_empty(
            &mut row,
            "code_display",
            fhir_first_coding_field(code_obj, "display")
                .or_else(|| code_obj.and_then(|v| json_path_str(v, &["text"]))),
        );
        insert_if_non_empty(
            &mut row,
            "code_system",
            fhir_first_coding_field(code_obj, "system"),
        );

        let value_codeable = obj.get("valueCodeableConcept");
        insert_if_non_empty(
            &mut row,
            "value_code",
            fhir_first_coding_field(value_codeable, "code"),
        );
        insert_if_non_empty(
            &mut row,
            "value_display",
            fhir_first_coding_field(value_codeable, "display")
                .or_else(|| value_codeable.and_then(|v| json_path_str(v, &["text"]))),
        );

        if json_path_str(resource, &["resourceType"]).as_deref() == Some("Patient") {
            insert_if_non_empty(&mut row, "gender", json_path_str(resource, &["gender"]));
            insert_if_non_empty(
                &mut row,
                "birth_date",
                json_path_str(resource, &["birthDate"]),
            );
            if let Some(name0) = json_path(resource, &["name"]).and_then(|v| v.as_array())
                && let Some(first_name) = name0.first()
            {
                insert_if_non_empty(
                    &mut row,
                    "family_name",
                    json_path_str(first_name, &["family"]),
                );
                insert_if_non_empty(
                    &mut row,
                    "given_name",
                    first_name
                        .get("given")
                        .and_then(|v| v.as_array())
                        .map(|parts| {
                            parts
                                .iter()
                                .filter_map(json_scalar_to_string)
                                .collect::<Vec<String>>()
                                .join(" ")
                        }),
                );
            }
        }

        rows.push(row);
    }

    let preferred = [
        "resource_type",
        "resource_id",
        "status",
        "intent",
        "subject_reference",
        "patient_reference",
        "encounter_reference",
        "effective_datetime",
        "performed_datetime",
        "onset_datetime",
        "recorded_date",
        "issued",
        "authored_on",
        "code_system",
        "code_code",
        "code_display",
        "value_quantity_value",
        "value_quantity_unit",
        "value_code",
        "value_display",
        "value_string",
        "value_boolean",
        "value_integer",
        "gender",
        "birth_date",
        "family_name",
        "given_name",
    ];

    map_rows_to_tabular(rows, &preferred, "fhir has no resources to parse")
}

fn parse_hl7_content(content: &str) -> Result<Vec<Vec<String>>> {
    let normalized = content.replace("\r\n", "\n").replace('\r', "\n");
    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    let mut context: HashMap<String, String> = HashMap::new();
    let mut has_message = false;
    let mut emitted_for_message = false;
    let mut field_sep = '|';

    for raw_line in normalized.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.len() < 3 {
            continue;
        }

        let seg = &line[..3];
        if seg == "MSH" {
            if has_message && !emitted_for_message && !context.is_empty() {
                rows.push(context.clone());
            }

            has_message = true;
            emitted_for_message = false;
            context.clear();

            field_sep = line.chars().nth(3).unwrap_or('|');
            let fields: Vec<&str> = line.split(field_sep).collect();

            context.insert("segment_type".to_string(), "HL7".to_string());
            context.insert("field_separator".to_string(), field_sep.to_string());
            insert_if_non_empty(
                &mut context,
                "encoding_characters",
                Some(hl7_msh_field(&fields, 2).to_string()),
            );
            insert_if_non_empty(
                &mut context,
                "sending_application",
                Some(hl7_msh_field(&fields, 3).to_string()),
            );
            insert_if_non_empty(
                &mut context,
                "sending_facility",
                Some(hl7_msh_field(&fields, 4).to_string()),
            );
            insert_if_non_empty(
                &mut context,
                "receiving_application",
                Some(hl7_msh_field(&fields, 5).to_string()),
            );
            insert_if_non_empty(
                &mut context,
                "receiving_facility",
                Some(hl7_msh_field(&fields, 6).to_string()),
            );
            insert_if_non_empty(
                &mut context,
                "message_datetime",
                Some(hl7_msh_field(&fields, 7).to_string()),
            );
            let message_type = hl7_msh_field(&fields, 9);
            insert_if_non_empty(
                &mut context,
                "message_type",
                Some(hl7_component(message_type, 1)),
            );
            insert_if_non_empty(
                &mut context,
                "trigger_event",
                Some(hl7_component(message_type, 2)),
            );
            insert_if_non_empty(
                &mut context,
                "message_control_id",
                Some(hl7_msh_field(&fields, 10).to_string()),
            );
            insert_if_non_empty(
                &mut context,
                "processing_id",
                Some(hl7_msh_field(&fields, 11).to_string()),
            );
            insert_if_non_empty(
                &mut context,
                "hl7_version",
                Some(hl7_msh_field(&fields, 12).to_string()),
            );
            continue;
        }

        if !has_message {
            continue;
        }

        let fields: Vec<&str> = line.split(field_sep).collect();
        match seg {
            "PID" => {
                insert_if_non_empty(
                    &mut context,
                    "patient_id",
                    Some(hl7_field(&fields, 3).to_string()),
                );
                let patient_name = hl7_field(&fields, 5);
                insert_if_non_empty(
                    &mut context,
                    "patient_family_name",
                    Some(hl7_component(patient_name, 1)),
                );
                insert_if_non_empty(
                    &mut context,
                    "patient_given_name",
                    Some(hl7_component(patient_name, 2)),
                );
                insert_if_non_empty(
                    &mut context,
                    "patient_birth_date",
                    Some(hl7_field(&fields, 7).to_string()),
                );
                insert_if_non_empty(
                    &mut context,
                    "patient_sex",
                    Some(hl7_field(&fields, 8).to_string()),
                );
            }
            "PV1" => {
                insert_if_non_empty(
                    &mut context,
                    "patient_class",
                    Some(hl7_field(&fields, 2).to_string()),
                );
                insert_if_non_empty(
                    &mut context,
                    "visit_number",
                    Some(hl7_component(hl7_field(&fields, 19), 1)),
                );
            }
            "ORC" => {
                insert_if_non_empty(
                    &mut context,
                    "order_control",
                    Some(hl7_field(&fields, 1).to_string()),
                );
                insert_if_non_empty(
                    &mut context,
                    "placer_order_number",
                    Some(hl7_component(hl7_field(&fields, 2), 1)),
                );
                insert_if_non_empty(
                    &mut context,
                    "filler_order_number",
                    Some(hl7_component(hl7_field(&fields, 3), 1)),
                );
            }
            "OBR" => {
                insert_if_non_empty(
                    &mut context,
                    "order_set_id",
                    Some(hl7_field(&fields, 1).to_string()),
                );
                let universal_service = hl7_field(&fields, 4);
                insert_if_non_empty(
                    &mut context,
                    "universal_service_code",
                    Some(hl7_component(universal_service, 1)),
                );
                insert_if_non_empty(
                    &mut context,
                    "universal_service_text",
                    Some(hl7_component(universal_service, 2)),
                );
                insert_if_non_empty(
                    &mut context,
                    "observation_request_datetime",
                    Some(hl7_field(&fields, 7).to_string()),
                );
            }
            "OBX" => {
                let mut row = context.clone();
                insert_if_non_empty(
                    &mut row,
                    "obx_set_id",
                    Some(hl7_field(&fields, 1).to_string()),
                );
                insert_if_non_empty(
                    &mut row,
                    "obx_value_type",
                    Some(hl7_field(&fields, 2).to_string()),
                );
                let obx3 = hl7_field(&fields, 3);
                insert_if_non_empty(&mut row, "obx_code", Some(hl7_component(obx3, 1)));
                insert_if_non_empty(&mut row, "obx_text", Some(hl7_component(obx3, 2)));
                insert_if_non_empty(
                    &mut row,
                    "obx_value",
                    Some(hl7_field(&fields, 5).to_string()),
                );
                insert_if_non_empty(
                    &mut row,
                    "obx_units",
                    Some(hl7_field(&fields, 6).to_string()),
                );
                insert_if_non_empty(
                    &mut row,
                    "obx_reference_range",
                    Some(hl7_field(&fields, 7).to_string()),
                );
                insert_if_non_empty(
                    &mut row,
                    "obx_abnormal_flags",
                    Some(hl7_field(&fields, 8).to_string()),
                );
                insert_if_non_empty(
                    &mut row,
                    "obx_result_status",
                    Some(hl7_field(&fields, 11).to_string()),
                );
                insert_if_non_empty(
                    &mut row,
                    "obx_observation_datetime",
                    Some(hl7_field(&fields, 14).to_string()),
                );
                rows.push(row);
                emitted_for_message = true;
            }
            _ => {}
        }
    }

    if has_message && !emitted_for_message && !context.is_empty() {
        rows.push(context);
    }

    let preferred = [
        "segment_type",
        "message_control_id",
        "message_type",
        "trigger_event",
        "hl7_version",
        "message_datetime",
        "sending_application",
        "sending_facility",
        "receiving_application",
        "receiving_facility",
        "patient_id",
        "patient_family_name",
        "patient_given_name",
        "patient_birth_date",
        "patient_sex",
        "visit_number",
        "order_control",
        "placer_order_number",
        "filler_order_number",
        "order_set_id",
        "universal_service_code",
        "universal_service_text",
        "observation_request_datetime",
        "obx_set_id",
        "obx_value_type",
        "obx_code",
        "obx_text",
        "obx_value",
        "obx_units",
        "obx_reference_range",
        "obx_abnormal_flags",
        "obx_result_status",
        "obx_observation_datetime",
    ];

    map_rows_to_tabular(rows, &preferred, "hl7 has no supported segments to parse")
}

fn parse_cda_content(content: &str) -> Result<Vec<Vec<String>>> {
    let mut reader = XmlReader::from_str(content);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();

    let mut doc_fields: HashMap<String, String> = HashMap::new();
    let mut patient_fields: HashMap<String, String> = HashMap::new();
    let mut patient_given_parts: Vec<String> = Vec::new();
    let mut patient_family = String::new();
    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    let mut current_obs: Option<HashMap<String, String>> = None;

    loop {
        match reader.read_event_into(&mut buf)? {
            XmlEvent::Start(event) => {
                let name = qname_local_lower(event.name());
                path.push(name.clone());

                match name.as_str() {
                    "observation" => {
                        let mut row = doc_fields.clone();
                        for (k, v) in &patient_fields {
                            row.insert(k.clone(), v.clone());
                        }
                        let patient_name =
                            build_patient_name(&patient_family, &patient_given_parts);
                        if !patient_name.is_empty() {
                            row.insert("patient_name".to_string(), patient_name);
                        }
                        current_obs = Some(row);
                    }
                    "id" => {
                        let root = attr_value(&reader, &event, "root");
                        let extension = attr_value(&reader, &event, "extension");
                        let id_value = combine_id_parts(root, extension);

                        if path_ends_with(&path, &["clinicaldocument", "id"]) {
                            if let Some(v) = id_value {
                                doc_fields.insert("document_id".to_string(), v);
                            }
                        } else if path_ends_with(&path, &["recordtarget", "patientrole", "id"]) {
                            if let Some(v) = id_value {
                                patient_fields.insert("patient_id".to_string(), v);
                            }
                        } else if path_ends_with(
                            &path,
                            &["componentof", "encompassingencounter", "id"],
                        ) {
                            if let Some(v) = id_value {
                                doc_fields.insert("encounter_id".to_string(), v);
                            }
                        } else if path_ends_with(&path, &["observation", "id"])
                            && let Some(obs) = current_obs.as_mut()
                            && let Some(v) = id_value
                        {
                            obs.insert("observation_id".to_string(), v);
                        }
                    }
                    "effectivetime" => {
                        if path_ends_with(&path, &["clinicaldocument", "effectivetime"]) {
                            if let Some(v) = attr_value(&reader, &event, "value") {
                                doc_fields.insert("document_effective_time".to_string(), v);
                            }
                        } else if path_ends_with(&path, &["observation", "effectivetime"])
                            && let Some(obs) = current_obs.as_mut()
                        {
                            if let Some(v) = attr_value(&reader, &event, "value")
                                .or_else(|| attr_value(&reader, &event, "nullFlavor"))
                            {
                                obs.insert("observation_effective_time".to_string(), v);
                            }
                        }
                    }
                    "administrativegendercode" => {
                        if path_ends_with(
                            &path,
                            &[
                                "recordtarget",
                                "patientrole",
                                "patient",
                                "administrativegendercode",
                            ],
                        ) && let Some(v) = attr_value(&reader, &event, "code")
                        {
                            patient_fields.insert("patient_gender_code".to_string(), v);
                        }
                    }
                    "birthtime" => {
                        if path_ends_with(
                            &path,
                            &["recordtarget", "patientrole", "patient", "birthtime"],
                        ) && let Some(v) = attr_value(&reader, &event, "value")
                        {
                            patient_fields.insert("patient_birth_time".to_string(), v);
                        }
                    }
                    "code" => {
                        if path_ends_with(&path, &["observation", "code"])
                            && let Some(obs) = current_obs.as_mut()
                        {
                            if let Some(v) = attr_value(&reader, &event, "code") {
                                obs.insert("observation_code".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "displayName") {
                                obs.insert("observation_display".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "codeSystem") {
                                obs.insert("observation_code_system".to_string(), v);
                            }
                        }
                    }
                    "value" => {
                        if path_ends_with(&path, &["observation", "value"])
                            && let Some(obs) = current_obs.as_mut()
                        {
                            if let Some(v) = attr_value(&reader, &event, "value") {
                                obs.insert("observation_value".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "unit") {
                                obs.insert("observation_unit".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "code") {
                                obs.insert("observation_value_code".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "displayName") {
                                obs.insert("observation_value_display".to_string(), v);
                            }
                        }
                    }
                    _ => {}
                }
            }
            XmlEvent::Empty(event) => {
                let name = qname_local_lower(event.name());
                path.push(name.clone());
                match name.as_str() {
                    "id" => {
                        let root = attr_value(&reader, &event, "root");
                        let extension = attr_value(&reader, &event, "extension");
                        let id_value = combine_id_parts(root, extension);
                        if path_ends_with(&path, &["clinicaldocument", "id"]) {
                            if let Some(v) = id_value {
                                doc_fields.insert("document_id".to_string(), v);
                            }
                        } else if path_ends_with(&path, &["recordtarget", "patientrole", "id"]) {
                            if let Some(v) = id_value {
                                patient_fields.insert("patient_id".to_string(), v);
                            }
                        } else if path_ends_with(
                            &path,
                            &["componentof", "encompassingencounter", "id"],
                        ) {
                            if let Some(v) = id_value {
                                doc_fields.insert("encounter_id".to_string(), v);
                            }
                        } else if path_ends_with(&path, &["observation", "id"])
                            && let Some(obs) = current_obs.as_mut()
                            && let Some(v) = id_value
                        {
                            obs.insert("observation_id".to_string(), v);
                        }
                    }
                    "effectivetime" => {
                        if path_ends_with(&path, &["clinicaldocument", "effectivetime"]) {
                            if let Some(v) = attr_value(&reader, &event, "value") {
                                doc_fields.insert("document_effective_time".to_string(), v);
                            }
                        } else if path_ends_with(&path, &["observation", "effectivetime"])
                            && let Some(obs) = current_obs.as_mut()
                            && let Some(v) = attr_value(&reader, &event, "value")
                        {
                            obs.insert("observation_effective_time".to_string(), v);
                        }
                    }
                    "administrativegendercode" => {
                        if path_ends_with(
                            &path,
                            &[
                                "recordtarget",
                                "patientrole",
                                "patient",
                                "administrativegendercode",
                            ],
                        ) && let Some(v) = attr_value(&reader, &event, "code")
                        {
                            patient_fields.insert("patient_gender_code".to_string(), v);
                        }
                    }
                    "birthtime" => {
                        if path_ends_with(
                            &path,
                            &["recordtarget", "patientrole", "patient", "birthtime"],
                        ) && let Some(v) = attr_value(&reader, &event, "value")
                        {
                            patient_fields.insert("patient_birth_time".to_string(), v);
                        }
                    }
                    "code" => {
                        if path_ends_with(&path, &["observation", "code"])
                            && let Some(obs) = current_obs.as_mut()
                        {
                            if let Some(v) = attr_value(&reader, &event, "code") {
                                obs.insert("observation_code".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "displayName") {
                                obs.insert("observation_display".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "codeSystem") {
                                obs.insert("observation_code_system".to_string(), v);
                            }
                        }
                    }
                    "value" => {
                        if path_ends_with(&path, &["observation", "value"])
                            && let Some(obs) = current_obs.as_mut()
                        {
                            if let Some(v) = attr_value(&reader, &event, "value") {
                                obs.insert("observation_value".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "unit") {
                                obs.insert("observation_unit".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "code") {
                                obs.insert("observation_value_code".to_string(), v);
                            }
                            if let Some(v) = attr_value(&reader, &event, "displayName") {
                                obs.insert("observation_value_display".to_string(), v);
                            }
                        }
                    }
                    _ => {}
                }
                let _ = path.pop();
            }
            XmlEvent::Text(event) => {
                let text = event.decode()?.trim().to_string();
                if text.is_empty() {
                    buf.clear();
                    continue;
                }

                if path_ends_with(
                    &path,
                    &["recordtarget", "patientrole", "patient", "name", "given"],
                ) {
                    patient_given_parts.push(text.clone());
                } else if path_ends_with(
                    &path,
                    &["recordtarget", "patientrole", "patient", "name", "family"],
                ) {
                    patient_family = text.clone();
                }

                if let Some(obs) = current_obs.as_mut() {
                    if path_ends_with(&path, &["observation", "value"]) {
                        obs.entry("observation_value".to_string())
                            .or_insert_with(|| text.clone());
                    } else if path_ends_with(&path, &["observation", "text"]) {
                        obs.entry("observation_text".to_string())
                            .and_modify(|v| {
                                if !v.is_empty() {
                                    v.push_str(" | ");
                                }
                                v.push_str(&text);
                            })
                            .or_insert(text.clone());
                    }
                }
            }
            XmlEvent::End(event) => {
                let name = qname_local_lower(event.name());
                if name == "name"
                    && path_ends_with(&path, &["recordtarget", "patientrole", "patient", "name"])
                {
                    let patient_name = build_patient_name(&patient_family, &patient_given_parts);
                    if !patient_name.is_empty() {
                        patient_fields.insert("patient_name".to_string(), patient_name);
                    }
                } else if name == "observation"
                    && let Some(obs) = current_obs.take()
                {
                    rows.push(obs);
                }
                let _ = path.pop();
            }
            XmlEvent::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if rows.is_empty() && (!doc_fields.is_empty() || !patient_fields.is_empty()) {
        let mut row = doc_fields.clone();
        for (k, v) in patient_fields {
            row.insert(k, v);
        }
        rows.push(row);
    }

    let preferred = [
        "document_id",
        "document_effective_time",
        "encounter_id",
        "patient_id",
        "patient_name",
        "patient_gender_code",
        "patient_birth_time",
        "observation_id",
        "observation_code_system",
        "observation_code",
        "observation_display",
        "observation_effective_time",
        "observation_value",
        "observation_unit",
        "observation_value_code",
        "observation_value_display",
        "observation_text",
    ];

    map_rows_to_tabular(rows, &preferred, "cda has no supported records to parse")
}

fn parse_rdf_content(content: &str) -> Result<Vec<Vec<String>>> {
    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    let cursor = Cursor::new(content.as_bytes());
    let mut parser = TurtleParser::new(cursor, None);
    parser.parse_all(&mut |triple| -> std::result::Result<(), anyhow::Error> {
        let mut row = HashMap::new();
        row.insert(
            "subject".to_string(),
            rio_subject_to_string(&triple.subject),
        );
        row.insert("predicate".to_string(), triple.predicate.iri.to_string());
        let (object, object_kind) = rio_term_to_string_kind(&triple.object);
        row.insert("object".to_string(), object);
        row.insert("object_kind".to_string(), object_kind);
        rows.push(row);
        Ok(())
    })?;

    map_rows_to_tabular(
        rows,
        &["subject", "predicate", "object", "object_kind"],
        "rdf/turtle has no triples to parse",
    )
}

fn parse_html_content(content: &str) -> Result<Vec<Vec<String>>> {
    let block = extract_pre_block(content).context(
        "html has no parseable <pre> block for rdf/turtle; use raw .ttl/.rdf when possible",
    )?;
    let decoded = decode_html_entities(&block);
    if looks_turtle_content(decoded.trim_start()) {
        parse_rdf_content(&decoded)
    } else {
        bail!("html pre block is not rdf/turtle content")
    }
}

fn extract_pre_block(content: &str) -> Option<String> {
    let mut index = 0usize;
    let mut first_block: Option<String> = None;
    while let Some(rel_start) = content[index..].find("<pre") {
        let start = index + rel_start;
        let open_end_rel = content[start..].find('>')?;
        let open_end = start + open_end_rel;
        let close_rel = content[open_end + 1..].find("</pre>")?;
        let close = open_end + 1 + close_rel;
        let opening_tag = &content[start..=open_end];
        let block = content[open_end + 1..close].to_string();
        if first_block.is_none() {
            first_block = Some(block.clone());
        }
        if opening_tag.to_ascii_lowercase().contains("rdf") {
            return Some(block);
        }
        index = close + "</pre>".len();
    }
    first_block
}

fn decode_html_entities(input: &str) -> String {
    input
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

fn rio_subject_to_string(subject: &RioSubject<'_>) -> String {
    match subject {
        RioSubject::NamedNode(node) => node.iri.to_string(),
        RioSubject::BlankNode(node) => format!("_:{}", node.id),
        _ => "_:triple_subject".to_string(),
    }
}

fn rio_term_to_string_kind(term: &RioTerm<'_>) -> (String, String) {
    match term {
        RioTerm::NamedNode(node) => (node.iri.to_string(), "iri".to_string()),
        RioTerm::BlankNode(node) => (format!("_:{}", node.id), "blank_node".to_string()),
        RioTerm::Literal(literal) => match literal {
            RioLiteral::Simple { value } => (value.to_string(), "literal".to_string()),
            RioLiteral::LanguageTaggedString { value, language } => {
                (format!("{value}@{language}"), "literal_lang".to_string())
            }
            RioLiteral::Typed { value, datatype } => {
                (value.to_string(), format!("literal_typed:{}", datatype.iri))
            }
        },
        RioTerm::Triple(_) => ("_:embedded_triple".to_string(), "triple".to_string()),
    }
}

fn map_rows_to_tabular(
    rows: Vec<HashMap<String, String>>,
    preferred: &[&str],
    empty_msg: &str,
) -> Result<Vec<Vec<String>>> {
    if rows.is_empty() {
        bail!("{empty_msg}");
    }

    let mut all_headers: HashSet<String> = HashSet::new();
    for row in &rows {
        for key in row.keys() {
            all_headers.insert(key.clone());
        }
    }

    let mut headers: Vec<String> = preferred
        .iter()
        .filter(|h| all_headers.contains(**h))
        .map(|h| h.to_string())
        .collect();
    let mut extra_headers: Vec<String> = all_headers
        .into_iter()
        .filter(|h| !headers.iter().any(|existing| existing == h))
        .collect();
    extra_headers.sort();
    headers.extend(extra_headers);

    let mut tabular_rows = Vec::with_capacity(rows.len() + 1);
    tabular_rows.push(headers.clone());
    for row in rows {
        let values = headers
            .iter()
            .map(|header| row.get(header).cloned().unwrap_or_default())
            .collect();
        tabular_rows.push(values);
    }

    Ok(tabular_rows)
}

fn parse_json_documents(content: &str) -> Result<Vec<JsonValue>> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        bail!("json input is empty");
    }

    if let Ok(value) = serde_json::from_str::<JsonValue>(trimmed) {
        return Ok(match value {
            JsonValue::Array(items) => items,
            other => vec![other],
        });
    }

    let mut docs = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: JsonValue = serde_json::from_str(line)
            .with_context(|| format!("invalid ndjson at line {}", idx + 1))?;
        docs.push(value);
    }

    if docs.is_empty() {
        bail!("json has no parseable records");
    }

    Ok(docs)
}

fn flatten_json_value(value: &JsonValue, prefix: Option<&str>, out: &mut HashMap<String, String>) {
    match value {
        JsonValue::Object(obj) => {
            for (key, child) in obj {
                let child_prefix = match prefix {
                    Some(p) => format!("{p}.{key}"),
                    None => key.to_string(),
                };
                flatten_json_value(child, Some(&child_prefix), out);
            }
        }
        JsonValue::Array(items) => {
            if let Some(p) = prefix {
                if items.iter().all(|v| {
                    matches!(
                        v,
                        JsonValue::Null
                            | JsonValue::Bool(_)
                            | JsonValue::Number(_)
                            | JsonValue::String(_)
                    )
                }) {
                    let joined = items
                        .iter()
                        .filter_map(json_scalar_to_string)
                        .collect::<Vec<String>>()
                        .join("|");
                    out.insert(p.to_string(), joined);
                } else {
                    for (idx, child) in items.iter().enumerate() {
                        let child_prefix = format!("{p}[{idx}]");
                        flatten_json_value(child, Some(&child_prefix), out);
                    }
                }
            } else {
                for (idx, child) in items.iter().enumerate() {
                    let child_prefix = format!("item[{idx}]");
                    flatten_json_value(child, Some(&child_prefix), out);
                }
            }
        }
        _ => {
            let key = prefix.unwrap_or("value");
            out.insert(
                key.to_string(),
                json_scalar_to_string(value).unwrap_or_default(),
            );
        }
    }
}

fn json_scalar_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Null => Some(String::new()),
        JsonValue::Bool(v) => Some(v.to_string()),
        JsonValue::Number(v) => Some(v.to_string()),
        JsonValue::String(v) => Some(v.to_string()),
        _ => None,
    }
}

fn json_path<'a>(value: &'a JsonValue, path: &[&str]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in path {
        current = current.as_object()?.get(*segment)?;
    }
    Some(current)
}

fn json_path_str(value: &JsonValue, path: &[&str]) -> Option<String> {
    json_path(value, path).and_then(json_scalar_to_string)
}

fn fhir_first_coding_field(codeable: Option<&JsonValue>, field: &str) -> Option<String> {
    let codeable = codeable?;
    if let Some(codings) = codeable.get("coding").and_then(|v| v.as_array()) {
        for coding in codings {
            if let Some(v) = json_path_str(coding, &[field])
                && !v.is_empty()
            {
                return Some(v);
            }
        }
    }
    json_path_str(codeable, &[field])
}

fn collect_fhir_resources(value: &JsonValue, out: &mut Vec<JsonValue>) {
    match value {
        JsonValue::Object(obj) => {
            if let Some(resource_type) = obj.get("resourceType").and_then(|v| v.as_str()) {
                if resource_type.eq_ignore_ascii_case("Bundle")
                    && let Some(entries) = obj.get("entry").and_then(|v| v.as_array())
                {
                    for entry in entries {
                        if let Some(resource) = entry.get("resource") {
                            collect_fhir_resources(resource, out);
                        }
                    }
                    return;
                }
                out.push(value.clone());
                return;
            }
            if let Some(entries) = obj.get("entry").and_then(|v| v.as_array()) {
                for entry in entries {
                    if let Some(resource) = entry.get("resource") {
                        collect_fhir_resources(resource, out);
                    }
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                collect_fhir_resources(item, out);
            }
        }
        _ => {}
    }
}

fn looks_fhir_json_content(content: &str) -> bool {
    parse_json_documents(content)
        .map(|docs| docs.iter().any(is_fhir_json_value))
        .unwrap_or(false)
}

fn is_fhir_json_value(value: &JsonValue) -> bool {
    match value {
        JsonValue::Object(obj) => {
            obj.get("resourceType").and_then(|v| v.as_str()).is_some()
                || obj
                    .get("entry")
                    .and_then(|v| v.as_array())
                    .is_some_and(|entries| {
                        entries.iter().any(|entry| {
                            entry
                                .get("resource")
                                .and_then(|res| res.get("resourceType"))
                                .and_then(|v| v.as_str())
                                .is_some()
                        })
                    })
        }
        JsonValue::Array(items) => items.iter().any(is_fhir_json_value),
        _ => false,
    }
}

fn looks_hl7_message(content: &str) -> bool {
    content
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .is_some_and(|line| line.starts_with("MSH") && line.len() > 3)
}

fn looks_turtle_content(trimmed_content: &str) -> bool {
    trimmed_content.starts_with("@prefix")
        || trimmed_content.starts_with("@base")
        || (trimmed_content.contains(" fhir:")
            && (trimmed_content.contains("fhir:v") || trimmed_content.contains(" a fhir:")))
}

fn looks_html_content(trimmed_content: &str) -> bool {
    trimmed_content.starts_with("<!DOCTYPE")
        || trimmed_content.starts_with("<html")
        || trimmed_content.starts_with("<HTML")
}

fn hl7_field<'a>(fields: &'a [&str], field_no: usize) -> &'a str {
    fields.get(field_no).copied().unwrap_or("")
}

fn hl7_msh_field<'a>(fields: &'a [&str], field_no: usize) -> &'a str {
    if field_no <= 1 {
        return "";
    }
    fields.get(field_no - 1).copied().unwrap_or("")
}

fn hl7_component(value: &str, one_based_index: usize) -> String {
    if one_based_index == 0 {
        return String::new();
    }
    value
        .split('^')
        .nth(one_based_index - 1)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn insert_if_non_empty(row: &mut HashMap<String, String>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            row.insert(key.to_string(), trimmed.to_string());
        }
    }
}

fn qname_local_lower(name: QName<'_>) -> String {
    let raw = String::from_utf8_lossy(name.as_ref());
    raw.rsplit(':')
        .next()
        .unwrap_or(raw.as_ref())
        .to_ascii_lowercase()
}

fn combine_id_parts(root: Option<String>, extension: Option<String>) -> Option<String> {
    match (root, extension) {
        (Some(root), Some(extension)) if !root.is_empty() && !extension.is_empty() => {
            Some(format!("{root}:{extension}"))
        }
        (Some(root), _) if !root.is_empty() => Some(root),
        (_, Some(extension)) if !extension.is_empty() => Some(extension),
        _ => None,
    }
}

fn build_patient_name(family: &str, given_parts: &[String]) -> String {
    let given = given_parts
        .iter()
        .map(String::as_str)
        .collect::<Vec<&str>>()
        .join(" ");
    let mut parts: Vec<&str> = Vec::new();
    if !given.trim().is_empty() {
        parts.push(given.trim());
    }
    if !family.trim().is_empty() {
        parts.push(family.trim());
    }
    parts.join(" ")
}

fn detect_delimiter(content: &str) -> u8 {
    let lines: Vec<&str> = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(40)
        .collect();

    if lines.is_empty() {
        return b',';
    }

    let candidates = [b',', b'\t', b';', b'|'];
    let mut best = b',';
    let mut best_score = f64::MIN;

    for delimiter in candidates {
        let delim_char = delimiter as char;
        let counts: Vec<usize> = lines
            .iter()
            .map(|line| count_delimiters(line, delim_char))
            .collect();
        let lines_with_delim = counts.iter().filter(|count| **count > 0).count();
        if lines_with_delim == 0 {
            continue;
        }

        let avg = counts.iter().sum::<usize>() as f64 / counts.len() as f64;
        let variance = counts
            .iter()
            .map(|count| {
                let delta = *count as f64 - avg;
                delta * delta
            })
            .sum::<f64>()
            / counts.len() as f64;

        let score = (lines_with_delim as f64 * 3.0) + avg - variance;
        if score > best_score {
            best_score = score;
            best = delimiter;
        }
    }

    best
}

fn count_delimiters(line: &str, delimiter: char) -> usize {
    let mut in_quotes = false;
    let mut count = 0usize;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '"' {
            if in_quotes && chars.peek() == Some(&'"') {
                let _ = chars.next();
            } else {
                in_quotes = !in_quotes;
            }
            continue;
        }

        if ch == delimiter && !in_quotes {
            count += 1;
        }
    }

    count
}

fn build_profile(rows: Vec<Vec<String>>) -> Result<Profile> {
    if rows.is_empty() {
        bail!("cannot infer profile from empty data");
    }

    let header_row = detect_header_row(&rows);
    let metadata_rows = header_row;
    let headers = rows
        .get(header_row)
        .context("failed to read detected header row")?
        .clone();
    let normalized_headers = normalize_headers(&headers);
    let total_columns = headers.len();

    let data_rows = if header_row + 1 < rows.len() {
        rows[header_row + 1..].to_vec()
    } else {
        Vec::new()
    };

    let mut columns = Vec::new();
    for col_idx in 0..total_columns {
        let values: Vec<&str> = data_rows
            .iter()
            .map(|row| row.get(col_idx).map_or("", String::as_str))
            .collect();
        columns.push(analyze_column(
            col_idx,
            normalized_headers[col_idx].clone(),
            &values,
        ));
    }

    Ok(Profile {
        header_row,
        metadata_rows,
        total_columns,
        rows: data_rows,
        columns,
    })
}

fn detect_header_row(rows: &[Vec<String>]) -> usize {
    let scan_limit = rows.len().min(50);
    let mut best_idx = 0usize;
    let mut best_score = f64::MIN;

    for (idx, row) in rows.iter().take(scan_limit).enumerate() {
        if row.is_empty() {
            continue;
        }
        let total = row.len();
        if total == 0 {
            continue;
        }

        let non_empty: Vec<String> = row
            .iter()
            .map(|cell| cell.trim())
            .filter(|cell| !cell.is_empty())
            .map(ToString::to_string)
            .collect();
        if non_empty.len() < 2 {
            continue;
        }

        let null_ratio = 1.0 - ratio(non_empty.len(), total);
        let string_like = non_empty
            .iter()
            .filter(|value| {
                !looks_bool(value) && !looks_numeric(value) && parse_date(value).is_none()
            })
            .count();
        let unique_ratio = {
            let unique: HashSet<String> =
                non_empty.iter().map(|v| v.to_ascii_lowercase()).collect();
            ratio(unique.len(), non_empty.len())
        };
        let string_ratio = ratio(string_like, non_empty.len());
        let score = (string_ratio * 0.55) + (unique_ratio * 0.30) + ((1.0 - null_ratio) * 0.15);

        if string_ratio >= 0.45 && null_ratio <= 0.80 && score > best_score {
            best_score = score;
            best_idx = idx;
        }
    }

    best_idx
}

fn normalize_headers(raw_headers: &[String]) -> Vec<String> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    raw_headers
        .iter()
        .enumerate()
        .map(|(idx, raw)| {
            let mut normalized = normalize_header(raw);
            if normalized.is_empty() {
                normalized = format!("column_{}", idx + 1);
            }
            let count = seen.entry(normalized.clone()).or_insert(0);
            *count += 1;
            if *count == 1 {
                normalized
            } else {
                format!("{normalized}_{count}")
            }
        })
        .collect()
}

fn normalize_header(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

fn analyze_column(index: usize, name: String, values: &[&str]) -> ColumnStats {
    let mut null_count = 0usize;
    let mut non_null_count = 0usize;
    let mut numeric_count = 0usize;
    let mut int_count = 0usize;
    let mut bool_count = 0usize;
    let mut date_count = 0usize;
    let mut string_count = 0usize;

    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            null_count += 1;
            continue;
        }

        non_null_count += 1;
        if parse_bool(trimmed).is_some() {
            bool_count += 1;
            continue;
        }
        if parse_date(trimmed).is_some() {
            date_count += 1;
            continue;
        }
        if parse_i64(trimmed).is_some() {
            int_count += 1;
            numeric_count += 1;
            continue;
        }
        if parse_f64(trimmed).is_some() {
            numeric_count += 1;
            continue;
        }
        string_count += 1;
    }

    let inferred_type = infer_type(
        non_null_count,
        numeric_count,
        int_count,
        bool_count,
        date_count,
    );

    ColumnStats {
        index,
        name,
        inferred_type,
        null_count,
        non_null_count,
        numeric_count,
        bool_count,
        date_count,
        string_count,
    }
}

fn infer_type(
    non_null_count: usize,
    numeric_count: usize,
    int_count: usize,
    bool_count: usize,
    date_count: usize,
) -> ColumnType {
    if non_null_count == 0 {
        return ColumnType::String;
    }

    if ratio(date_count, non_null_count) >= 0.80 {
        return ColumnType::Date;
    }
    if ratio(bool_count, non_null_count) >= 0.95 {
        return ColumnType::Bool;
    }
    if ratio(numeric_count, non_null_count) >= 0.90 {
        if ratio(int_count, numeric_count.max(1)) >= 0.95 {
            return ColumnType::Int;
        }
        return ColumnType::Float;
    }
    ColumnType::String
}

fn has_mixed_types(col: &ColumnStats) -> bool {
    let mut present = 0usize;
    if col.numeric_count > 0 {
        present += 1;
    }
    if col.bool_count > 0 {
        present += 1;
    }
    if col.date_count > 0 {
        present += 1;
    }
    if col.string_count > 0 {
        present += 1;
    }
    present > 1
}

fn build_series(name: &str, inferred_type: &ColumnType, values: &[String]) -> Result<Series> {
    let series = match inferred_type {
        ColumnType::String => {
            let vals: Vec<Option<String>> = values
                .iter()
                .map(|v| {
                    let trimmed = v.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .collect();
            Series::new(name.into(), vals)
        }
        ColumnType::Int => {
            let vals: Vec<Option<i64>> = values.iter().map(|v| parse_i64(v.trim())).collect();
            Series::new(name.into(), vals)
        }
        ColumnType::Float => {
            let vals: Vec<Option<f64>> = values.iter().map(|v| parse_f64(v.trim())).collect();
            Series::new(name.into(), vals)
        }
        ColumnType::Bool => {
            let vals: Vec<Option<bool>> = values.iter().map(|v| parse_bool(v.trim())).collect();
            Series::new(name.into(), vals)
        }
        ColumnType::Date => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).context("invalid epoch date")?;
            let vals: Vec<Option<i32>> = values
                .iter()
                .map(|v| {
                    parse_date(v.trim()).map(|d| {
                        d.signed_duration_since(epoch)
                            .num_days()
                            .try_into()
                            .unwrap_or_default()
                    })
                })
                .collect();
            let raw = Series::new(name.into(), vals);
            raw.cast(&DataType::Date)?
        }
    };

    Ok(series)
}

fn parse_i64(raw: &str) -> Option<i64> {
    let normalized = normalize_numeric(raw)?;
    if let Ok(v) = normalized.parse::<i64>() {
        return Some(v);
    }

    if let Ok(v) = normalized.parse::<f64>() {
        if v.fract().abs() < f64::EPSILON && v.is_finite() {
            return Some(v as i64);
        }
    }
    None
}

fn parse_f64(raw: &str) -> Option<f64> {
    let normalized = normalize_numeric(raw)?;
    normalized.parse::<f64>().ok()
}

fn normalize_numeric(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let negative_parentheses = trimmed.starts_with('(') && trimmed.ends_with(')');
    let mut core = trimmed
        .trim_matches('(')
        .trim_matches(')')
        .trim_start_matches('$')
        .replace(',', "");

    if core.is_empty() {
        return None;
    }
    if negative_parentheses && !core.starts_with('-') {
        core = format!("-{core}");
    }

    Some(core)
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" => Some(true),
        "false" | "f" | "no" | "n" => Some(false),
        _ => None,
    }
}

fn parse_date(raw: &str) -> Option<NaiveDate> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    for fmt in DATE_FORMATS {
        if let Ok(d) = NaiveDate::parse_from_str(trimmed, fmt) {
            return Some(d);
        }
    }

    for fmt in DATETIME_FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Some(dt.date());
        }
    }

    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.date_naive());
    }

    None
}

fn looks_numeric(value: &str) -> bool {
    parse_f64(value).is_some()
}

fn looks_bool(value: &str) -> bool {
    parse_bool(value).is_some()
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_normalization_is_stable_and_unique() {
        let input = vec![
            " Sample ID ".to_string(),
            "Sample-ID".to_string(),
            "".to_string(),
        ];
        let output = normalize_headers(&input);
        assert_eq!(output, vec!["sample_id", "sample_id_2", "column_3"]);
    }

    #[test]
    fn numeric_parsing_handles_commas_and_parentheses() {
        assert_eq!(parse_i64("1,234"), Some(1234));
        assert_eq!(parse_i64("(42)"), Some(-42));
        assert_eq!(parse_f64("$3,100.5"), Some(3100.5));
    }

    #[test]
    fn delimiter_detection_finds_tsv() {
        let data = "id\tvalue\tdate\n1\t10.5\t2026-01-01\n2\t11.0\t2026-01-02\n";
        assert_eq!(detect_delimiter(data), b'\t');
    }

    #[test]
    fn delimiter_detection_finds_pipe() {
        let data = "patient_id|test_name|result\nP-1|CRP|4.1\nP-2|CRP|2.8\n";
        assert_eq!(detect_delimiter(data), b'|');
    }

    #[test]
    fn parse_bool_does_not_use_numeric_forms() {
        assert_eq!(parse_bool("1"), None);
        assert_eq!(parse_bool("0"), None);
    }

    #[test]
    fn strip_utf8_bom_from_content() {
        let mut content = "\u{feff}id,value\n1,10\n".to_string();
        strip_utf8_bom(&mut content);
        assert_eq!(content, "id,value\n1,10\n");
    }

    #[test]
    fn cxml_parsing_extracts_line_items() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<cXML payloadID="pid-1" timestamp="2026-04-26T09:00:00Z">
  <Request>
    <OrderRequest>
      <OrderRequestHeader orderID="PO-42" orderDate="2026-04-26T09:00:01Z">
        <ShipTo><Address><Name>Lab Site A</Name></Address></ShipTo>
        <BillTo><Address><Name>Billing Team</Name></Address></BillTo>
      </OrderRequestHeader>
      <ItemOut lineNumber="1" quantity="2">
        <ItemID><SupplierPartID>SKU-1</SupplierPartID></ItemID>
        <ItemDetail>
          <UnitPrice><Money currency="USD">10.5</Money></UnitPrice>
          <Description>Test kit</Description>
          <UnitOfMeasure>EA</UnitOfMeasure>
          <Classification domain="UNSPSC">411161</Classification>
          <Extrinsic name="LineType">Quantity</Extrinsic>
        </ItemDetail>
      </ItemOut>
    </OrderRequest>
  </Request>
</cXML>"#;

        let rows = parse_cxml_content(xml, CxmlMode::Mapped).expect("cxml parsing should succeed");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], "order_id");
        assert!(rows[0].contains(&"supplier_part_id".to_string()));
        assert!(rows[0].contains(&"description".to_string()));

        let headers = &rows[0];
        let values = &rows[1];
        let idx = |key: &str| {
            headers
                .iter()
                .position(|h| h == key)
                .expect("expected header key")
        };
        assert_eq!(values[idx("order_id")], "PO-42");
        assert_eq!(values[idx("line_number")], "1");
        assert_eq!(values[idx("quantity")], "2");
        assert_eq!(values[idx("supplier_part_id")], "SKU-1");
        assert_eq!(values[idx("description")], "Test kit");
        assert_eq!(values[idx("unit_price")], "10.5");
        assert_eq!(values[idx("currency")], "USD");
        assert_eq!(values[idx("ship_to_name")], "Lab Site A");
        assert_eq!(values[idx("bill_to_name")], "Billing Team");
        assert_eq!(values[idx("extrinsic_linetype")], "Quantity");
    }

    #[test]
    fn cxml_auto_mode_captures_distribution_segments() {
        let xml = r#"<cXML>
  <Request>
    <OrderRequest>
      <OrderRequestHeader orderID="PO-100" />
      <ItemOut lineNumber="1" quantity="1">
        <Distribution>
          <Accounting name="Main Account Split">
            <Segment type="Cost Centre" id="0001" description="COST_CTR"/>
            <Segment type="GL account" id="4000" description="GL_ACC"/>
          </Accounting>
          <Charge><Money currency="USD">123.45</Money></Charge>
        </Distribution>
      </ItemOut>
    </OrderRequest>
  </Request>
</cXML>"#;

        let rows =
            parse_cxml_content(xml, CxmlMode::Auto).expect("cxml auto parsing should succeed");
        assert_eq!(rows.len(), 2);

        let headers = &rows[0];
        let values = &rows[1];
        let idx = |key: &str| {
            headers
                .iter()
                .position(|h| h == key)
                .expect("expected header key")
        };

        assert_eq!(
            values[idx("x_itemout_distribution_accounting_attr_name")],
            "Main Account Split"
        );
        assert_eq!(
            values[idx("x_itemout_distribution_accounting_segment_attr_type")],
            "Cost Centre | GL account"
        );
        assert_eq!(
            values[idx("x_itemout_distribution_accounting_segment_attr_id")],
            "0001 | 4000"
        );
        assert_eq!(
            values[idx("x_itemout_distribution_accounting_segment_attr_description")],
            "COST_CTR | GL_ACC"
        );
        assert_eq!(values[idx("x_itemout_distribution_charge_money")], "123.45");
        assert_eq!(
            values[idx("x_itemout_distribution_charge_money_attr_currency")],
            "USD"
        );
    }

    #[test]
    fn cxml_parsing_supports_invoice_detail_item() {
        let xml = r#"<cXML>
  <Request>
    <OrderRequest>
      <OrderRequestHeader orderID="12345678">
        <InvoiceRequest purpose="standard">
          <InvoiceHeader>
            <InvoiceDetailRequest>
              <Extrinsic name="ext_invoice_id">INV-789</Extrinsic>
            </InvoiceDetailRequest>
          </InvoiceHeader>
          <InvoiceDetailItem lineNumber="101">
            <Extrinsic name="description">Low Arc Kitchen Faucet</Extrinsic>
            <Quantity>2</Quantity>
            <UnitPrice>41.15</UnitPrice>
            <UnitOfMeasure>EA</UnitOfMeasure>
            <LineTotal>86.8</LineTotal>
          </InvoiceDetailItem>
        </InvoiceRequest>
      </OrderRequestHeader>
    </OrderRequest>
  </Request>
</cXML>"#;

        let rows = parse_cxml_content(xml, CxmlMode::Mapped)
            .expect("invoice-detail cxml parsing should succeed");
        assert_eq!(rows.len(), 2);
        let headers = &rows[0];
        let values = &rows[1];
        let idx = |key: &str| {
            headers
                .iter()
                .position(|h| h == key)
                .expect("expected header key")
        };

        assert_eq!(values[idx("order_id")], "12345678");
        assert_eq!(values[idx("invoice_purpose")], "standard");
        assert_eq!(values[idx("line_number")], "101");
        assert_eq!(values[idx("quantity")], "2");
        assert_eq!(values[idx("unit_price")], "41.15");
        assert_eq!(values[idx("line_total")], "86.8");
        assert_eq!(values[idx("header_extrinsic_ext_invoice_id")], "INV-789");
        assert_eq!(
            values[idx("extrinsic_description")],
            "Low Arc Kitchen Faucet"
        );
    }

    #[test]
    fn infer_inner_extension_for_gz_path() {
        let p1 = Path::new("/tmp/a.xml.gz");
        let p2 = Path::new("/tmp/b.cxml.gz");
        let p3 = Path::new("/tmp/c.gz");
        assert_eq!(
            infer_inner_extension_from_gz_path(p1).as_deref(),
            Some("xml")
        );
        assert_eq!(
            infer_inner_extension_from_gz_path(p2).as_deref(),
            Some("cxml")
        );
        assert_eq!(infer_inner_extension_from_gz_path(p3).as_deref(), None);
    }

    #[test]
    fn json_flatten_extracts_nested_fields() {
        let json = r#"{
  "id": "A-1",
  "patient": {"name": {"given": "Jane", "family": "Doe"}},
  "result": {"value": 3.2, "unit": "mg/dL"},
  "flags": ["fasting", "verified"]
}"#;

        let rows = parse_json_content(json).expect("json parsing should succeed");
        let headers = &rows[0];
        let values = &rows[1];
        let idx = |key: &str| {
            headers
                .iter()
                .position(|h| h == key)
                .expect("expected header key")
        };

        assert_eq!(values[idx("id")], "A-1");
        assert_eq!(values[idx("patient.name.given")], "Jane");
        assert_eq!(values[idx("result.value")], "3.2");
        assert_eq!(values[idx("flags")], "fasting|verified");
    }

    #[test]
    fn fhir_bundle_extracts_resource_rows() {
        let json = r#"{
  "resourceType":"Bundle",
  "type":"collection",
  "entry":[
    {
      "resource":{
        "resourceType":"Patient",
        "id":"p1",
        "gender":"female",
        "birthDate":"1980-01-01",
        "name":[{"family":"Doe","given":["Jane"]}]
      }
    },
    {
      "resource":{
        "resourceType":"Observation",
        "id":"obs1",
        "status":"final",
        "subject":{"reference":"Patient/p1"},
        "code":{"coding":[{"system":"http://loinc.org","code":"29463-7","display":"Body Weight"}]},
        "effectiveDateTime":"2026-04-25",
        "valueQuantity":{"value":185,"unit":"lbs"}
      }
    }
  ]
}"#;

        let rows = parse_fhir_content(json).expect("fhir parsing should succeed");
        assert_eq!(rows.len(), 3);

        let headers = &rows[0];
        let p_values = &rows[1];
        let o_values = &rows[2];
        let idx = |key: &str| {
            headers
                .iter()
                .position(|h| h == key)
                .expect("expected header key")
        };

        assert_eq!(p_values[idx("resource_type")], "Patient");
        assert_eq!(p_values[idx("resource_id")], "p1");
        assert_eq!(o_values[idx("resource_type")], "Observation");
        assert_eq!(o_values[idx("code_code")], "29463-7");
        assert_eq!(o_values[idx("value_quantity_value")], "185");
    }

    #[test]
    fn hl7_extracts_obx_rows() {
        let hl7 = "MSH|^~\\&|LIS|LAB|EHR|HOSP|202604251200||ORU^R01|MSG00001|P|2.5\rPID|1||12345^^^HOSP^MR||Doe^Jane||19800101|F\rOBR|1||A1001|88304^Path report\rOBX|1|NM|718-7^Hemoglobin||13.2|g/dL|12-16|N|||F|202604251159\r";
        let rows = parse_hl7_content(hl7).expect("hl7 parsing should succeed");

        assert_eq!(rows.len(), 2);
        let headers = &rows[0];
        let values = &rows[1];
        let idx = |key: &str| {
            headers
                .iter()
                .position(|h| h == key)
                .expect("expected header key")
        };

        assert_eq!(values[idx("message_type")], "ORU");
        assert_eq!(values[idx("trigger_event")], "R01");
        assert_eq!(values[idx("patient_id")], "12345^^^HOSP^MR");
        assert_eq!(values[idx("obx_code")], "718-7");
        assert_eq!(values[idx("obx_value")], "13.2");
    }

    #[test]
    fn cda_extracts_patient_and_observation() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
  <id root="2.16.840.1.113883.19.5" extension="TT998"/>
  <effectiveTime value="20260425121000"/>
  <recordTarget>
    <patientRole>
      <id extension="PAT-1"/>
      <patient>
        <name><given>Jane</given><family>Doe</family></name>
        <administrativeGenderCode code="F"/>
        <birthTime value="19800101"/>
      </patient>
    </patientRole>
  </recordTarget>
  <component>
    <structuredBody>
      <component>
        <section>
          <entry>
            <observation classCode="OBS" moodCode="EVN">
              <id root="1.2.3.4.5" extension="OBS-1"/>
              <code code="718-7" displayName="Hemoglobin" codeSystem="2.16.840.1.113883.6.1"/>
              <effectiveTime value="20260425115900"/>
              <value xsi:type="PQ" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" value="13.2" unit="g/dL"/>
            </observation>
          </entry>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>"#;

        let rows = parse_cda_content(xml).expect("cda parsing should succeed");
        assert_eq!(rows.len(), 2);

        let headers = &rows[0];
        let values = &rows[1];
        let idx = |key: &str| {
            headers
                .iter()
                .position(|h| h == key)
                .expect("expected header key")
        };

        assert_eq!(values[idx("document_id")], "2.16.840.1.113883.19.5:TT998");
        assert_eq!(values[idx("patient_name")], "Jane Doe");
        assert_eq!(values[idx("observation_code")], "718-7");
        assert_eq!(values[idx("observation_value")], "13.2");
    }

    #[test]
    fn rdf_turtle_extracts_triples() {
        let ttl = r#"@prefix fhir: <http://hl7.org/fhir/> .
<http://hl7.org/fhir/Patient/example> a fhir:Patient ;
  fhir:id [ fhir:v "example" ] .
"#;

        let rows = parse_rdf_content(ttl).expect("rdf parsing should succeed");
        assert_eq!(rows.len(), 4);

        let headers = &rows[0];
        let values = &rows[1];
        let idx = |key: &str| {
            headers
                .iter()
                .position(|h| h == key)
                .expect("expected header key")
        };

        assert!(values[idx("subject")].contains("Patient/example"));
        assert!(values[idx("predicate")].contains("rdf-syntax-ns#type"));
        assert_eq!(values[idx("object_kind")], "iri");
    }

    #[test]
    fn html_with_rdf_pre_extracts_triples() {
        let html = r#"<!DOCTYPE html><html><body><pre class="rdf">
@prefix fhir: &lt;http://hl7.org/fhir/&gt; .
&lt;http://hl7.org/fhir/Patient/example&gt; a fhir:Patient .
</pre></body></html>"#;

        let rows = parse_html_content(html).expect("html rdf parsing should succeed");
        assert_eq!(rows.len(), 2);
        let headers = &rows[0];
        let values = &rows[1];
        let pred_idx = headers
            .iter()
            .position(|h| h == "predicate")
            .expect("expected predicate");
        assert!(values[pred_idx].contains("rdf-syntax-ns#type"));
    }
}
