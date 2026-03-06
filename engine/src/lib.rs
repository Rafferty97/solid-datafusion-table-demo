use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::file::FileSource;
use crate::file_format::FileFormat;
use crate::json_infer::{JsonDetector, JsonKind};
use crate::utils::chunk_ranges;

mod file;
mod file_format;
mod js_object_store;
mod json_infer;
mod plan;
mod record_set;
mod utils;

#[wasm_bindgen(js_name = "Schema")]
#[derive(Clone)]
pub struct JsSchema(SchemaRef);

impl JsSchema {
    pub fn inner(&self) -> &SchemaRef {
        &self.0
    }
}

#[wasm_bindgen(js_class = "Schema")]
impl JsSchema {
    pub fn empty() -> Self {
        Self(Arc::new(Schema::empty()))
    }

    pub fn to_string(&self) -> String {
        self.inner().to_string()
    }
}

#[wasm_bindgen]
pub struct FileFormatAndSchema {
    format: FileFormat,
    schema: JsSchema,
}

#[wasm_bindgen]
impl FileFormatAndSchema {
    pub fn format(&self) -> FileFormat {
        self.format.clone()
    }

    pub fn schema(&self) -> JsSchema {
        self.schema.clone()
    }
}

#[wasm_bindgen]
pub async fn infer_file_format_and_schema(
    file: &web_sys::File,
    max_records: Option<usize>,
) -> Result<FileFormatAndSchema, String> {
    use datafusion::arrow::csv::reader::Format as CsvFormat;
    use datafusion::arrow::json::reader::{infer_json_schema_with_options, InferJsonSchemaOptions};
    use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;

    let filename = file.name();
    let ext = std::path::Path::new(&filename)
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or("no file extension")?;

    let format = match ext {
        "csv" => {
            let encoding = infer_file_encoding(file).await?;
            FileFormat::Csv { encoding, has_headers: true }
        }
        "json" | "jsonl" => {
            let kind = infer_json_kind(file).await?;
            FileFormat::Json {
                flatten_top_level_arrays: kind == JsonKind::JsonArray,
                single_field: (kind == JsonKind::JsonValues).then(|| "value".to_string()),
            }
        }
        "parquet" => FileFormat::Parquet,
        ext => Err(format!("unknown file extension: {ext}"))?,
    };

    let bytes = file.read(0..file.size()).await;
    let reader = std::io::Cursor::new(bytes);

    let schema = match format.clone() {
        FileFormat::Csv { has_headers, .. } => {
            let (schema, _) = CsvFormat::default()
                .with_header(has_headers)
                .infer_schema(reader, max_records)
                .map_err(|err| err.to_string())?;
            Arc::new(schema)
        }
        FileFormat::Json { flatten_top_level_arrays, single_field } => {
            let options = InferJsonSchemaOptions {
                max_read_records: max_records,
                flatten_top_level_arrays,
                single_field,
                ..Default::default()
            };
            let (schema, _) =
                infer_json_schema_with_options(reader, options).map_err(|err| err.to_string())?;
            Arc::new(schema)
        }
        FileFormat::Parquet { .. } => {
            let reader = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|err| err.to_string())?;
            Arc::clone(reader.schema())
        }
    };
    let schema = JsSchema(schema);

    Ok(FileFormatAndSchema { format, schema })
}

#[wasm_bindgen]
pub async fn infer_file_encoding(file: &web_sys::File) -> Result<String, String> {
    let mut detector = chardet::UniversalDetector::new();
    for (range, _) in chunk_ranges(file.size() as _, 64 * 1024) {
        let bytes = file
            .slice_with_i32_and_i32(range.start as _, range.end as _)
            .map_err(|_| "cannot read file".to_string())?;
        let bytes = JsFuture::from(bytes.array_buffer()).await.unwrap();
        let bytes = js_sys::Uint8Array::new(&bytes).to_vec();
        detector.feed(&bytes);
    }
    Ok(detector.close().0)
}

#[wasm_bindgen]
pub async fn infer_json_kind(file: &web_sys::File) -> Result<JsonKind, String> {
    let mut detector = JsonDetector::new();
    for (range, _) in chunk_ranges(file.size() as _, 64 * 1024) {
        let bytes = file
            .slice_with_i32_and_i32(range.start as _, range.end as _)
            .map_err(|_| "cannot read file".to_string())?;
        let bytes = JsFuture::from(bytes.array_buffer()).await.unwrap();
        let bytes = js_sys::Uint8Array::new(&bytes).to_vec();
        if detector.feed(&bytes).map_err(|err| err.to_string())? {
            break;
        }
    }
    detector.finish().map_err(|err| err.to_string())
}
