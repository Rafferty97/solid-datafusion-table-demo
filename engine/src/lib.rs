use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use wasm_bindgen::prelude::*;

use crate::file::FileSource;
use crate::file_format::FileFormat;

mod file;
mod file_format;
mod js_object_store;
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
    file: web_sys::File,
    max_records: Option<usize>,
) -> Result<FileFormatAndSchema, String> {
    use datafusion::arrow::csv::reader::Format as CsvFormat;
    use datafusion::arrow::json::reader::infer_json_schema;
    use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;

    let filename = file.name();
    let ext = std::path::Path::new(&filename)
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or("no file extension")?;

    let format = match ext {
        "csv" => FileFormat::Csv {
            encoding: "utf-8".into(),
            has_headers: true,
        },
        "json" | "jsonl" => FileFormat::Json {
            flatten_top_level_arrays: false,
            single_field: None,
        },
        "parquet" => FileFormat::Parquet,
        ext => Err(format!("unknown file extension: {ext}"))?,
    };

    let bytes = file.read(0..file.size()).await;
    let reader = std::io::Cursor::new(bytes);

    let schema = match format {
        FileFormat::Csv { has_headers, .. } => {
            let (schema, _) = CsvFormat::default()
                .with_header(has_headers)
                .infer_schema(reader, max_records)
                .map_err(|err| err.to_string())?;
            Arc::new(schema)
        }
        FileFormat::Json { .. } => {
            let (schema, _) =
                infer_json_schema(reader, max_records).map_err(|err| err.to_string())?;
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
