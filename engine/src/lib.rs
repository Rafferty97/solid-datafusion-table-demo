mod js_object_store;
mod record_set;
mod utils;

use std::ops::Range;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::js_object_store::{File, JsObjectStore};
use crate::record_set::RecordSet;

// #[wasm_bindgen]
// pub struct Plan {
//     plan: LogicalPlan,
//     files: Arc<Mutex<Vec<>>>
// }

#[wasm_bindgen]
pub fn empty() -> Result<RecordSet, String> {
    let schema = Schema::empty().into();
    let batches = vec![RecordBatch::new_empty(schema)];
    Ok(batches.into())
}

#[wasm_bindgen]
pub async fn read_file(file: web_sys::File) -> Result<RecordSet, String> {
    // let batches = SessionContext::new()
    //     .read_empty()
    //     .map_err(|_| "cannot read empty")?
    //     .select([
    //         lit("hello world").alias("foo"),
    //         range(lit(min), lit(max), lit(1)).alias("n"),
    //     ])
    //     .map_err(|_| "cannot select")?
    //     .unnest_columns(&["n"])
    //     .map_err(|_| "cannot unnest")?
    //     .collect()
    //     .await
    //     .map_err(|_| "cannot collect")?;

    let filename = file.name();
    let ext = std::path::Path::new(&filename)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap();

    let object_store = Arc::new(JsObjectStore::new(vec![File::from_file(file)]));

    let ctx = SessionContext::new();
    ctx.register_object_store(&url::Url::from_str("js:///").unwrap(), object_store);

    let batches = match ext {
        "csv" => ctx.read_csv("js:///0.csv", Default::default()).await,
        "json" => ctx.read_json("js:///0.json", Default::default()).await,
        "jsonl" => ctx.read_json("js:///0.json", Default::default()).await,
        "parquet" => ctx.read_parquet("js:///0.parquet", Default::default()).await,
        _ => Err(format!("unknown file extension: {ext}"))?,
    };
    let batches = batches.unwrap().collect().await.unwrap();

    Ok(batches.into())
}

#[wasm_bindgen]
pub async fn file_read_test(file: web_sys::Blob) -> Vec<u8> {
    let file = FileHandle { file };
    file.read(0..file.size().min(100)).await
}

pub struct FileHandle {
    file: web_sys::Blob,
}

impl FileHandle {
    pub fn size(&self) -> usize {
        self.file.size() as usize
    }

    pub async fn read(&self, range: Range<usize>) -> Vec<u8> {
        let bytes = self
            .file
            .slice_with_i32_and_i32(range.start as _, range.end as _)
            .unwrap();
        let bytes = JsFuture::from(bytes.array_buffer()).await.unwrap();
        let bytes = js_sys::Uint8Array::new(&bytes).to_vec();
        // let bytes = decoder
        //     .as_ref()
        //     .map(|d| d.decode_range(&bytes, src_range.start, range))
        //     .unwrap_or(bytes);
        bytes
    }
}
