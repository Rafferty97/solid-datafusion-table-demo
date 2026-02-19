mod byte_transform;
mod js_object_store;
mod record_set;
mod utils;

use std::convert::TryFrom;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::collect;
use datafusion::prelude::*;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::js_object_store::{File, JsObjectStore};
use crate::record_set::RecordSet;

#[wasm_bindgen]
pub struct Plan {
    plan: LogicalPlan,
    files: Arc<[File]>,
}

#[wasm_bindgen]
pub fn empty() -> Result<RecordSet, String> {
    let schema = Schema::empty().into();
    let batches = vec![RecordBatch::new_empty(schema)];
    Ok(batches.into())
}

#[wasm_bindgen]
pub async fn read_file(file: web_sys::File) -> Result<Plan, String> {
    let filename = file.name();
    let ext = std::path::Path::new(&filename)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap();

    let file = File::from_file(file);
    let files = Arc::new([file]);

    let ctx = SessionContext::new();
    let file_store = Arc::new(JsObjectStore::new(files.clone()));
    ctx.register_object_store(&url::Url::try_from("js:///").unwrap(), file_store);
    let plan = match ext {
        "csv" => ctx.read_csv("js:///0.csv", Default::default()).await,
        "json" => ctx.read_json("js:///0.json", Default::default()).await,
        "jsonl" => ctx.read_json("js:///0.json", Default::default()).await,
        "parquet" => ctx.read_parquet("js:///0.parquet", Default::default()).await,
        _ => Err(format!("unknown file extension: {ext}"))?,
    };
    let plan = plan.map_err(|err| err.to_string())?.into_unoptimized_plan();

    Ok(Plan { plan, files })
}

#[wasm_bindgen]
impl Plan {
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self, String> {
        let Self { plan, files } = self;
        let plan = LogicalPlanBuilder::new(plan)
            .limit(skip, fetch)
            .map_err(|err| err.to_string())?
            .build()
            .map_err(|err| err.to_string())?;
        Ok(Self { plan, files })
    }

    pub async fn collect(&self) -> Result<RecordSet, String> {
        let files = self.files.clone();
        let file_store = Arc::new(JsObjectStore::new(files));

        let state = SessionContext::new().state();
        state
            .runtime_env()
            .register_object_store(&url::Url::try_from("js:///").unwrap(), file_store);
        let physical_plan = state
            .create_physical_plan(&self.plan)
            .await
            .map_err(|err| err.to_string())?;
        let task_ctx = Arc::new(TaskContext::from(&state));

        let schema = physical_plan.schema();
        let batches = collect(physical_plan, task_ctx).await.map_err(|err| err.to_string())?;

        Ok(RecordSet::new(schema, batches))
    }
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
