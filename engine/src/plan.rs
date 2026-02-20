use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::collect;
use datafusion::prelude::*;
use encoding_rs::Encoding;
use wasm_bindgen::prelude::*;

use crate::byte_transform::utf8_encoder::Utf8Encoder;
use crate::file::{FileReader, FileSource};
use crate::js_object_store::JsObjectStore;
use crate::record_set::RecordSet;

#[wasm_bindgen]
pub struct Plan {
    plan: LogicalPlan,
    files: Arc<[FileReader]>,
}

#[wasm_bindgen]
impl Plan {
    pub async fn read_file(file: web_sys::File) -> Result<Self, String> {
        let filename = file.name();
        let ext = std::path::Path::new(&filename)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap();

        let transform = Utf8Encoder::new(Encoding::for_label(b"ISO-8859-1").unwrap());
        let file = file.transform(transform).await;
        let file = FileReader::new(file);
        let files = Arc::new([file]);

        let ctx = SessionContext::new();
        let file_store = Arc::new(JsObjectStore::new(files.clone()));
        ctx.register_object_store(&url::Url::try_from("js:///").unwrap(), file_store);
        let plan = match ext {
            "csv" => {
                let opts = CsvReadOptions::new().has_header(true);
                ctx.read_csv("js:///0.csv", opts).await
            }
            "json" => ctx.read_json("js:///0.json", Default::default()).await,
            "jsonl" => ctx.read_json("js:///0.json", Default::default()).await,
            "parquet" => {
                ctx.read_parquet("js:///0.parquet", Default::default())
                    .await
            }
            _ => Err(format!("unknown file extension: {ext}"))?,
        };
        let plan = plan.map_err(|err| err.to_string())?.into_unoptimized_plan();

        Ok(Plan { plan, files })
    }

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
        let batches = collect(physical_plan, task_ctx)
            .await
            .map_err(|err| format!("{err:?}"))?;

        Ok(RecordSet::new(schema, batches))
    }
}
