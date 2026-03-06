use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::provider_as_source;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::physical_plan::collect;
use datafusion::prelude::*;
use wasm_bindgen::prelude::*;

use crate::file::FileReader;
use crate::file_format::FileFormat;
use crate::js_object_store::JsObjectStore;
use crate::record_set::RecordSet;
use crate::JsSchema;

#[wasm_bindgen]
pub struct Plan {
    plan: LogicalPlan,
    files: Arc<[FileReader]>,
}

#[wasm_bindgen]
impl Plan {
    pub async fn read_file(
        file: web_sys::File,
        format: FileFormat,
        schema: &JsSchema,
    ) -> Result<Self, String> {
        let file = FileReader::new(file);
        let files = Arc::new([file]);

        let format: Arc<dyn datafusion::datasource::file_format::FileFormat> = match format {
            FileFormat::Json { flatten_top_level_arrays, single_field } => {
                let format = datafusion::datasource::file_format::json::JsonFormat::default()
                    .with_newline_delimited(!flatten_top_level_arrays)
                    .with_single_field(single_field.is_some());
                Arc::new(format)
            }
            FileFormat::Csv { has_headers, .. } => {
                let format = datafusion::datasource::file_format::csv::CsvFormat::default()
                    .with_has_header(has_headers);
                Arc::new(format)
            }
            FileFormat::Parquet => {
                let format = datafusion::datasource::file_format::parquet::ParquetFormat::default();
                Arc::new(format)
            }
        };

        let url = "js:///0";
        let config =
            ListingTableConfig::new(ListingTableUrl::parse(url).map_err(|err| err.to_string())?)
                .with_listing_options(ListingOptions::new(format).with_file_extension(""))
                .with_schema(schema.inner().clone());
        let listing_table = Arc::new(ListingTable::try_new(config).map_err(|err| err.to_string())?);
        let source = provider_as_source(listing_table);

        let plan = LogicalPlanBuilder::scan(UNNAMED_TABLE, source, None)
            .map_err(|err| err.to_string())?
            .build()
            .map_err(|err| err.to_string())?;

        Ok(Plan { plan, files })
    }

    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self, String> {
        let plan = LogicalPlanBuilder::new(self.plan)
            .limit(skip, fetch)
            .map_err(|err| err.to_string())?
            .build()
            .map_err(|err| err.to_string())?;
        Ok(Self { plan, ..self })
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
