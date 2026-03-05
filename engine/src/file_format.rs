use serde::{Deserialize, Serialize};
use tsify::Tsify;

#[derive(Tsify, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(rename_all = "camelCase", tag = "format")]
pub enum FileFormat {
    #[serde(rename_all = "camelCase")]
    Json {
        #[tsify(optional)]
        flatten_top_level_arrays: bool,
        #[tsify(optional)]
        single_field: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Csv {
        encoding: String,
        has_headers: bool,
    },
    Parquet,
}
