mod record_set;
mod utils;

use std::ops::Range;

use datafusion::prelude::*;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::record_set::RecordSet;

#[wasm_bindgen]
pub async fn create_record_set(min: u32, max: u32) -> Result<RecordSet, String> {
    let batches = SessionContext::new()
        .read_empty()
        .map_err(|_| "cannot read empty")?
        .select([
            lit("hello world").alias("foo"),
            range(lit(min), lit(max), lit(1)).alias("n"),
        ])
        .map_err(|_| "cannot select")?
        .unnest_columns(&["n"])
        .map_err(|_| "cannot unnest")?
        .collect()
        .await
        .map_err(|_| "cannot collect")?;

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
