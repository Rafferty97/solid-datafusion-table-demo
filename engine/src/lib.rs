mod utils;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct RecordSet {
    num_rows: usize,
    batches: Vec<RecordBatch>,
}

#[wasm_bindgen]
impl RecordSet {
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn encode_schema(&self) -> Vec<u8> {
        use datafusion::arrow::ipc::writer::{
            write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
        };

        let schema = self.batches[0].schema();

        let mut buffer = vec![];
        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(true);
        let opts = IpcWriteOptions::default();

        let encoded =
            generator.schema_to_bytes_with_dictionary_tracker(&schema, &mut tracker, &opts);
        write_message(&mut buffer, encoded, &opts).unwrap();

        buffer
    }

    pub fn encode_rows(&self, start: usize, end: usize) -> Vec<u8> {
        use datafusion::arrow::ipc::writer::{
            write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
        };

        let mut buffer = vec![];
        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let opts = IpcWriteOptions::default();

        self.batches
            .iter()
            .scan(0, |offset, batch| {
                let output = (*offset, batch);
                *offset += batch.num_rows();
                Some(output)
            })
            .skip_while(|(offset, batch)| start >= offset + batch.num_rows())
            .map_while(|(offset, batch)| {
                let i = start.saturating_sub(offset);
                let j = end.checked_sub(offset)?.min(batch.num_rows());
                Some(batch.slice(i, j - i))
            })
            .for_each(|batch| {
                let (dicts, batch) = generator
                    .encoded_batch(&batch, &mut tracker, &opts)
                    .unwrap();
                assert!(dicts.is_empty());
                write_message(&mut buffer, batch, &opts).unwrap();
            });

        buffer
    }
}

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
    let num_rows = batches.iter().map(|b| b.num_rows()).sum();
    Ok(RecordSet { num_rows, batches })
}
